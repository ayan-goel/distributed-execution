package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/logformat"
)

type LogGap struct {
	Stream        string `json:"stream,omitempty"`
	FirstSequence uint64 `json:"firstSequence"`
	LastSequence  uint64 `json:"lastSequence"`
}
type LogCompletion struct {
	LogsComplete bool            `json:"logsComplete"`
	Gaps         []CompletionGap `json:"gaps"`
}
type CompletionGap struct {
	Stream string `json:"stream"`
	First  uint64 `json:"first"`
	Last   uint64 `json:"last"`
}
type LogSegment struct {
	ArtifactID      string         `json:"artifactId"`
	Stream          string         `json:"stream"`
	FirstSequence   uint64         `json:"firstSequence"`
	LastSequence    uint64         `json:"lastSequence"`
	Gaps            []LogGap       `json:"gaps"`
	Object          downloadObject `json:"object"`
	DownloadURL     string         `json:"downloadUrl"`
	Method          string         `json:"method"`
	RequiredHeaders http.Header    `json:"requiredHeaders"`
	ExpiresAt       time.Time      `json:"expiresAt"`
}
type LogPage struct {
	AttemptID  string         `json:"attemptId"`
	Stream     string         `json:"stream"`
	Segments   []LogSegment   `json:"segments"`
	NextCursor string         `json:"nextCursor"`
	HasMore    bool           `json:"hasMore"`
	Completion *LogCompletion `json:"completion"`
}

func (c *Client) ListLogs(ctx context.Context, attemptID, stream, cursor string) (LogPage, error) {
	if !downloadUUID(attemptID) || !slices.Contains([]string{"stdout", "stderr"}, stream) || len(cursor) > 256 {
		return LogPage{}, errors.New("invalid log attempt, stream, or cursor")
	}
	path := "/v1/attempts/" + attemptID + "/logs?stream=" + stream
	if cursor != "" {
		path += "&cursor=" + url.QueryEscape(cursor)
	}
	body, err := c.requestBody(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return LogPage{}, err
	}
	var page LogPage
	if json.Unmarshal(body, &page) != nil || page.AttemptID != attemptID || page.Stream != stream || page.Segments == nil || len(page.Segments) > 100 || len(page.NextCursor) > 256 || len(page.Segments) > 0 && page.NextCursor == "" {
		return LogPage{}, errors.New("invalid log page")
	}
	if len(page.Segments) == 0 && (page.HasMore || page.NextCursor != cursor) {
		return LogPage{}, errors.New("invalid empty log page")
	}
	var previous uint64
	for _, segment := range page.Segments {
		if segment.Stream != stream || segment.FirstSequence == 0 || segment.LastSequence < segment.FirstSequence || segment.LastSequence > 1<<63-1 || segment.FirstSequence <= previous || len(segment.Gaps) > 1024 || segment.Object.SizeBytes < 1 || segment.Object.SizeBytes > logformat.MaxSegmentBytes {
			return LogPage{}, errors.New("invalid log segment metadata")
		}
		previous = segment.LastSequence
		if err := c.validateGrant(segment.grant()); err != nil {
			return LogPage{}, errors.New("invalid log download grant")
		}
		var lastGap uint64
		for _, gap := range segment.Gaps {
			if gap.FirstSequence < segment.FirstSequence || gap.LastSequence < gap.FirstSequence || gap.LastSequence > segment.LastSequence || gap.FirstSequence <= lastGap {
				return LogPage{}, errors.New("invalid log segment gaps")
			}
			lastGap = gap.LastSequence
		}
	}
	if page.Completion != nil {
		if page.Completion.Gaps == nil || len(page.Completion.Gaps) > 1024 || page.Completion.LogsComplete && len(page.Completion.Gaps) != 0 {
			return LogPage{}, errors.New("invalid log completion")
		}
		for _, gap := range page.Completion.Gaps {
			if !slices.Contains([]string{"stdout", "stderr"}, gap.Stream) || gap.First == 0 || gap.Last < gap.First || gap.Last > 1<<63-1 {
				return LogPage{}, errors.New("invalid log completion gap")
			}
		}
	}
	return page, nil
}

func (s LogSegment) grant() artifactGrant {
	return artifactGrant{Name: s.Stream, ArtifactID: s.ArtifactID, Object: s.Object, DownloadURL: s.DownloadURL,
		Method: s.Method, RequiredHeaders: s.RequiredHeaders, ExpiresAt: s.ExpiresAt}
}

func (c *Client) FetchLogSegment(ctx context.Context, attemptID string, segment LogSegment) (logformat.Segment, error) {
	grant := segment.grant()
	if !downloadUUID(attemptID) || segment.Object.SizeBytes < 1 || segment.Object.SizeBytes > logformat.MaxSegmentBytes || c.validateGrant(grant) != nil {
		return logformat.Segment{}, errors.New("invalid or expired log grant")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, grant.DownloadURL, nil)
	if err != nil {
		return logformat.Segment{}, errors.New("invalid log download request")
	}
	for key, values := range grant.RequiredHeaders {
		if strings.EqualFold(key, "Host") {
			request.Host = values[0]
		} else {
			request.Header.Set(key, values[0])
		}
	}
	// Storage grants are bearer capabilities. Never send project credentials,
	// proxies, cookies, redirects, or unbounded response bodies to storage.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.MaxResponseHeaderBytes = 64 << 10
	transport.ResponseHeaderTimeout = 10 * time.Second
	defer transport.CloseIdleConnections()
	transfer := &http.Client{Transport: transport, Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := transfer.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return logformat.Segment{}, ctx.Err()
		}
		return logformat.Segment{}, errors.New("log transfer failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("X-Amz-Version-Id") != grant.Object.Version || response.Header.Get("Content-Encoding") != "" || response.ContentLength >= 0 && response.ContentLength != grant.Object.SizeBytes {
		return logformat.Segment{}, errors.New("log response version or size mismatch")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, grant.Object.SizeBytes+1))
	if err != nil || int64(len(raw)) != grant.Object.SizeBytes {
		return logformat.Segment{}, errors.New("log response size mismatch")
	}
	hash := sha256.Sum256(raw)
	if hex.EncodeToString(hash[:]) != grant.Object.SHA256 {
		return logformat.Segment{}, errors.New("log response checksum mismatch")
	}
	decoded, err := logformat.Decode(raw)
	if err != nil || decoded.AttemptID != attemptID || decoded.Stream != segment.Stream {
		return logformat.Segment{}, errors.New("invalid log object identity")
	}
	if err := verifyLogRange(decoded, segment); err != nil {
		return logformat.Segment{}, err
	}
	return decoded, nil
}

func verifyLogRange(decoded logformat.Segment, metadata LogSegment) error {
	next := metadata.FirstSequence
	gaps := make([]LogGap, 0, len(metadata.Gaps))
	for _, record := range decoded.Records {
		if record.Sequence < next || record.Sequence > metadata.LastSequence {
			return errors.New("invalid log record range")
		}
		if record.Sequence > next {
			gaps = append(gaps, LogGap{FirstSequence: next, LastSequence: record.Sequence - 1})
		}
		next = record.Sequence + 1
	}
	if next != metadata.LastSequence+1 || !slices.Equal(gaps, metadata.Gaps) {
		return errors.New("log object does not match catalog range")
	}
	return nil
}
