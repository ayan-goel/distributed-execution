package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

type logCursor struct {
	Version   int    `json:"v"`
	AttemptID string `json:"a"`
	Stream    string `json:"s"`
	After     uint64 `json:"n"`
}

type LogSegmentDownload struct {
	store.LogSegment
	DownloadURL     string      `json:"downloadUrl"`
	Method          string      `json:"method"`
	RequiredHeaders http.Header `json:"requiredHeaders"`
	ExpiresAt       time.Time   `json:"expiresAt"`
}

type LogList struct {
	AttemptID  string               `json:"attemptId"`
	Stream     string               `json:"stream"`
	Segments   []LogSegmentDownload `json:"segments"`
	NextCursor string               `json:"nextCursor"`
	HasMore    bool                 `json:"hasMore"`
	Completion *store.LogCompletion `json:"completion"`
}

func parseLogQuery(raw, attemptID string) (string, uint64, int, string, error) {
	q, err := url.ParseQuery(raw)
	if err != nil {
		return "", 0, 0, "", err
	}
	for key, values := range q {
		if (key != "stream" && key != "cursor" && key != "limit") || len(values) != 1 {
			return "", 0, 0, "", store.ErrInvalid
		}
	}
	stream := q.Get("stream")
	if stream != "stdout" && stream != "stderr" {
		return "", 0, 0, "", store.ErrInvalid
	}
	limit := 50
	if values, present := q["limit"]; present {
		value := values[0]
		limit, err = strconv.Atoi(value)
		if err != nil || limit < 1 || limit > store.MaxLogPageSize {
			return "", 0, 0, "", store.ErrInvalid
		}
	}
	value := q.Get("cursor")
	if value == "" {
		if _, present := q["cursor"]; present {
			return "", 0, 0, "", store.ErrInvalid
		}
		return strings.ToUpper(stream), 0, limit, "", nil
	}
	if len(value) > 256 {
		return "", 0, 0, "", store.ErrInvalid
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", 0, 0, "", store.ErrInvalid
	}
	var cursor logCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil || cursor.Version != 1 || cursor.AttemptID != attemptID || cursor.Stream != stream || cursor.After > math.MaxInt64 {
		return "", 0, 0, "", store.ErrInvalid
	}
	return strings.ToUpper(stream), cursor.After, limit, value, nil
}

func encodeLogCursor(attemptID, stream string, after uint64) string {
	body, _ := json.Marshal(logCursor{Version: 1, AttemptID: attemptID, Stream: strings.ToLower(stream), After: after})
	return base64.RawURLEncoding.EncodeToString(body)
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request, p store.Principal) {
	if !p.Allows(store.RoleRead) {
		fail(w, 403, "FORBIDDEN", "read permission required", false)
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/attempts/"), "/logs")
	parsed, err := uuid.Parse(id)
	if err != nil {
		fail(w, 404, "NOT_FOUND", "attempt not found", false)
		return
	}
	stream, after, limit, cursor, err := parseLogQuery(r.URL.RawQuery, parsed.String())
	if err != nil {
		fail(w, 400, "INVALID_ARGUMENT", "invalid log query or cursor", false)
		return
	}
	page, err := store.ListLogSegments(r.Context(), s.pool, p.ProjectID, parsed.String(), stream, after, limit)
	if err != nil {
		s.storeError(w, err)
		return
	}
	response := LogList{AttemptID: parsed.String(), Stream: strings.ToLower(stream), Segments: []LogSegmentDownload{}, NextCursor: cursor, HasMore: page.More, Completion: page.Completion}
	if len(page.Segments) > 0 && s.objects == nil {
		fail(w, 503, "OBJECT_STORAGE_NOT_CONFIGURED", "log downloads are not configured", false)
		return
	}
	for _, segment := range page.Segments {
		// INVARIANT: sign only an authorized catalog entry's exact version.
		// The returned URL is a short-lived bearer capability, never a log field.
		object := segment.Object
		grant, err := s.objects.PresignDownload(r.Context(), objectstore.Object{Key: object.Key, Version: object.Version, Size: object.SizeBytes, SHA256: object.SHA256}, time.Minute)
		if err != nil {
			fail(w, 503, "OBJECT_STORAGE_UNAVAILABLE", "log download signing unavailable", true)
			return
		}
		segment.Stream = strings.ToLower(segment.Stream)
		response.Segments = append(response.Segments, LogSegmentDownload{LogSegment: segment, DownloadURL: grant.URL, Method: grant.Method, RequiredHeaders: grant.Headers, ExpiresAt: grant.ExpiresAt.UTC()})
	}
	if len(page.Segments) > 0 {
		response.NextCursor = encodeLogCursor(parsed.String(), stream, page.Segments[len(page.Segments)-1].LastSequence)
	}
	// Recheck revocation after signing so a token revoked during adapter work
	// cannot receive newly minted bearer download grants.
	current, err := store.Authenticate(r.Context(), s.pool, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if err != nil {
		if errors.Is(err, store.ErrUnauthorized) {
			fail(w, 401, "UNAUTHENTICATED", "invalid credentials", false)
		} else {
			fail(w, 503, "UNAVAILABLE", "authentication unavailable", true)
		}
		return
	}
	if current.ProjectID != p.ProjectID || !current.Allows(store.RoleRead) {
		fail(w, 403, "FORBIDDEN", "project access denied", false)
		return
	}
	writeJSON(w, 200, response)
}
