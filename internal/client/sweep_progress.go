package client

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/spec"
)

type SweepProgress struct {
	Total      int `json:"total"`
	Queued     int `json:"queued"`
	RetryWait  int `json:"retryWait"`
	Active     int `json:"active"`
	Cancelling int `json:"cancelling"`
	Succeeded  int `json:"succeeded"`
	Failed     int `json:"failed"`
	Cancelled  int `json:"cancelled"`
}

type SweepSummary struct {
	ID                     string        `json:"id"`
	ProjectID              string        `json:"projectId"`
	Name                   string        `json:"name"`
	State                  string        `json:"state"`
	SpecHash               string        `json:"specHash"`
	MaxConcurrent          int           `json:"maxConcurrent"`
	FailFast               bool          `json:"failFast"`
	CancelRunningOnFailure bool          `json:"cancelRunningOnFailure"`
	CreatedAt              time.Time     `json:"createdAt"`
	Progress               SweepProgress `json:"progress"`
}

type SweepChild struct {
	ID                string                 `json:"id"`
	Index             int                    `json:"index"`
	State             string                 `json:"state"`
	Parameters        map[string]string      `json:"parameters"`
	CurrentAttemptID  *string                `json:"currentAttemptId"`
	AcceptedAttemptID *string                `json:"acceptedAttemptId"`
	Metrics           map[string]json.Number `json:"metrics"`
}

type SweepPage struct {
	Sweep      SweepSummary `json:"sweep"`
	Children   []SweepChild `json:"children"`
	HasMore    bool         `json:"hasMore"`
	NextCursor string       `json:"nextCursor"`
}

var sweepParameterName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
var sweepMetricName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$`)

func (c *Client) GetSweep(ctx context.Context, id, cursor string, limit int) (SweepPage, error) {
	if !downloadUUID(id) || limit < 1 || limit > 100 || cursor != "" && !visibleASCII(cursor, 256) {
		return SweepPage{}, errors.New("sweep inspection requires a canonical UUID, limit 1–100, and a bounded cursor")
	}
	query := url.Values{"limit": {strconv.Itoa(limit)}}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	body, err := c.requestBody(ctx, http.MethodGet, "/v1/sweeps/"+id+"?"+query.Encode(), nil, "")
	if err != nil {
		return SweepPage{}, err
	}
	// Keep metric tokens raw until their scalar type is checked. json.Number
	// alone also accepts quoted numbers, which are outside the result contract.
	var wire struct {
		SweepPage
		Children []struct {
			SweepChild
			Metrics map[string]json.RawMessage `json:"metrics"`
		} `json:"children"`
	}
	invalid := errors.New("invalid sweep progress response")
	if json.Unmarshal(body, &wire) != nil || wire.Children == nil || len(wire.Children) > limit || !validSweepSummary(wire.Sweep, id) {
		return SweepPage{}, invalid
	}
	page := wire.SweepPage
	page.Children = make([]SweepChild, 0, len(wire.Children))
	seen := map[string]bool{}
	counts := map[string]int{}
	previous := -1
	for _, item := range wire.Children {
		child := item.SweepChild
		if !downloadUUID(child.ID) || seen[child.ID] || child.Index < 0 || child.Index >= page.Sweep.Progress.Total ||
			previous >= 0 && child.Index != previous+1 || !slices.Contains([]string{"QUEUED", "RETRY_WAIT", "ACTIVE", "CANCELLING", "SUCCEEDED", "FAILED", "CANCELLED"}, child.State) ||
			child.Parameters == nil || len(child.Parameters) < 1 || len(child.Parameters) > 32 || item.Metrics == nil || len(item.Metrics) > 256 {
			return SweepPage{}, invalid
		}
		for _, attempt := range []*string{child.CurrentAttemptID, child.AcceptedAttemptID} {
			if attempt != nil && !downloadUUID(*attempt) {
				return SweepPage{}, invalid
			}
		}
		if (child.State == "SUCCEEDED") != (child.AcceptedAttemptID != nil) || child.State != "SUCCEEDED" && len(item.Metrics) != 0 {
			return SweepPage{}, invalid
		}
		for name, value := range child.Parameters {
			if !sweepParameterName.MatchString(name) || strings.HasPrefix(name, "DISPATCH_") || len(value) > 8192 || strings.ContainsRune(value, 0) {
				return SweepPage{}, invalid
			}
		}
		child.Metrics = make(map[string]json.Number, len(item.Metrics))
		for name, raw := range item.Metrics {
			// JSONB can expand a bounded exponent token into a longer decimal.
			// Allow that finite range while retaining a per-value memory bound.
			if !sweepMetricName.MatchString(name) || len(raw) == 0 || len(raw) > 2048 || raw[0] != '-' && (raw[0] < '0' || raw[0] > '9') {
				return SweepPage{}, invalid
			}
			number := json.Number(raw)
			value, err := number.Float64()
			if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
				return SweepPage{}, invalid
			}
			// A nonzero decimal underflow must not silently become a zero result.
			mantissa, _, _ := strings.Cut(strings.ToLower(number.String()), "e")
			if value == 0 && strings.Trim(mantissa, "-+.0") != "" {
				return SweepPage{}, invalid
			}
			child.Metrics[name] = number
		}
		page.Children = append(page.Children, child)
		counts[child.State]++
		previous, seen[child.ID] = child.Index, true
	}
	p := page.Sweep.Progress
	for state, count := range map[string]int{"QUEUED": p.Queued, "RETRY_WAIT": p.RetryWait, "ACTIVE": p.Active, "CANCELLING": p.Cancelling, "SUCCEEDED": p.Succeeded, "FAILED": p.Failed, "CANCELLED": p.Cancelled} {
		if counts[state] > count {
			return SweepPage{}, invalid
		}
	}
	// Stable membership makes gaps and premature end-of-results detectable even
	// though each page can observe newer job states than the previous request.
	if cursor == "" && (len(page.Children) == 0 || page.Children[0].Index != 0) ||
		page.HasMore && (len(page.Children) == 0 || previous >= p.Total-1 || !visibleASCII(page.NextCursor, 256) || page.NextCursor == cursor) ||
		!page.HasMore && (page.NextCursor != "" || len(page.Children) > 0 && previous != p.Total-1) {
		return SweepPage{}, invalid
	}
	return page, nil
}

func validSweepSummary(s SweepSummary, id string) bool {
	p := s.Progress
	if s.ID != id || !downloadUUID(s.ProjectID) || s.Name == "" || len(s.Name) > 128 || !objectSHA.MatchString(s.SpecHash) || s.CreatedAt.IsZero() ||
		p.Total < 1 || p.Total > spec.MaxSweepJobs || s.MaxConcurrent < 1 || s.MaxConcurrent > spec.MaxSweepJobs || s.CancelRunningOnFailure && !s.FailFast {
		return false
	}
	total := 0
	for _, count := range []int{p.Queued, p.RetryWait, p.Active, p.Cancelling, p.Succeeded, p.Failed, p.Cancelled} {
		if count < 0 || count > p.Total {
			return false
		}
		total += count
	}
	state := "ACTIVE"
	if p.Succeeded+p.Failed+p.Cancelled == p.Total {
		switch {
		case p.Failed > 0:
			state = "FAILED"
		case p.Cancelled > 0:
			state = "CANCELLED"
		default:
			state = "SUCCEEDED"
		}
	} else if p.Queued == p.Total {
		state = "QUEUED"
	}
	return total == p.Total && state == s.State
}
