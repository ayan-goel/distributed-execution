package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCollectSweepRejectsChangedMembershipAcrossPages(t *testing.T) {
	second := strings.Replace(progressResponse(), `"index":0`, `"index":1`, 1)
	second = strings.Replace(second, `00000000-0000-0000-0000-000000000003`, `00000000-0000-0000-0000-000000000005`, 1)
	second = strings.Replace(second, `00000000-0000-0000-0000-000000000004`, `00000000-0000-0000-0000-000000000006`, 1)
	second = strings.Replace(second, `"state":"ACTIVE"`, `"state":"SUCCEEDED"`, 1)
	second = strings.Replace(second, `"queued":1,"succeeded":1`, `"queued":0,"succeeded":2`, 1)
	second = strings.Replace(second, `"hasMore":true,"nextCursor":"next"`, `"hasMore":false,"nextCursor":""`, 1)
	for _, tc := range []struct{ name, body string }{
		{"valid", second},
		{"duplicate child", strings.Replace(second, `00000000-0000-0000-0000-000000000005`, `00000000-0000-0000-0000-000000000003`, 1)},
		{"changed spec", strings.Replace(second, strings.Repeat("a", 64), strings.Repeat("b", 64), 1)},
		{"changed project", strings.Replace(second, `00000000-0000-0000-0000-000000000002`, `00000000-0000-0000-0000-000000000009`, 1)},
		{"empty end", strings.Split(second, `"children":`)[0] + `"children":[],"hasMore":false,"nextCursor":""}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			c, _ := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				body := progressResponse()
				if calls == 2 {
					if r.URL.Query().Get("cursor") != "next" {
						t.Error("continuation cursor lost")
					}
					body = tc.body
				}
				if calls > 2 {
					t.Fatal("collection did not stop")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
			}))
			result, err := c.CollectSweep(context.Background(), progressSweepID)
			if (err == nil) != (tc.name == "valid") || calls != 2 {
				t.Fatal("collection validation", calls, err)
			}
			if err == nil && (len(result.Children) != 2 || result.Children[1].Index != 1) {
				t.Fatal("collection omitted a child", result)
			}
		})
	}
}

func TestCollectSweepStopsAtExportByteLimit(t *testing.T) {
	const total = 1000
	metrics := map[string]json.Number{}
	for i := range 60 {
		metrics[fmt.Sprintf("m%d", i)] = json.Number("0." + strings.Repeat("1", 990))
	}
	calls, next := 0, 0
	c, _ := New("https://dispatch.example.org", "private", false, transportFunc(func(*http.Request) (*http.Response, error) {
		calls++
		page := SweepPage{Sweep: SweepSummary{ID: progressSweepID, ProjectID: "00000000-0000-0000-0000-000000000002", Name: "large", State: "SUCCEEDED", SpecHash: strings.Repeat("a", 64), MaxConcurrent: 1, CreatedAt: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC), Progress: SweepProgress{Total: total, Succeeded: total}}, Children: []SweepChild{}}
		for range 25 {
			if next == total {
				break
			}
			attempt := fmt.Sprintf("00000000-0000-0001-0000-%012d", next+1)
			page.Children = append(page.Children, SweepChild{ID: fmt.Sprintf("00000000-0000-0002-0000-%012d", next+1), Index: next, State: "SUCCEEDED", Parameters: map[string]string{"SEED": fmt.Sprint(next)}, AcceptedAttemptID: &attempt, Metrics: metrics})
			next++
		}
		page.HasMore = next < total
		if page.HasMore {
			page.NextCursor = fmt.Sprintf("page-%d", calls)
		}
		body, err := json.Marshal(page)
		if err != nil || len(body) > 2<<20 {
			t.Fatal("invalid size fixture", len(body), err)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(body))), Header: http.Header{}}, nil
	}))
	result, err := c.CollectSweep(context.Background(), progressSweepID)
	if err == nil || !strings.Contains(err.Error(), "32 MiB") || len(result.Children) != 0 || calls >= 40 {
		t.Fatal("oversized export was not stopped", calls, len(result.Children), err)
	}
}
