//go:build integration

package api

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

type testEventPage struct {
	JobID  string `json:"jobId"`
	Events []struct {
		Sequence  int64           `json:"sequence"`
		AttemptID *string         `json:"attemptId"`
		Type      string          `json:"type"`
		Payload   json.RawMessage `json:"payload"`
		CreatedAt time.Time       `json:"createdAt"`
	} `json:"events"`
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor"`
}

func TestHTTPJobEventsPageRealTransitionsAndResume(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	issue := func(project string, role store.Role) string {
		t.Helper()
		token, _, err := store.IssueToken(ctx, pool, project, role)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	submit, reader, foreign := issue("research", store.RoleSubmit), issue("research", store.RoleRead), issue("other", store.RoleRead)
	h := New(pool, resolverFunc(func(context.Context, string) (string, error) {
		return "registry.example.org/eval@sha256:" + strings.Repeat("a", 64), nil
	}), nil)
	w := call(h, "POST", "/v1/jobs", submit, "events-job", jobBody(t))
	var job store.JobRecord
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &job) != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	path := "/v1/jobs/" + job.ID + "/events"
	read := func(query string) testEventPage {
		t.Helper()
		w := call(h, "GET", path+query, reader, "", nil)
		var page testEventPage
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || page.JobID != job.ID || page.Events == nil || page.NextCursor == "" {
			t.Fatal(w.Code, w.Body.String())
		}
		return page
	}
	first := read("?limit=1")
	if len(first.Events) != 1 || first.Events[0].Sequence != 1 || first.Events[0].Type != "SUBMITTED" || first.Events[0].AttemptID != nil || first.Events[0].CreatedAt.IsZero() || !strings.Contains(string(first.Events[0].Payload), job.SpecHash) || first.HasMore {
		t.Fatal(first)
	}
	empty := read("?cursor=" + url.QueryEscape(first.NextCursor))
	if len(empty.Events) != 0 || empty.HasMore || empty.NextCursor != first.NextCursor {
		t.Fatal("empty page lost continuation", empty)
	}
	if w := call(h, "POST", "/v1/jobs/"+job.ID+"/cancel", submit, "", nil); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	next := read("?limit=100&cursor=" + url.QueryEscape(empty.NextCursor))
	if len(next.Events) != 1 || next.Events[0].Sequence != 2 || next.Events[0].Type != "CANCEL_REQUESTED" || next.HasMore || !strings.Contains(string(next.Events[0].Payload), "CANCELLED") {
		t.Fatal(next)
	}
	first = read("?limit=1")
	if !first.HasMore {
		t.Fatal("lookahead missed next row", first)
	}
	// Continuation compares sequence values; it must survive removal of an anchor.
	if _, err := pool.Exec(ctx, "DELETE FROM job_events WHERE job_id=$1 AND sequence=1", job.ID); err != nil {
		t.Fatal(err)
	}
	if page := read("?cursor=" + url.QueryEscape(first.NextCursor)); len(page.Events) != 1 || page.Events[0].Sequence != 2 {
		t.Fatal("deleted anchor broke continuation", page)
	}
	for _, query := range []string{"?limit=0", "?limit=101", "?limit=1&limit=2", "?cursor=", "?cursor=bad", "?state=QUEUED", "?cursor=" + encodeEventCursor(job.ProjectID, uuid.NewString(), 1)} {
		w := call(h, "GET", path+query, reader, "", nil)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "INVALID_ARGUMENT") {
			t.Fatal("invalid query did not return stable error", query, w.Code, w.Body.String())
		}
	}
	for _, tc := range []struct {
		token, path string
		status      int
	}{
		{foreign, path, 404}, {"", path, 401}, {reader, "/v1/jobs/" + uuid.NewString() + "/events", 404},
		{reader, "/v1/jobs/../events", 404},
	} {
		w := call(h, "GET", tc.path, tc.token, "", nil)
		if w.Code != tc.status || strings.Contains(w.Body.String(), job.SpecHash) {
			t.Fatal("unauthorized/invalid read", w.Code, w.Body.String())
		}
	}
	// Large HTML-escaped payloads fit the database limit but expand on the wire.
	// Exercise byte-budget pagination rather than relying only on the row limit.
	if _, err := pool.Exec(ctx, `INSERT INTO job_events(job_id,sequence,type,payload)
		SELECT $1,n,'TEST_LARGE',jsonb_build_object('value',repeat('<',64000)) FROM generate_series(3,12) n`, job.ID); err != nil {
		t.Fatal(err)
	}
	cursor, last, count := "", int64(1), 0
	for pageNumber := 0; ; pageNumber++ {
		w := call(h, "GET", path+"?limit=100"+cursor, reader, "", nil)
		var page testEventPage
		if w.Code != 200 || len(w.Body.Bytes()) > store.MaxEventPageBytes || json.Unmarshal(w.Body.Bytes(), &page) != nil || len(page.Events) == 0 {
			t.Fatal("invalid size-bounded page", w.Code, w.Body.Len())
		}
		for _, event := range page.Events {
			if event.Sequence != last+1 {
				t.Fatal("event skipped/repeated", event.Sequence, last)
			}
			last = event.Sequence
			count++
			if event.Type == "TEST_LARGE" {
				var payload map[string]string
				if json.Unmarshal(event.Payload, &payload) != nil || payload["value"] != strings.Repeat("<", 64000) {
					t.Fatal("payload changed")
				}
			}
		}
		if !page.HasMore {
			if count != 11 || last != 12 || pageNumber == 0 {
				t.Fatal("incomplete byte-budget traversal", count, last, pageNumber)
			}
			break
		}
		if pageNumber > 12 || page.NextCursor == "" || cursor == "&cursor="+url.QueryEscape(page.NextCursor) {
			t.Fatal("pagination did not advance")
		}
		cursor = "&cursor=" + url.QueryEscape(page.NextCursor)
	}
}
