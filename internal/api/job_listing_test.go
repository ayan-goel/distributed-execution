package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/store"
)

func TestJobCursorRejectsAmbiguousEncodingAndPreservesFilterIdentity(t *testing.T) {
	p := store.Principal{Project: "research", ProjectID: "00000000-0000-0000-0000-000000000001"}
	f := store.JobListFilter{State: "QUEUED", Labels: map[string]string{"cohort": "a=b"}}
	position := store.JobListPosition{ID: "00000000-0000-0000-0000-000000000002", CreatedAt: time.Date(2026, 10, 1, 12, 0, 0, 123456000, time.UTC)}
	cursor, err := encodeJobCursor(p.ProjectID, f, position)
	if err != nil || len(cursor) > 512 {
		t.Fatal(cursor, err)
	}
	filter, after, limit, err := parseJobQuery("label=cohort%3Da%3Db&state=QUEUED&limit=100&cursor="+cursor, p)
	if err != nil || filter.Labels["cohort"] != "a=b" || after == nil || *after != position || limit != 100 {
		t.Fatal(filter, after, limit, err)
	}
	body, _ := base64.RawURLEncoding.DecodeString(cursor)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	invalid := []string{string(body) + " ", strings.Replace(string(body), `"v":1`, `"v":1,"v":1`, 1), strings.Replace(string(body), `"v":1`, `"v":1,"extra":1`, 1)}
	for _, field := range []string{"v", "p", "f", "t", "id"} {
		copy := make(map[string]json.RawMessage, len(raw))
		for k, v := range raw {
			copy[k] = v
		}
		delete(copy, field)
		b, _ := json.Marshal(copy)
		invalid = append(invalid, string(b))
	}
	for _, replacement := range [][2]string{{`"v":1`, `"v":2`}, {`"v":1`, `"v":null`}, {position.ID, "00000000-0000-0000-0000-000000000000"}, {`2026-10-01T12:00:00.123456Z`, `0001-01-01T00:00:00Z`}} {
		invalid = append(invalid, strings.Replace(string(body), replacement[0], replacement[1], 1))
	}
	for _, body := range invalid {
		bad := base64.RawURLEncoding.EncodeToString([]byte(body))
		if _, _, _, err := parseJobQuery("state=QUEUED&label=cohort=a%3Db&cursor="+url.QueryEscape(bad), p); err == nil {
			t.Fatal("invalid cursor accepted", body)
		}
	}
	if jobFilterHash(store.JobListFilter{}) != jobFilterHash(store.JobListFilter{Labels: map[string]string{}}) {
		t.Fatal("nil and empty label filters have different identities")
	}
	// The full public envelope and largest cursor fit the store's reservation.
	page := JobPage{Project: strings.Repeat("p", 128), ProjectID: p.ProjectID, JobListPage: store.JobListPage{Jobs: []store.JobSummary{}, HasMore: true}, NextCursor: strings.Repeat("x", 512)}
	b, _ := json.Marshal(page)
	if len(b)+1 > 1024 {
		t.Fatal("public envelope exceeds reserved bytes", len(b))
	}
}

func TestJobListingRequiresReadRoleBeforeDatabaseAccess(t *testing.T) {
	w := httptest.NewRecorder()
	New(nil, nil, nil).listJobs(w, httptest.NewRequest("GET", "/v1/jobs", nil), store.Principal{Role: "unknown"})
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
}
