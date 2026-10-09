package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

const progressSweepID = "00000000-0000-0000-0000-000000000001"

func progressResponse() string {
	return `{"sweep":{"id":"` + progressSweepID + `","projectId":"00000000-0000-0000-0000-000000000002","name":"grid","state":"ACTIVE","specHash":"` + strings.Repeat("a", 64) + `","maxConcurrent":1,"createdAt":"2026-10-08T00:00:00Z","progress":{"total":2,"queued":1,"succeeded":1}},"children":[{"id":"00000000-0000-0000-0000-000000000003","index":0,"state":"SUCCEEDED","parameters":{"SEED":"1"},"acceptedAttemptId":"00000000-0000-0000-0000-000000000004","metrics":{"score":9007199254740993}}],"hasMore":true,"nextCursor":"next"}`
}

func TestSweepProgressClientValidatesPagesAndPreservesMetrics(t *testing.T) {
	for _, tc := range []struct{ name, old, new string }{
		{"valid", "", ""},
		{"wrong sweep", `"id":"` + progressSweepID, `"id":"00000000-0000-0000-0000-000000000009`},
		{"invalid counts", `"queued":1`, `"queued":2`},
		{"negative count", `"queued":1`, `"queued":-1`},
		{"wrong aggregate state", `"state":"ACTIVE"`, `"state":"SUCCEEDED"`},
		{"missing acceptance", `"acceptedAttemptId":"00000000-0000-0000-0000-000000000004"`, `"acceptedAttemptId":null`},
		{"unaccepted metric", `"state":"SUCCEEDED","parameters"`, `"state":"FAILED","parameters"`},
		{"infinite metric", `9007199254740993`, `1e999`},
		{"underflow metric", `9007199254740993`, `1e-999`},
		{"quoted metric", `9007199254740993`, `"12"`},
		{"missing cursor", `"nextCursor":"next"`, `"nextCursor":""`},
		{"truncated page", `"hasMore":true,"nextCursor":"next"`, `"hasMore":false,"nextCursor":""`},
		{"skipped first child", `"index":0`, `"index":1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := progressResponse()
			if tc.old != "" {
				body = strings.Replace(body, tc.old, tc.new, 1)
			}
			c, err := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != "GET" || r.URL.Path != "/v1/sweeps/"+progressSweepID || r.URL.Query().Get("limit") != "1" || r.Header.Get("Authorization") != "Bearer private" {
					t.Error("incorrect sweep progress request")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			page, err := c.GetSweep(context.Background(), progressSweepID, "", 1)
			if (err == nil) != (tc.name == "valid") {
				t.Fatal("page validation", err)
			}
			if err == nil && page.Children[0].Metrics["score"] != json.Number("9007199254740993") {
				t.Fatal("metric precision changed")
			}
		})
	}
}

func TestSweepProgressClientRejectsInvalidArgumentsBeforeIO(t *testing.T) {
	c, _ := New("https://dispatch.example.org", "private", false, transportFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid arguments reached transport")
		return nil, nil
	}))
	for _, tc := range []struct {
		id, cursor string
		limit      int
	}{{"bad", "", 1}, {"00000000-0000-0000-0000-000000000000", "", 1}, {progressSweepID, "", 0}, {progressSweepID, "", 101}, {progressSweepID, strings.Repeat("a", 257), 1}, {progressSweepID, "\n", 1}} {
		if _, err := c.GetSweep(context.Background(), tc.id, tc.cursor, tc.limit); err == nil {
			t.Fatal("invalid sweep arguments accepted", tc)
		}
	}
}
