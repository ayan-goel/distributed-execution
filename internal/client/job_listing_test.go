package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

const listedProject = "00000000-0000-0000-0000-000000000001"
const listedJob = "00000000-0000-0000-0000-000000000002"

func listedPageBody() string {
	return `{"project":"research","projectId":"` + listedProject + `","jobs":[{"id":"` + listedJob + `","projectId":"` + listedProject + `","name":"training","state":"QUEUED","labels":{"cohort":"a=b","empty":""},"priority":0,"createdAt":"2026-10-01T12:00:00Z"}],"hasMore":false,"nextCursor":""}`
}

func listingClient(t *testing.T, body string) *Client {
	t.Helper()
	c, err := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClientJobListingValidatesPageAndArguments(t *testing.T) {
	opts := JobListOptions{Project: "research", State: "QUEUED", Labels: map[string]string{"cohort": "a=b", "empty": ""}, Limit: 50}
	c := listingClient(t, listedPageBody())
	page, err := c.ListJobs(context.Background(), opts)
	if err != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != listedJob {
		t.Fatal(page, err)
	}
	for _, replace := range [][2]string{
		{`"project":"research"`, `"project":"other"`}, {`"projectId":"` + listedProject + `"`, `"projectId":"bad"`},
		{`"jobs":[`, `"jobs":null,"unused":[`}, {`"hasMore":false`, `"hasMore":true`},
		{`"nextCursor":""`, `"nextCursor":"unexpected"`}, {`"priority":0`, `"priority":null`}, {`"priority":0`, `"priority":4`},
		{`"state":"QUEUED"`, `"state":"ACTIVE"`}, {`"state":"QUEUED"`, `"state":"unknown"`},
		{`"createdAt":"2026-10-01T12:00:00Z"`, `"createdAt":"0001-01-01T00:00:00Z"`},
		{`"createdAt":"2026-10-01T12:00:00Z"`, `"createdAt":"0001-01-01T00:01:00+01:00"`},
		{`"name":"training"`, `"name":"\u001b[31m"`}, {`"labels":{"cohort":"a=b","empty":""}`, `"labels":{}`},
		{`"labels":{"cohort":"a=b","empty":""}`, `"labels":null`},
		{`"labels":{"cohort":"a=b","empty":""}`, `"labels":{"cohort":"a=b","cohort":"a=b","empty":""}`},
		{`"hasMore":false`, `"hasMore":false,"hasMore":false`}, {`"priority":0`, `"priority":0,"priority":0`},
	} {
		body := strings.Replace(listedPageBody(), replace[0], replace[1], 1)
		if _, err := listingClient(t, body).ListJobs(context.Background(), opts); err == nil {
			t.Fatal("invalid listing accepted", replace)
		}
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(listedPageBody()), &raw); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"project", "projectId", "jobs", "hasMore", "nextCursor"} {
		copy := make(map[string]json.RawMessage, len(raw))
		for k, v := range raw {
			copy[k] = v
		}
		delete(copy, field)
		body, _ := json.Marshal(copy)
		if _, err := listingClient(t, string(body)).ListJobs(context.Background(), opts); err == nil {
			t.Fatal("missing field accepted", field)
		}
	}
	c, _ = New("https://dispatch.example.org", "private", false, transportFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid options reached transport")
		return nil, nil
	}))
	for _, bad := range []JobListOptions{{Limit: 0}, {Limit: 101}, {Limit: 1, Project: "bad project"}, {Limit: 1, State: "running"}, {Limit: 1, Cursor: strings.Repeat("a", 513)}, {Limit: 1, Labels: map[string]string{"bad key": "x"}}, {Limit: 1, Labels: map[string]string{"key": "\x00"}}, {Limit: 1, Labels: map[string]string{"key": "\xff"}}, {Limit: 1, Labels: map[string]string{"key": strings.Repeat("<", 8192)}}} {
		if _, err := c.ListJobs(context.Background(), bad); err == nil {
			t.Fatal("invalid listing options accepted")
		}
	}
}

func TestClientJobListingPreservesQueryEncodingAndBounds(t *testing.T) {
	c, _ := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
		q := r.URL.Query()
		if r.Method != "GET" || r.URL.Path != "/v1/jobs" || q.Get("project") != "research" || q.Get("cursor") != "opaque" || q.Get("limit") != "1" || len(q["label"]) != 1 || q["label"][0] != "eq=a=b &+" || r.Header.Get("Authorization") != "Bearer private" {
			t.Error("listing request changed", r.URL.String())
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"project":"research","projectId":"` + listedProject + `","jobs":[],"hasMore":false,"nextCursor":""}`)), Header: http.Header{}}, nil
	}))
	if _, err := c.ListJobs(context.Background(), JobListOptions{Project: "research", Cursor: "opaque", Limit: 1, Labels: map[string]string{"eq": "a=b &+"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := listingClient(t, strings.Repeat(" ", (8<<20)+1)).ListJobs(context.Background(), JobListOptions{Limit: 1}); err == nil {
		t.Fatal("oversized listing accepted")
	}
}

func TestClientJobListingRejectsMixedScopeAndIncorrectOrdering(t *testing.T) {
	var page struct{ Jobs []json.RawMessage }
	if json.Unmarshal([]byte(listedPageBody()), &page) != nil {
		t.Fatal("bad fixture")
	}
	first := string(page.Jobs[0])
	for _, tc := range []struct {
		row   string
		valid bool
	}{
		{strings.Replace(first, listedJob, "00000000-0000-0000-0000-000000000001", 1), true},
		{first, false},
		{strings.Replace(first, listedJob, "00000000-0000-0000-0000-000000000003", 1), false},
		{strings.Replace(strings.Replace(first, listedJob, "00000000-0000-0000-0000-000000000003", 1), "2026-10-01", "2026-10-02", 1), false},
		{strings.Replace(first, `"projectId":"`+listedProject+`"`, `"projectId":"00000000-0000-0000-0000-000000000003"`, 1), false},
	} {
		body := strings.Replace(listedPageBody(), `],"hasMore"`, `,`+tc.row+`],"hasMore"`, 1)
		_, err := listingClient(t, body).ListJobs(context.Background(), JobListOptions{Limit: 50})
		if (err == nil) != tc.valid {
			t.Fatal("row scope/order validation", tc.valid, err)
		}
	}
	for _, field := range []string{"id", "projectId", "name", "state", "labels", "priority", "createdAt"} {
		var row map[string]json.RawMessage
		json.Unmarshal(page.Jobs[0], &row)
		delete(row, field)
		b, _ := json.Marshal(row)
		body := strings.Replace(listedPageBody(), first, string(b), 1)
		if _, err := listingClient(t, body).ListJobs(context.Background(), JobListOptions{Limit: 50}); err == nil {
			t.Fatal("missing summary field accepted", field)
		}
	}
}
