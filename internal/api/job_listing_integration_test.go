//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
)

func TestHTTPJobListingScopesFiltersAndCursors(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	tokenIDs := map[string]string{}
	issue := func(project string, role store.Role) string {
		t.Helper()
		token, id, err := store.IssueToken(ctx, pool, project, role)
		if err != nil {
			t.Fatal(err)
		}
		tokenIDs[token] = id
		return token
	}
	submit, reader, foreign := issue("research", store.RoleSubmit), issue("research", store.RoleRead), issue("other", store.RoleSubmit)
	h := New(pool, resolverFunc(func(context.Context, string) (string, error) {
		return "registry.example.org/eval@sha256:" + strings.Repeat("a", 64), nil
	}), nil)
	var ids []string
	for i := range 7 {
		job, err := spec.DecodeJob(bytes.NewReader(jobBody(t)))
		if err != nil {
			t.Fatal(err)
		}
		job.Metadata.Name = fmt.Sprintf("listed-%d", i)
		job.Metadata.Labels = map[string]string{"cohort": "alpha", "empty": "", "eq": "a=b"}
		job.Spec.Priority = i % 4
		token := submit
		if i == 6 {
			token = foreign
			job.Metadata.Project = "other"
		}
		if i == 5 {
			job.Metadata.Labels["cohort"] = "beta"
		}
		body, _, _ := job.Canonical()
		w := call(h, "POST", "/v1/jobs", token, fmt.Sprintf("listed-%d", i), body)
		var got store.JobRecord
		if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &got) != nil {
			t.Fatal(w.Code, w.Body.String())
		}
		if i < 4 {
			ids = append(ids, got.ID)
		}
		if i == 4 {
			if w := call(h, "POST", "/v1/jobs/"+got.ID+"/cancel", submit, "", nil); w.Code != 200 {
				t.Fatal(w.Code)
			}
		}
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET created_at='2026-10-01T12:00:00Z'"); err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	slices.Reverse(ids)
	path := "/v1/jobs?project=research&state=QUEUED&label=cohort%3Dalpha&label=empty%3D&label=eq%3Da%3Db&limit=2"
	var first struct {
		Project, ProjectID string
		Jobs               []store.JobSummary
		HasMore            bool
		NextCursor         string
	}
	w := call(h, "GET", path, reader, "", nil)
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &first) != nil || first.Project != "research" || len(first.Jobs) != 2 || !first.HasMore || first.NextCursor == "" || first.Jobs[0].ID != ids[0] || first.Jobs[1].ID != ids[1] {
		t.Fatal("filtered first page", w.Code, w.Body.String())
	}
	for _, j := range first.Jobs {
		if j.ProjectID != first.ProjectID || j.Labels["eq"] != "a=b" || j.Priority < 0 || j.Priority > 3 {
			t.Fatal("bad summary", j)
		}
	}
	next := "/v1/jobs?state=QUEUED&label=eq%3Da%3Db&label=empty%3D&label=cohort%3Dalpha&limit=100&cursor=" + url.QueryEscape(first.NextCursor)
	w = call(h, "GET", next, submit, "", nil)
	var second struct {
		Jobs       []store.JobSummary
		HasMore    bool
		NextCursor string
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &second) != nil || len(second.Jobs) != 2 || second.HasMore || second.NextCursor != "" || second.Jobs[0].ID != ids[2] || second.Jobs[1].ID != ids[3] {
		t.Fatal("continuation changed filter identity/order", w.Code, w.Body.String())
	}
	for _, tc := range []struct {
		path, token string
		code        int
	}{
		{path, "", 401}, {"/v1/jobs?project=other", reader, 403}, {"/v1/jobs?cursor=" + first.NextCursor, foreign, 400},
		{next + "&state=QUEUED", reader, 400}, {strings.Replace(next, "state=QUEUED", "state=CANCELLED", 1), reader, 400},
		{strings.Replace(next, "cohort%3Dalpha", "cohort%3Dbeta", 1), reader, 400},
		{"/v1/jobs?limit=0", reader, 400}, {"/v1/jobs?state=running", reader, 400}, {"/v1/jobs?cursor=", reader, 400},
		{"/v1/jobs?label=x", reader, 400}, {"/v1/jobs?label=x=1&label=x=1", reader, 400}, {"/v1/jobs?label=bad+key=x", reader, 400},
		{"/v1/jobs?label=x=%00", reader, 400}, {"/v1/jobs?label=x=%ff", reader, 400}, {"/v1/jobs?project=", reader, 400},
		{"/v1/jobs?unknown=1", reader, 400}, {"/v1/jobs?state=", reader, 400}, {"/v1/jobs?limit=1;state=QUEUED", reader, 400},
		{"/v1/jobs?label=x=" + strings.Repeat("a", 12<<10), reader, 400},
	} {
		if w := call(h, "GET", tc.path, tc.token, "", nil); w.Code != tc.code {
			t.Fatal("query accepted or incorrectly authorized", tc.path, w.Code, w.Body.String())
		}
	}
	w = call(h, "GET", "/v1/jobs?label=absent=", reader, "", nil)
	if w.Code != 200 || !bytes.Contains(w.Body.Bytes(), []byte(`"jobs":[]`)) {
		t.Fatal("invalid empty result", w.Code, w.Body.String())
	}
	w = call(h, "GET", "/v1/jobs", foreign, "", nil)
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &first) != nil || len(first.Jobs) != 1 || first.Project != "other" {
		t.Fatal("foreign token scope", w.Code, w.Body.String())
	}
	// Exercise the query boundary through net/http with the executable listener's
	// header allowance, including actual request-line and bearer-header overhead.
	server := httptest.NewUnstartedServer(h)
	server.Config.MaxHeaderBytes = 16 << 10
	server.Start()
	defer server.Close()
	boundary := "label=a=" + strings.Repeat("x", 8192) + "&label=b="
	boundary += strings.Repeat("x", maxJobQueryBytes-len(boundary))
	for _, extra := range []string{"", "x"} {
		req, err := http.NewRequest("GET", server.URL+"/v1/jobs?"+boundary+extra, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+reader)
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		want := 200
		if extra != "" {
			want = 400
		}
		if response.StatusCode != want {
			t.Fatal("configured listener query boundary", response.StatusCode, want)
		}
	}
	if err := store.RevokeToken(ctx, pool, "research", tokenIDs[reader]); err != nil {
		t.Fatal(err)
	}
	if w := call(h, "GET", path, reader, "", nil); w.Code != 401 {
		t.Fatal("revoked listing token accepted", w.Code)
	}
}

func TestCLIJobListingUsesRealHTTPAndLargeEscapedSummary(t *testing.T) {
	pool := apiPool(t)
	token, _, err := store.IssueToken(context.Background(), pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	h := New(pool, resolverFunc(func(context.Context, string) (string, error) {
		return "registry.example.org/eval@sha256:" + strings.Repeat("a", 64), nil
	}), nil)
	job, _ := spec.DecodeJob(bytes.NewReader(jobBody(t)))
	job.Metadata.Labels = map[string]string{"cohort": "alpha"}
	for i := range 96 {
		job.Metadata.Labels[fmt.Sprintf("key%d", i)] = strings.Repeat("<", 8192)
	}
	var raw bytes.Buffer
	enc := json.NewEncoder(&raw)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(job); err != nil {
		t.Fatal(err)
	}
	w := call(h, "POST", "/v1/jobs", token, "large-labels", raw.Bytes())
	var created store.JobRecord
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &created) != nil {
		t.Fatal(w.Code, w.Body.Len())
	}
	server := httptest.NewServer(h)
	defer server.Close()
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": token, "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	var out, errs bytes.Buffer
	args := []string{"jobs", "list", "--project", "research", "--state", "QUEUED", "--label", "cohort=alpha", "--limit", "1", "--json"}
	if code := cli.Run(context.Background(), args, env, &out, &errs); code != 0 || out.Len() <= 4<<20 {
		t.Fatal("listing failed above generic 4 MiB allowance", code, out.Len(), errs.String())
	}
	var page struct {
		Jobs       []store.JobSummary
		HasMore    bool
		NextCursor string
	}
	if json.Unmarshal(out.Bytes(), &page) != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != created.ID || len(page.Jobs[0].Labels) != 97 || page.HasMore || page.NextCursor != "" {
		t.Fatal("CLI lost full summary")
	}
	out.Reset()
	errs.Reset()
	if code := cli.Run(context.Background(), args[:len(args)-1], env, &out, &errs); code != 0 || !strings.Contains(out.String(), created.ID) || !strings.Contains(out.String(), job.Metadata.Name) {
		t.Fatal("human listing failed", code, out.String(), errs.String())
	}
}
