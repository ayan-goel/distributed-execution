//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/store"
)

func TestWaitCLIObservesRealCancellationAndPreservesJobsOnTimeout(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	submit, _, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := store.IssueToken(ctx, pool, "research", store.RoleRead)
	if err != nil {
		t.Fatal(err)
	}
	foreign, _, err := store.IssueToken(ctx, pool, "other", store.RoleRead)
	if err != nil {
		t.Fatal(err)
	}
	h := New(pool, resolverFunc(func(context.Context, string) (string, error) {
		return "registry.example.org/eval@sha256:" + strings.Repeat("a", 64), nil
	}), nil)
	firstRead := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r)
		if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/v1/jobs/") {
			once.Do(func() { close(firstRead) })
		}
	}))
	defer server.Close()
	env := func(token string) func(string) string {
		return func(key string) string {
			return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": token, "DISPATCH_DEV_INSECURE": "1"}[key]
		}
	}
	create := func(key string) store.JobRecord {
		t.Helper()
		w := call(h, "POST", "/v1/jobs", submit, key, jobBody(t))
		var job store.JobRecord
		if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &job) != nil {
			t.Fatal(w.Code, w.Body.String())
		}
		return job
	}
	job := create("wait-terminal")
	var out, errs bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- cli.Run(ctx, []string{"wait", job.ID, "--timeout", "5s", "--poll-interval", "100ms", "--json"}, env(reader), &out, &errs)
	}()
	select {
	case <-firstRead:
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not reach actual HTTP status read")
	}
	c, err := client.New(server.URL, submit, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled, err := c.CancelJob(ctx, job.ID); err != nil || cancelled.State != "CANCELLED" {
		t.Fatal(cancelled.State, err)
	}
	select {
	case code := <-done:
		var got client.Job
		if code != 1 || errs.Len() != 0 || json.Unmarshal(out.Bytes(), &got) != nil || got.ID != job.ID || got.State != "CANCELLED" || got.SpecHash != job.SpecHash {
			t.Fatal("wait lost real terminal result", code, out.String(), errs.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not observe committed cancellation")
	}
	job = create("wait-timeout")
	snapshot := func() string {
		t.Helper()
		var body string
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_object('job',to_jsonb(j),'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.sequence) FROM job_events e WHERE e.job_id=j.id))::text FROM jobs j WHERE j.id=$1`, job.ID).Scan(&body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	before := snapshot()
	out.Reset()
	errs.Reset()
	args := []string{"wait", job.ID, "--timeout", "150ms", "--json"}
	if code := cli.Run(ctx, args, env(reader), &out, &errs); code != 2 || out.Len() != 0 || !strings.Contains(errs.String(), "timed out") || snapshot() != before {
		t.Fatal("ordinary timeout changed stored job/events", code, errs.String())
	}
	out.Reset()
	errs.Reset()
	if code := cli.Run(ctx, append(args, "--cancel-on-timeout"), env(reader), &out, &errs); code != 2 || out.Len() != 0 || !strings.Contains(errs.String(), "FORBIDDEN") || snapshot() != before {
		t.Fatal("timeout bypassed read-only permissions", code, errs.String())
	}
	out.Reset()
	errs.Reset()
	if code := cli.Run(ctx, args, env(foreign), &out, &errs); code != 2 || out.Len() != 0 || !strings.Contains(errs.String(), "NOT_FOUND") || snapshot() != before {
		t.Fatal("wait exposed foreign job", code, errs.String())
	}
	out.Reset()
	errs.Reset()
	if code := cli.Run(ctx, append(args, "--cancel-on-timeout"), env(submit), &out, &errs); code != 2 || out.Len() != 0 || !strings.Contains(errs.String(), "CANCELLED") {
		t.Fatal("explicit timeout cancellation unavailable", code, errs.String())
	}
	if got, err := store.GetJob(ctx, pool, job.ProjectID, job.ID); err != nil || got.State != "CANCELLED" {
		t.Fatal("explicit timeout cancellation did not persist", got.State, err)
	}
}
