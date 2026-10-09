//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func TestHTTPSweepRetryIsScopedAtomicAndReplayable(t *testing.T) {
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
	submit, reader, foreign := issue("research", store.RoleSubmit), issue("research", store.RoleRead), issue("other", store.RoleSubmit)
	job, err := spec.DecodeJob(bytes.NewReader(jobBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: job.Metadata,
		Spec: spec.SweepSpec{JobTemplate: job, Matrix: map[string][]string{"SEED": {"1", "2", "3"}}, MaxConcurrent: 2}}
	body, _ := json.Marshal(sweep)
	h := New(pool, resolverFunc(func(context.Context, string) (string, error) {
		return "registry.example.org/eval@sha256:" + strings.Repeat("a", 64), nil
	}), nil)
	created := call(h, "POST", "/v1/sweeps", submit, "source", body)
	if created.Code != 201 {
		t.Fatal(created.Code, created.Body.String())
	}
	var source store.SweepRecord
	if err := json.Unmarshal(created.Body.Bytes(), &source); err != nil {
		t.Fatal(err)
	}
	path := "/v1/sweeps/" + source.ID + "/retry"
	for _, tc := range []struct {
		name, path, token, key, body string
		code                         int
	}{
		{"unauthenticated", path, "", "retry", "", 401},
		{"reader", path, reader, "retry", "", 403},
		{"foreign", path, foreign, "retry", "", 404},
		{"unfinished", path, submit, "retry", "", 409},
		{"missing", "/v1/sweeps/" + uuid.NewString() + "/retry", submit, "retry", "", 404},
		{"invalid", "/v1/sweeps/bad/retry", submit, "retry", "", 404},
		{"noncanonical", "/v1/sweeps/" + strings.ReplaceAll(source.ID, "-", "") + "/retry", submit, "retry", "", 404},
		{"nil", "/v1/sweeps/" + uuid.Nil.String() + "/retry", submit, "retry", "", 404},
		{"nested", path + "/extra/retry", submit, "retry", "", 404},
		{"query", path + "?mode=all", submit, "retry", "", 400},
		{"empty-query", path + "?", submit, "retry", "", 400},
		{"empty-key", path, submit, "", "", 400},
		{"long-key", path, submit, strings.Repeat("a", 129), "", 400},
		{"space-key", path, submit, "retry key", "", 400},
		{"body", path, submit, "retry", `{}`, 400},
		{"mode", path, submit, "retry", `{"mode":"all"}`, 400},
		{"whitespace", path, submit, "retry", " ", 400},
		{"oversize", path, submit, "retry", strings.Repeat("x", spec.MaxDocumentBytes+1), 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := call(h, "POST", tc.path, tc.token, tc.key, []byte(tc.body))
			if response.Code != tc.code {
				t.Fatal("retry rejection", response.Code, tc.code, response.Body.String())
			}
			var result struct{ Error APIError }
			if json.Unmarshal(response.Body.Bytes(), &result) != nil || result.Error.Code == "" || result.Error.RequestID == "" || result.Error.Retryable {
				t.Fatal("invalid retry error contract", response.Body.String())
			}
		})
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM idempotency_keys WHERE endpoint LIKE '%/retry'").Scan(&count); err != nil || count != 0 {
		t.Fatal("rejected retry retained a key", count, err)
	}
	for _, id := range source.ChildIDs {
		if _, err := store.RequestCancellation(ctx, pool, source.ProjectID, id); err != nil {
			t.Fatal(err)
		}
	}
	// The retry endpoint copies frozen records and has no reason to consult
	// image resolution or storage, even for a fresh retry during an outage.
	h.images = resolverFunc(func(context.Context, string) (string, error) {
		t.Error("retry contacted image resolver")
		return "", context.DeadlineExceeded
	})
	responses := make(chan *httptest.ResponseRecorder, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			responses <- call(h, "POST", path, submit, "retry", nil)
		}()
	}
	wg.Wait()
	close(responses)
	var first store.SweepRetryRecord
	newCount := 0
	for response := range responses {
		if response.Code != 201 && response.Code != 200 {
			t.Fatal("retry unavailable", response.Code, response.Body.String())
		}
		var result store.SweepRetryRecord
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if first.ID == "" {
			first = result
		}
		if !reflect.DeepEqual(first, result) || result.ID == source.ID || result.ParentSweepID != source.ID || result.ProjectID != source.ProjectID || result.CreatedAt.IsZero() || result.SpecHash == source.SpecHash || len(result.Children) != 3 || result.MaxConcurrent != 2 {
			t.Fatal("retry changed identity or mapping", first, result)
		}
		for i, child := range result.Children {
			if child.Index != i || child.ParentJobID != source.ChildIDs[i] || child.ID == child.ParentJobID {
				t.Fatal("retry lost ordered lineage", child)
			}
		}
		if response.Header().Get("Location") != "/v1/sweeps/"+result.ID || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("retry response headers", response.Header())
		}
		if response.Code == 201 {
			newCount++
		}
	}
	if newCount != 1 {
		t.Fatal("same-key calls created multiple retries", newCount)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": submit, "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	var cliRetry client.SweepRetry
	for i := range 2 {
		var out, errout bytes.Buffer
		if code := cli.Run(ctx, []string{"sweep", "retry", source.ID, "--idempotency-key", "cli-retry", "--json"}, env, &out, &errout); code != 0 {
			t.Fatal("CLI retry failed against real HTTP/PostgreSQL", code, errout.String())
		}
		var result client.SweepRetry
		if err := json.Unmarshal(out.Bytes(), &result); err != nil || result.ParentSweepID != source.ID || len(result.Children) != 3 || result.ID == first.ID || i == 1 && !reflect.DeepEqual(cliRetry, result) {
			t.Fatal("CLI changed retry identity/mapping", out.String(), err)
		}
		cliRetry = result
	}
	c, err := client.New(server.URL, reader, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.GetSweep(ctx, cliRetry.ID, "", 100)
	if err != nil || page.Sweep.Progress.Queued != 3 || page.Sweep.Progress.Total != 3 {
		t.Fatal("CLI retry cannot use ordinary progress inspection", page, err)
	}
	nextPath := "/v1/sweeps/" + first.ID + "/retry"
	if response := call(h, "POST", nextPath, submit, "retry", nil); response.Code != 409 {
		t.Fatal("key replay crossed source sweep scope", response.Code, response.Body.String())
	}
	for _, child := range first.Children {
		if _, err := store.RequestCancellation(ctx, pool, first.ProjectID, child.ID); err != nil {
			t.Fatal(err)
		}
	}
	chained := call(h, "POST", nextPath, submit, "retry", nil)
	var next store.SweepRetryRecord
	if chained.Code != 201 || json.Unmarshal(chained.Body.Bytes(), &next) != nil || next.ID == first.ID || next.ParentSweepID != first.ID || len(next.Children) != 3 {
		t.Fatal("same key could not create a separately scoped retry", chained.Code, chained.Body.String())
	}
	for i, child := range next.Children {
		if child.ParentJobID != first.Children[i].ID {
			t.Fatal("chained retry skipped its immediate parent", child)
		}
	}
	if _, err := pool.Exec(ctx, "UPDATE projects SET cpu_quota=1,memory_quota_mib=1 WHERE id=$1", source.ProjectID); err != nil {
		t.Fatal(err)
	}
	replay := call(h, "POST", path, submit, "retry", nil)
	var replayed store.SweepRetryRecord
	if replay.Code != 200 || json.Unmarshal(replay.Body.Bytes(), &replayed) != nil || !reflect.DeepEqual(first, replayed) {
		t.Fatal("changed quotas broke replay", replay.Code, replay.Body.String())
	}
	if response := call(h, "POST", path, submit, "new", nil); response.Code != 422 {
		t.Fatal("fresh retry bypassed quota", response.Code, response.Body.String())
	}
	for _, tc := range []struct {
		token string
		code  int
	}{{reader, 403}, {foreign, 404}} {
		if response := call(h, "POST", path, tc.token, "retry", nil); response.Code != tc.code {
			t.Fatal("replay bypassed authorization", response.Code, response.Body.String())
		}
	}
	revoked, tokenID, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RevokeToken(ctx, pool, "research", tokenID); err != nil {
		t.Fatal(err)
	}
	if response := call(h, "POST", path, revoked, "retry", nil); response.Code != 401 {
		t.Fatal("revoked token replay bypassed authentication", response.Code, response.Body.String())
	}
	if _, err := pool.Exec(ctx, "UPDATE projects SET enabled=false WHERE id=$1", source.ProjectID); err != nil {
		t.Fatal(err)
	}
	if response := call(h, "POST", path, submit, "retry", nil); response.Code != 401 {
		t.Fatal("disabled project replay bypassed authentication", response.Code, response.Body.String())
	}
	var sweeps, jobs int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM sweeps),(SELECT count(*) FROM jobs)").Scan(&sweeps, &jobs); err != nil || sweeps != 4 || jobs != 12 {
		t.Fatal("rejected or replayed requests created extra rows", sweeps, jobs, err)
	}
}
