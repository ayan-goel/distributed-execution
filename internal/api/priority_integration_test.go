//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
)

func TestCLIJobPriorityAdmissionReplayAndValidation(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	token, _, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	h := New(pool, resolverFunc(func(context.Context, string) (string, error) {
		return "registry.example.org/eval@sha256:" + strings.Repeat("a", 64), nil
	}), nil)
	server := httptest.NewServer(h)
	defer server.Close()
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": token, "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	job, err := spec.DecodeJob(bytes.NewReader(jobBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "job.json")
	for priority := range 4 {
		job.Spec.Priority = priority
		body, _, err := job.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("priority-%d", priority)
		var first store.JobRecord
		for repeat := range 2 {
			var out, diagnostic bytes.Buffer
			if code := cli.Run(ctx, []string{"submit", path, "--idempotency-key", key, "--json"}, env, &out, &diagnostic); code != 0 {
				t.Fatal("CLI priority submission", code, diagnostic.String())
			}
			var got store.JobRecord
			if err := json.Unmarshal(out.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if repeat == 0 {
				first = got
			} else if got.ID != first.ID || got.SpecHash != first.SpecHash || !bytes.Equal(got.Spec, first.Spec) {
				t.Fatal("priority replay changed frozen job", got)
			}
		}
		var stored int
		if priority == 0 {
			explicit := bytes.Replace(body, []byte(`"spec":{`), []byte(`"spec":{"priority":0,`), 1)
			if response := call(h, "POST", "/v1/jobs", token, key, explicit); response.Code != 200 {
				t.Fatal("explicit default did not recover omitted priority", response.Code)
			}
		}
		if err := pool.QueryRow(ctx, "SELECT priority FROM jobs WHERE id=$1", first.ID).Scan(&stored); err != nil || stored != priority {
			t.Fatal("admission dropped priority", stored, priority, err)
		}
		var frozen spec.Job
		if err := json.Unmarshal(first.Spec, &frozen); err != nil || frozen.Spec.Priority != priority {
			t.Fatal("response lost frozen priority", frozen.Spec.Priority, priority, err)
		}
		job.Spec.Priority = (priority + 1) % 4
		changed, _, _ := job.Canonical()
		if response := call(h, "POST", "/v1/jobs", token, key, changed); response.Code != 409 {
			t.Fatal("changed priority replayed", response.Code, response.Body.String())
		}
	}
	for _, invalid := range []string{"-1", "4", "0.5", `"3"`, "null"} {
		body := bytes.Replace(jobBody(t), []byte(`"spec":{`), []byte(`"spec":{"priority":`+invalid+`,`), 1)
		if response := call(h, "POST", "/v1/jobs", token, "invalid-"+invalid, body); response.Code != 422 {
			t.Fatal("invalid priority status", invalid, response.Code, response.Body.String())
		}
	}
	var jobs, keys int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM jobs),(SELECT count(*) FROM idempotency_keys)").Scan(&jobs, &keys); err != nil || jobs != 4 || keys != 4 {
		t.Fatal("priority admission leaked rows or keys", jobs, keys, err)
	}
}
