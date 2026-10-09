package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

func localSweepFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	job, err := os.ReadFile("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "job.yaml"), job, 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "sweep.yaml")
	if err := os.WriteFile(path, []byte(`apiVersion: dispatch.dev/v1alpha1
kind: Sweep
metadata:
  name: local-grid
  project: research
spec:
  jobTemplateFile: job.yaml
  matrix:
    SEED: ["1", "2", "3"]
  maxConcurrent: 2
  failFast: true
`), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSweepCLIResolvesRelativeTemplateAndRecordsRecoveryKey(t *testing.T) {
	path := localSweepFile(t)
	var out, errs bytes.Buffer
	if code := Run(context.Background(), []string{"sweep", "validate", path, "--json"}, func(string) string {
		t.Fatal("offline sweep validation consulted credentials")
		return ""
	}, &out, &errs); code != 0 || !strings.Contains(out.String(), `"childCount":3`) || errs.Len() != 0 {
		t.Fatal("local sweep validation failed", code, out.String(), errs.String())
	}
	const id = "00000000-0000-0000-0000-000000000001"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != "POST" || r.URL.Path != "/v1/sweeps" || r.Header.Get("Authorization") != "Bearer private" || !strings.Contains(errs.String(), r.Header.Get("Idempotency-Key")) {
			t.Error("incorrect sweep request or missing recovery key")
		}
		body, _ := io.ReadAll(r.Body)
		sweep, err := spec.DecodeSweep(bytes.NewReader(body))
		if err != nil || bytes.Contains(body, []byte("jobTemplateFile")) || sweep.Spec.JobTemplate.Kind != "Job" {
			t.Error("local template path reached server", string(body), err)
		}
		sweep.Spec.JobTemplate.Spec.Image = "registry.example.org/eval@sha256:" + strings.Repeat("a", 64)
		_, hash, _ := sweep.Canonical()
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "projectId": uuid.NewString(), "spec": sweep,
			"specHash": hash, "childIds": []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}, "maxConcurrent": 2, "createdAt": "2026-10-08T00:00:00Z"})
	}))
	defer server.Close()
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": "private", "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	out.Reset()
	errs.Reset()
	if code := Run(context.Background(), []string{"sweep", "submit", path, "--idempotency-key", "retry-sweep", "--json"}, env, &out, &errs); code != 0 || !strings.Contains(out.String(), id) || !strings.Contains(errs.String(), "retry-sweep") || requests != 1 {
		t.Fatal("sweep submission failed", code, out.String(), errs.String(), requests)
	}
	if err := os.Remove(filepath.Join(filepath.Dir(path), "job.yaml")); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errs.Reset()
	if code := Run(context.Background(), []string{"sweep", "submit", path}, env, &out, &errs); code != 2 || requests != 1 || out.Len() != 0 {
		t.Fatal("missing template reached server", code, requests)
	}
}
