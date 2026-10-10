//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func TestHTTPAndCLIExposeProjectScopedHistoricalQueueDiagnostics(t *testing.T) {
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
	job, err := spec.DecodeJob(bytes.NewReader(jobBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	job.Spec.Placement.Labels["architecture"] = "amd64"
	body, _, _ := job.Canonical()
	var created store.JobRecord
	for repeat := range 2 {
		response := call(h, "POST", "/v1/jobs", submit, "diagnostic-job", body)
		if response.Code != []int{201, 200}[repeat] || bytes.Contains(response.Body.Bytes(), []byte(`"queueDiagnostics"`)) || json.Unmarshal(response.Body.Bytes(), &created) != nil {
			t.Fatal("submission/replay response changed", response.Code, response.Body.String())
		}
	}
	provision := store.WorkerProvision{Name: "diagnostic-worker", CertificateSHA256: sha256.Sum256([]byte("diagnostic test certificate")), Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4, Projects: []string{"research"}, Labels: map[string]string{"os": "linux", "architecture": "arm64"}}
	worker, err := store.ProvisionWorker(ctx, pool, provision)
	if err != nil {
		t.Fatal(err)
	}
	session := uuid.NewString()
	registration := store.Registration{RequestID: uuid.NewString(), SessionID: session, ProtocolVersion: 1, Resources: provision.Resources, Slots: provision.Slots, Labels: provision.Labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}}
	if _, err := store.RegisterSession(ctx, pool, worker, registration); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordHeartbeat(ctx, pool, worker, store.HeartbeatReport{RequestID: uuid.NewString(), SessionID: session, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}); err != nil {
		t.Fatal(err)
	}
	request := store.AcquisitionRequest{SessionID: session, RequestID: uuid.NewString()}
	if result, err := store.AcquireWork(ctx, pool, worker, request, store.AcquisitionPolicy{}); err != nil || result.NoWorkReason != "PLACEMENT_MISMATCH" {
		t.Fatal(result, err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	c, err := client.New(server.URL, reader, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.GetJob(ctx, created.ID)
	if err != nil || got.QueueDiagnostics == nil || len(got.QueueDiagnostics.Observations) != 1 {
		t.Fatal("real HTTP diagnostics missing", got, err)
	}
	d := got.QueueDiagnostics
	o := d.Observations[0]
	if d.AttemptCounter != 0 || d.AsOf.IsZero() || o.WorkerID != worker.WorkerID || o.SessionID != session || o.RequestID != request.RequestID || o.Reason != "PLACEMENT_MISMATCH" || o.ObservedAt.After(d.AsOf) {
		t.Fatal("observation context changed", d)
	}
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": reader, "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	var out, diagnostic bytes.Buffer
	if code := cli.Run(ctx, []string{"jobs", "get", created.ID}, env, &out, &diagnostic); code != 0 || !strings.Contains(out.String(), "Historical queue observations") || !strings.Contains(out.String(), o.Reason) || !strings.Contains(out.String(), o.WorkerID) || !strings.Contains(out.String(), o.ObservedAt.Format(time.RFC3339Nano)) {
		t.Fatal("CLI omitted historical worker context", code, out.String(), diagnostic.String())
	}
	out.Reset()
	if code := cli.Run(ctx, []string{"jobs", "get", created.ID, "--json"}, env, &out, &diagnostic); code != 0 || json.Unmarshal(out.Bytes(), &got) != nil || got.QueueDiagnostics == nil || len(got.QueueDiagnostics.Observations) != 1 || got.QueueDiagnostics.Observations[0] != o {
		t.Fatal("CLI JSON lost diagnostics", code, out.String(), diagnostic.String())
	}
	for _, id := range []string{created.ID, uuid.NewString()} {
		response := call(h, "GET", "/v1/jobs/"+id, foreign, "", nil)
		if response.Code != 404 || bytes.Contains(response.Body.Bytes(), []byte(o.WorkerID)) {
			t.Fatal("foreign project read diagnostic provenance", response.Code, response.Body.String())
		}
	}
	path := "/v1/jobs/" + created.ID
	if response := call(h, "GET", path, "", "", nil); response.Code != 401 {
		t.Fatal("unauthenticated diagnostic read", response.Code)
	}
	if response := call(h, "POST", path+"/cancel", reader, "", nil); response.Code != 403 {
		t.Fatal("reader gained mutation permission", response.Code)
	}
	response := call(h, "POST", path+"/cancel", submit, "", nil)
	if response.Code != 200 || bytes.Contains(response.Body.Bytes(), []byte(`"queueDiagnostics"`)) {
		t.Fatal("cancellation mutation response changed", response.Code, response.Body.String())
	}
	got, err = c.GetJob(ctx, created.ID)
	if err != nil || got.State != "CANCELLED" || got.QueueDiagnostics == nil || len(got.QueueDiagnostics.Observations) != 1 || got.QueueDiagnostics.Observations[0] != o {
		t.Fatal("terminal status lost historical evidence", got, err)
	}
	response = call(h, "POST", "/v1/jobs", submit, "unsampled-job", body)
	if response.Code != 201 || json.Unmarshal(response.Body.Bytes(), &created) != nil {
		t.Fatal(response.Code, response.Body.String())
	}
	out.Reset()
	if code := cli.Run(ctx, []string{"jobs", "get", created.ID}, env, &out, &diagnostic); code != 0 || !strings.Contains(out.String(), "No queue observations recorded") {
		t.Fatal("unsampled job status implies a blocker decision", code, out.String(), diagnostic.String())
	}
}
