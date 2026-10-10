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

	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func TestAttemptsCLIReadsRealRetryHistoryAndEnforcesProjectScope(t *testing.T) {
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
	w := call(h, "POST", "/v1/jobs", submit, "attempt-history", jobBody(t))
	var job store.JobRecord
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &job) != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	server := httptest.NewServer(h)
	defer server.Close()
	run := func(token string, asJSON bool) (int, string, string) {
		t.Helper()
		args := []string{"attempts", "list", job.ID}
		if asJSON {
			args = append(args, "--json")
		}
		var out, errs bytes.Buffer
		code := cli.Run(ctx, args, func(key string) string {
			return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": token, "DISPATCH_DEV_INSECURE": "1"}[key]
		}, &out, &errs)
		return code, out.String(), errs.String()
	}
	if code, out, errs := run(reader, true); code != 0 || out != `{"jobId":"`+job.ID+`","attempts":[]}`+"\n" || errs != "" {
		t.Fatal("empty history contract changed", code, out, errs)
	}
	acquire := func(name string) store.AttemptAuthority {
		t.Helper()
		p := store.WorkerProvision{Name: name, CertificateSHA256: sha256.Sum256([]byte(name)), Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4, Projects: []string{"research"}, Labels: map[string]string{"os": "linux", "architecture": "amd64"}}
		worker, err := store.ProvisionWorker(ctx, pool, p)
		if err != nil {
			t.Fatal(err)
		}
		session := uuid.NewString()
		registration := store.Registration{RequestID: uuid.NewString(), SessionID: session, ProtocolVersion: 1, Resources: p.Resources, Slots: p.Slots, Labels: p.Labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}}
		if _, err := store.RegisterSession(ctx, pool, worker, registration); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordHeartbeat(ctx, pool, worker, store.HeartbeatReport{RequestID: uuid.NewString(), SessionID: session, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}); err != nil {
			t.Fatal(err)
		}
		result, err := store.AcquireWork(ctx, pool, worker, store.AcquisitionRequest{SessionID: session, RequestID: uuid.NewString()}, store.AcquisitionPolicy{})
		if err != nil || result.Assignment == nil || result.Assignment.Authority.JobID != job.ID {
			t.Fatal("fixture could not acquire job", result, err)
		}
		return result.Assignment.Authority
	}
	first := acquire("history-first")
	// Advance only the disposable fixture's lease to exercise the real loss
	// transition without turning a CLI read test into a wall-clock expiry test.
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", first.AttemptID); err != nil {
		t.Fatal(err)
	}
	if count, err := store.ReapExpiredAttempts(ctx, pool, 64); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET next_eligible_at=clock_timestamp()-interval '1 second' WHERE id=$1", job.ID); err != nil {
		t.Fatal(err)
	}
	second := acquire("history-second")
	eventsResponse := call(h, "GET", "/v1/jobs/"+job.ID+"/events", reader, "", nil)
	var events testEventPage
	if eventsResponse.Code != 200 || json.Unmarshal(eventsResponse.Body.Bytes(), &events) != nil || len(events.Events) != 4 {
		t.Fatal("real attempt events missing", eventsResponse.Code, eventsResponse.Body.String())
	}
	for index, want := range []struct{ kind, attempt string }{{"ASSIGNED", first.AttemptID}, {"ATTEMPT_LOST", first.AttemptID}, {"ASSIGNED", second.AttemptID}} {
		event := events.Events[index+1]
		if event.Type != want.kind || event.AttemptID == nil || *event.AttemptID != want.attempt || event.Sequence != int64(index+2) {
			t.Fatal("event lost attempt identity", event, want)
		}
	}
	before, err := store.ListAttempts(ctx, pool, job.ProjectID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	code, out, errs := run(reader, true)
	var got struct {
		JobID    string           `json:"jobId"`
		Attempts []client.Attempt `json:"attempts"`
	}
	if code != 0 || errs != "" || json.Unmarshal([]byte(out), &got) != nil || got.JobID != job.ID || len(got.Attempts) != 2 {
		t.Fatal("real history lost", code, out, errs)
	}
	a, b := got.Attempts[0], got.Attempts[1]
	if a.ID != first.AttemptID || a.Number != 1 || a.State != "LOST" || a.Reason == nil || *a.Reason != "WORKER_LOST" || a.WorkerID != first.WorkerID || !a.CleanupPending || a.FinishedAt == nil || a.ExitCode != nil || b.ID != second.AttemptID || b.Number != 2 || b.State != "ASSIGNED" || b.WorkerID != second.WorkerID || b.CleanupPending || b.FinishedAt != nil {
		t.Fatal("retry evidence changed", got)
	}
	if code, out, errs := run(reader, false); code != 0 || errs != "" || !strings.Contains(out, job.ID) || !strings.Contains(out, first.AttemptID) || !strings.Contains(out, "WORKER_LOST") || !strings.Contains(out, second.WorkerID) {
		t.Fatal("human history lost evidence", code, out, errs)
	}
	for _, token := range []string{foreign, "invalid-token"} {
		if code, out, errs := run(token, true); code != 2 || out != "" || strings.Contains(errs, first.WorkerID) {
			t.Fatal("unauthorized history disclosed", code, out, errs)
		}
	}
	after, err := store.ListAttempts(ctx, pool, job.ProjectID, job.ID)
	beforeJSON, _ := json.Marshal(before)
	afterJSON, _ := json.Marshal(after)
	if err != nil || !bytes.Equal(beforeJSON, afterJSON) {
		t.Fatal("history reads changed attempts", err)
	}
}
