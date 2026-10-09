//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/jackc/pgx/v5"
)

func TestWorkerDaemonsExecuteFailedCancelledSweepRetry(t *testing.T) {
	f := newSweepDaemonFixture(t)
	datasetDir := filepath.Join(f.root, "input")
	if err := os.Mkdir(datasetDir, 0700); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(datasetDir, "input.txt")
	if err := os.WriteFile(input, []byte("frozen payload\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var dataset client.DatasetRegistration
	if err := json.Unmarshal(f.runCLI(t, "dataset", "upload", datasetDir, "--name", "retry-input", "--json"), &dataset); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, []byte("changed after upload\n"), 0600); err != nil {
		t.Fatal(err)
	}
	job := f.job
	job.Spec.Priority = 3
	job.Spec.Inputs = []spec.Input{{Dataset: dataset.Name, MountPath: "/inputs/data"}}
	job.Spec.Outputs = append(job.Spec.Outputs, spec.Output{Name: "result", Path: "/outputs/result.txt", Required: true, MaxBytes: 256})
	// Keep the victim running long enough to kill its agent. The same immutable
	// command succeeds on the retry and embeds its fresh job/attempt identities.
	job.Spec.Command = []string{"sh", "-c", `pause=2; if [ "$SEED" = 1 ]; then pause=10; fi; sleep "$pause"; cat /inputs/data/input.txt > /outputs/result.txt; printf '%s:%s\n' "$DISPATCH_JOB_ID" "$DISPATCH_ATTEMPT_ID" >> /outputs/result.txt; printf '{"seed":%s,"score":9007199254740993}\n' "$SEED" > /outputs/evaluation.json`}
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: job.Metadata, Spec: spec.SweepSpec{
		JobTemplate: job, MaxConcurrent: 2, FailFast: true, Matrix: map[string][]string{"SEED": {"1", "2", "3"}},
	}}
	source := f.submit(t, sweep)
	// Hold the real second child's completion so it occupies the second slot
	// until natural loss triggers fail-fast. The third child stays unstarted.
	gate := &delayedSweepCompletion{WorkerServiceServer: workerapi.NewService(f.pool, store.AcquisitionPolicy{AllowSoftScratch: true}, f.objects), jobID: source.ChildIDs[1], captured: make(chan *pb.CompleteAttemptRequest, 1)}
	daemons := f.startWorkers(t, gate)
	var victim store.AttemptAuthority
	var container string
	for {
		err := f.pool.QueryRow(f.ctx, `SELECT job_id::text,id::text,generation,worker_id::text,session_id::text,container_id
			FROM attempts WHERE job_id=$1 AND state='RUNNING'`, source.ChildIDs[0]).Scan(&victim.JobID, &victim.AttemptID, &victim.Generation, &victim.WorkerID, &victim.SessionID, &container)
		if err == nil {
			break
		}
		if err != pgx.ErrNoRows {
			t.Fatal(err)
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("source victim never ran")
		case <-time.After(50 * time.Millisecond):
		}
	}
	killed := false
	for _, daemon := range daemons {
		if daemon.workerID == victim.WorkerID {
			daemon.stop()
			killed = true
			break
		}
	}
	output, err := exec.CommandContext(f.ctx, "docker", "inspect", "--format", "{{.State.Running}}", container).CombinedOutput()
	if !killed || err != nil || strings.TrimSpace(string(output)) != "true" {
		t.Fatal("agent kill did not leave its real container running", killed, err, string(output))
	}
	var originalExpiry, now time.Time
	if err := f.pool.QueryRow(f.ctx, "SELECT lease_expires_at,clock_timestamp() FROM attempts WHERE id=$1", victim.AttemptID).Scan(&originalExpiry, &now); err != nil || originalExpiry.Sub(now) < 20*time.Second {
		t.Fatal("victim did not retain its natural lease", originalExpiry, now, err)
	}
	reaperCtx, stopReaper := context.WithCancel(f.ctx)
	reaperDone := make(chan struct{})
	var reaperLogs lockedBuffer
	go func() {
		defer close(reaperDone)
		runLeaseReaper(reaperCtx, f.pool, slog.New(slog.NewJSONHandler(&reaperLogs, nil)))
	}()
	t.Cleanup(func() {
		stopReaper()
		<-reaperDone
		if t.Failed() {
			t.Log("lease reaper:", reaperLogs.String())
		}
	})
	for {
		var page client.SweepPage
		if err := json.Unmarshal(f.runCLI(t, "sweep", "get", source.ID, "--json"), &page); err != nil {
			t.Fatal(err)
		}
		if page.Sweep.Progress.Failed == 1 {
			if page.Sweep.Progress != (client.SweepProgress{Total: 3, Failed: 1, Active: 1, Cancelled: 1}) {
				t.Fatal("natural failure did not cancel only the queued sibling", page.Sweep.Progress)
			}
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("natural lease loss did not terminalize source failure")
		case <-time.After(100 * time.Millisecond):
		}
	}
	var heldCompletion *pb.CompleteAttemptRequest
	select {
	case heldCompletion = <-gate.captured:
	case <-f.ctx.Done():
		t.Fatal("continuing sibling never produced a completion to hold")
	}
	var heldState string
	err = f.pool.QueryRow(f.ctx, "SELECT state FROM attempts WHERE id=$1", heldCompletion.GetAuthority().GetAttemptId()).Scan(&heldState)
	if err != nil || heldState != "FINALIZING" || heldCompletion.GetAuthority().GetJobId() != source.ChildIDs[1] || heldCompletion.ExitCode == nil || heldCompletion.GetExitCode() != 0 || !heldCompletion.GetStopped() || heldCompletion.GetReason() != pb.FailureReason_FAILURE_REASON_UNSPECIFIED || len(heldCompletion.GetOutputs()) != 2 {
		t.Fatal("held completion did not represent the successful continuing sibling", heldState, err)
	}
	gate.mu.Lock()
	gate.allowOld = true
	gate.mu.Unlock()
	for {
		var page client.SweepPage
		if err := json.Unmarshal(f.runCLI(t, "sweep", "get", source.ID, "--json"), &page); err != nil {
			t.Fatal(err)
		}
		if page.Sweep.State == "FAILED" {
			if page.Sweep.Progress != (client.SweepProgress{Total: 3, Failed: 1, Succeeded: 1, Cancelled: 1}) {
				t.Fatal("source sweep did not retain all three outcomes", page.Sweep.Progress)
			}
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("continuing sibling did not complete")
		case <-time.After(100 * time.Millisecond):
		}
	}
	gate.mu.Lock()
	evidenceErr := gate.evidenceErr
	gate.mu.Unlock()
	if evidenceErr != nil {
		t.Fatal(evidenceErr)
	}
	var expiry, lostAt time.Time
	var reason, reservation string
	if err := f.pool.QueryRow(f.ctx, `SELECT a.lease_expires_at,a.finished_at,a.reason,r.state FROM attempts a JOIN reservations r ON r.attempt_id=a.id WHERE a.id=$1`, victim.AttemptID).Scan(&expiry, &lostAt, &reason, &reservation); err != nil || !expiry.Equal(originalExpiry) || lostAt.Before(expiry) || lostAt.Sub(expiry) > 6*time.Second || reason != "WORKER_LOST" || reservation != "quarantined" {
		t.Fatal("source failure did not preserve natural loss evidence", expiry, lostAt, reason, reservation, err)
	}
	snapshot := `SELECT jsonb_build_object(
		'sweep',(SELECT to_jsonb(s) FROM sweeps s WHERE s.id=$1),
		'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY j.sweep_index) FROM jobs j WHERE j.sweep_id=$1),
		'attempts',(SELECT jsonb_agg(to_jsonb(a) ORDER BY a.id) FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE j.sweep_id=$1),
		'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.job_id,e.sequence) FROM job_events e JOIN jobs j ON j.id=e.job_id WHERE j.sweep_id=$1),
		'completions',(SELECT jsonb_agg(to_jsonb(c) ORDER BY c.attempt_id) FROM attempt_completions c JOIN jobs j ON j.id=c.job_id WHERE j.sweep_id=$1))`
	var before, after []byte
	if err := f.pool.QueryRow(f.ctx, snapshot, source.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	sourceExport := f.runCLI(t, "sweep", "export", source.ID)
	var retried, replay client.SweepRetry
	args := []string{"sweep", "retry", source.ID, "--idempotency-key", "live-subset-retry", "--json"}
	if err := json.Unmarshal(f.runCLI(t, args...), &retried); err != nil || retried.ParentSweepID != source.ID || len(retried.Children) != 2 || retried.SpecHash == source.SpecHash {
		t.Fatal("CLI did not create the selected retry subset", retried, err)
	}
	if err := json.Unmarshal(f.runCLI(t, args...), &replay); err != nil || !reflect.DeepEqual(retried, replay) {
		t.Fatal("live retry replay changed identity/membership", replay, err)
	}
	for index, child := range retried.Children {
		parent := source.ChildIDs[index*2]
		var frozen bool
		err := f.pool.QueryRow(f.ctx, `SELECT n.parent_job_id=p.id AND n.spec=p.spec AND n.spec_hash=p.spec_hash
			AND n.priority=p.priority AND n.priority=3 AND n.spec->'spec'->>'priority'='3'
			AND i.dataset_id=pi.dataset_id AND i.dataset_id=$3 AND i.mount_path=pi.mount_path
			FROM jobs n JOIN jobs p ON p.id=$2 JOIN job_inputs i ON i.job_id=n.id JOIN job_inputs pi ON pi.job_id=p.id WHERE n.id=$1`, child.ID, parent, dataset.DatasetID).Scan(&frozen)
		if err != nil || !frozen || child.Index != index || child.ParentJobID != parent || child.ID == parent {
			t.Fatal("retry lost frozen inputs/spec or selected the successful job", child, frozen, err)
		}
	}
	for {
		var page client.SweepPage
		if err := json.Unmarshal(f.runCLI(t, "sweep", "get", retried.ID, "--json"), &page); err != nil {
			t.Fatal(err)
		}
		if page.Sweep.Progress.Failed+page.Sweep.Progress.Cancelled != 0 {
			t.Fatal("retry child failed", page.Sweep.Progress)
		}
		if page.Sweep.State == "SUCCEEDED" {
			if page.Sweep.Progress != (client.SweepProgress{Total: 2, Succeeded: 2}) {
				t.Fatal("retry did not complete exactly two fresh children", page.Sweep.Progress)
			}
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("retry sweep did not complete")
		case <-time.After(100 * time.Millisecond):
		}
	}
	var attempts, workers, accepted, held, invalid int
	err = f.pool.QueryRow(f.ctx, `SELECT count(*),count(DISTINCT a.worker_id),count(c.attempt_id),count(*) FILTER(WHERE r.state<>'released'),
		count(*) FILTER(WHERE a.generation<>1 OR j.attempt_counter<>1 OR a.worker_id=$2)
		FROM jobs j JOIN attempts a ON a.job_id=j.id JOIN reservations r ON r.attempt_id=a.id
		LEFT JOIN attempt_completions c ON c.attempt_id=j.accepted_attempt_id AND c.state='SUCCEEDED' WHERE j.sweep_id=$1`, retried.ID, victim.WorkerID).Scan(&attempts, &workers, &accepted, &held, &invalid)
	if err != nil || attempts != 2 || workers != 2 || accepted != 2 || held != 0 || invalid != 0 {
		t.Fatal("retry execution reused old authority or leaked capacity", attempts, workers, accepted, held, invalid, err)
	}
	var results client.SweepResults
	if err := json.Unmarshal(f.runCLI(t, "sweep", "export", retried.ID), &results); err != nil || len(results.Children) != 2 {
		t.Fatal("retry export omitted results", results, err)
	}
	for index, child := range results.Children {
		seed := fmt.Sprint(index*2 + 1)
		if child.ID != retried.Children[index].ID || child.Index != index || child.State != "SUCCEEDED" || child.AcceptedAttemptID == nil || child.Parameters["SEED"] != seed || child.Metrics["seed"].String() != seed || child.Metrics["score"].String() != "9007199254740993" {
			t.Fatal("retry export lost selected parameters or accepted metrics", child)
		}
		destination := filepath.Join(f.root, "retry-result-"+seed)
		f.runCLI(t, "artifacts", "download", child.ID, "result", "--output", destination, "--json")
		data, err := os.ReadFile(destination)
		want := fmt.Sprintf("frozen payload\n%s:%s\n", child.ID, *child.AcceptedAttemptID)
		if err != nil || string(data) != want {
			t.Fatal("retry output lost frozen input bytes or fresh identities", string(data), err)
		}
	}
	if err := f.pool.QueryRow(f.ctx, snapshot, source.ID).Scan(&after); err != nil || !bytes.Equal(before, after) || !bytes.Equal(sourceExport, f.runCLI(t, "sweep", "export", source.ID)) {
		t.Fatal("retry changed original sweep/history/results", err)
	}
	t.Log("source: failed=1 succeeded=1 cancelled=1 after natural worker loss; retry: 2 fresh successful jobs across remaining workers with frozen dataset bytes; original history unchanged")
}
