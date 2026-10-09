//go:build integration

package main

import (
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

	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/jackc/pgx/v5"
)

func TestWorkerDaemonsRecover27ChildSweepAfterAgentLoss(t *testing.T) {
	f := newSweepDaemonFixture(t)
	job := f.job
	// Give the designated victim a wider kill window without making every child
	// slow. The replacement runs the same immutable command and delay.
	job.Spec.Command = []string{"sh", "-c", `printf '{"seed":%s,"rate":%s,"score":9007199254740993}\n' "$SEED" "$RATE" > /outputs/evaluation.json; pause=2; if [ "$ALGORITHM" = a ] && [ "$RATE" = 0.1 ] && [ "$SEED" = 1 ]; then pause=10; fi; sleep "$pause"`}
	job.Spec.Retry.MaxAttempts = 2
	job.Spec.Retry.On = []string{"WORKER_LOST"}
	job.Spec.Retry.InitialBackoffSeconds, job.Spec.Retry.MaxBackoffSeconds = 1, 1
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: job.Metadata, Spec: spec.SweepSpec{
		JobTemplate: job, MaxConcurrent: 2, FailFast: true, CancelRunningOnFailure: true,
		Matrix: map[string][]string{"ALGORITHM": {"a", "b", "c"}, "RATE": {"0.1", "0.01", "0.001"}, "SEED": {"1", "2", "3"}},
	}}
	submitted := f.submit(t, sweep)
	// Use the server's production detector and database clock. Mutating a lease
	// would skip the natural loss-detection part of this recovery demonstration.
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
	probe := &sweepAcquisitionProbe{WorkerServiceServer: workerapi.NewService(f.pool, store.AcquisitionPolicy{AllowSoftScratch: true}, f.objects), pool: f.pool, id: submitted.ID}
	daemons := f.startWorkers(t, probe)
	var old store.AttemptAuthority
	var container string
	for {
		err := f.pool.QueryRow(f.ctx, `SELECT a.job_id::text,a.id::text,a.generation,a.worker_id::text,a.session_id::text,a.container_id
			FROM attempts a WHERE a.job_id=$1 AND a.state='RUNNING'`, submitted.ChildIDs[0]).Scan(&old.JobID, &old.AttemptID, &old.Generation, &old.WorkerID, &old.SessionID, &container)
		if err == nil {
			break
		}
		if err != pgx.ErrNoRows {
			t.Fatal("cannot observe a running sweep child", err)
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("sweep never started a real running child")
		case <-time.After(50 * time.Millisecond):
		}
	}
	killed := false
	for _, daemon := range daemons {
		if daemon.workerID == old.WorkerID {
			daemon.stop()
			killed = true
			break
		}
	}
	if !killed {
		t.Fatal("running attempt was not owned by a fixture process")
	}
	output, err := exec.CommandContext(f.ctx, "docker", "inspect", "--format", "{{.State.Running}}", container).CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "true" {
		t.Fatal("worker kill did not leave an actually running orphan container", err, string(output))
	}
	var originalExpiry, dbNow time.Time
	if err := f.pool.QueryRow(f.ctx, "SELECT lease_expires_at,clock_timestamp() FROM attempts WHERE id=$1", old.AttemptID).Scan(&originalExpiry, &dbNow); err != nil || originalExpiry.Sub(dbNow) < 20*time.Second {
		t.Fatal("loss test did not retain the issued natural lease", originalExpiry, dbNow, err)
	}
	replay := f.submit(t, sweep)
	if replay.ID != submitted.ID || !reflect.DeepEqual(replay.ChildIDs, submitted.ChildIDs) {
		t.Fatal("submission recovery changed sweep membership")
	}
	for {
		var page client.SweepPage
		if err := json.Unmarshal(f.runCLI(t, "sweep", "get", submitted.ID, "--json"), &page); err != nil {
			t.Fatal(err)
		}
		if page.Sweep.Progress.Failed > 0 || page.Sweep.Progress.Cancelled > 0 {
			t.Fatal("retry-eligible loss triggered permanent fail-fast", page.Sweep.Progress)
		}
		if page.Sweep.State == "SUCCEEDED" {
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("sweep did not recover after worker loss", page.Sweep.Progress)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if probe.peak.Load() != 2 {
		t.Fatal("recovery sweep did not exercise its cap", probe.peak.Load())
	}
	var lost, accepted, attempts, completions, failFastEvents int
	err = f.pool.QueryRow(f.ctx, `SELECT count(*),count(*) FILTER(WHERE a.state='LOST'),count(*) FILTER(WHERE a.state='SUCCEEDED'),count(c.attempt_id),
		(SELECT count(*) FROM job_events e JOIN jobs x ON x.id=e.job_id WHERE x.sweep_id=$1 AND e.type='SWEEP_FAIL_FAST')
		FROM attempts a JOIN jobs j ON j.id=a.job_id LEFT JOIN attempt_completions c ON c.attempt_id=a.id WHERE j.sweep_id=$1`, submitted.ID).Scan(&attempts, &lost, &accepted, &completions, &failFastEvents)
	if err != nil || attempts != 28 || lost != 1 || accepted != 27 || completions != 27 || failFastEvents != 0 {
		t.Fatal("incorrect recovery history", attempts, lost, accepted, completions, failFastEvents, err)
	}
	var expiry, lostAt time.Time
	var oldReservation, replacementWorker, replacementID, replacementReservation string
	var generation int64
	err = f.pool.QueryRow(f.ctx, `SELECT a.lease_expires_at,a.finished_at,r.state,n.id::text,n.worker_id::text,n.generation,nr.state
		FROM attempts a JOIN reservations r ON r.attempt_id=a.id JOIN jobs j ON j.id=a.job_id JOIN attempts n ON n.id=j.accepted_attempt_id JOIN reservations nr ON nr.attempt_id=n.id WHERE a.id=$1`, old.AttemptID).Scan(&expiry, &lostAt, &oldReservation, &replacementID, &replacementWorker, &generation, &replacementReservation)
	if err != nil || !expiry.Equal(originalExpiry) || lostAt.Before(expiry) || lostAt.Sub(expiry) > 6*time.Second || oldReservation != "quarantined" || replacementWorker == old.WorkerID || replacementID == old.AttemptID || generation != old.Generation+1 || replacementReservation != "released" {
		t.Fatal("loss/replacement authority or capacity was incorrect", expiry, lostAt, oldReservation, replacementWorker, generation, replacementReservation, err)
	}
	var results client.SweepResults
	if err := json.Unmarshal(f.runCLI(t, "sweep", "export", submitted.ID), &results); err != nil {
		t.Fatal(err)
	}
	if len(results.Children) != 27 {
		t.Fatal("recovery export omitted children")
	}
	var recovered client.SweepChild
	for index, child := range results.Children {
		if child.Index != index || child.ID != submitted.ChildIDs[index] || child.State != "SUCCEEDED" || child.AcceptedAttemptID == nil || len(child.Metrics) != 3 || child.Metrics["seed"].String() != child.Parameters["SEED"] || child.Metrics["rate"].String() != child.Parameters["RATE"] || child.Metrics["score"].String() != "9007199254740993" {
			t.Fatal("recovery export lost accepted child results", child)
		}
		if child.ID == old.JobID {
			recovered = child
		}
	}
	if recovered.AcceptedAttemptID == nil || *recovered.AcceptedAttemptID != replacementID {
		t.Fatal("lost child did not accept its replacement result")
	}
	destination := filepath.Join(f.root, "recovered-metrics.json")
	f.runCLI(t, "artifacts", "download", old.JobID, "metrics", "--output", destination, "--json")
	data, err := os.ReadFile(destination)
	want := fmt.Sprintf("{\"seed\":%s,\"rate\":%s,\"score\":9007199254740993}\n", recovered.Parameters["SEED"], recovered.Parameters["RATE"])
	if err != nil || string(data) != want {
		t.Fatal("replacement artifact download changed metric source bytes", err, string(data))
	}
	t.Logf("27 children recovered across remaining local workers after agent kill; attempts=28 lost=1 succeeded=27; loss recorded %s after natural expiry; old reservation quarantined", lostAt.Sub(expiry))
}
