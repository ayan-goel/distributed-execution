//go:build integration

package store

import (
	"context"
	"errors"
	"testing"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

func TestAcquisitionRecordsBackfillBlockerWithoutChangingFrozenJob(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "UPDATE projects SET cpu_quota=8000 WHERE name='research'"); err != nil {
		t.Fatal(err)
	}
	blocked := queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Spec.Resources.CPUMillis = 6000 })
	if _, err := pool.Exec(ctx, "UPDATE jobs SET next_eligible_at=clock_timestamp()-interval '31 minutes' WHERE id=$1", blocked.ID); err != nil {
		t.Fatal(err)
	}
	fitting := queueAcquisitionJob(t, pool, nil)
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	result, err := AcquireWork(ctx, pool, worker, request, AcquisitionPolicy{})
	if err != nil || result.Assignment == nil || result.Assignment.Authority.JobID != fitting.ID {
		t.Fatal("backfill stopped working", result, err)
	}
	history, err := GetQueueBlockers(ctx, pool, blocked.ProjectID, blocked.ID)
	if err != nil || len(history) != 1 || history[0].Reason != "NO_RESOURCE_FIT" || history[0].RequestID != request.RequestID || history[0].AttemptCounter != 0 || history[0].ObservedAt.After(result.Assignment.ServerTime) {
		t.Fatal("backfilled job has no captured blocker", history, err)
	}
	var unchanged bool
	if err := pool.QueryRow(ctx, "SELECT state='QUEUED' AND spec=$2::jsonb AND spec_hash=$3 AND priority=0 AND event_sequence=1 FROM jobs WHERE id=$1", blocked.ID, blocked.Spec, blocked.SpecHash).Scan(&unchanged); err != nil || !unchanged {
		t.Fatal("diagnostics changed frozen job or lifecycle", unchanged, err)
	}
	if history, err := GetQueueBlockers(ctx, pool, fitting.ProjectID, fitting.ID); err != nil || len(history) != 0 {
		t.Fatal("assigned job recorded as blocked", history, err)
	}
}

func TestAcquisitionDiagnosticOnlyJobsPreserveNoWorkDecisions(t *testing.T) {
	for _, kind := range []string{"disabled", "backoff", "draining", "cancelled", "revoked"} {
		t.Run(kind, func(t *testing.T) {
			pool, worker, registration := readyAcquisitionWorker(t)
			ctx := context.Background()
			job := queueAcquisitionJob(t, pool, nil)
			query := map[string]string{
				"disabled": "UPDATE projects SET enabled=false",
				"backoff":  "UPDATE jobs SET next_eligible_at=clock_timestamp()+interval '1 hour'",
				"draining": "UPDATE workers SET drain_requested=true",
				"revoked":  "UPDATE worker_credentials SET revoked_at=clock_timestamp()",
			}[kind]
			if query != "" {
				if _, err := pool.Exec(ctx, query); err != nil {
					t.Fatal(err)
				}
			} else if _, err := RequestCancellation(ctx, pool, job.ProjectID, job.ID); err != nil {
				t.Fatal(err)
			}
			result, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
			if kind == "revoked" {
				if !errors.Is(err, ErrUnauthorized) || result.Assignment != nil {
					t.Fatal("revoked worker sampled work", result, err)
				}
			} else {
				want := "QUEUE_EMPTY"
				if kind == "draining" {
					want = "WORKER_NOT_READY"
				}
				if err != nil || result.Assignment != nil || result.NoWorkReason != want {
					t.Fatal("diagnostics changed no-work decision", kind, result, err)
				}
			}
			history, err := GetQueueBlockers(ctx, pool, job.ProjectID, job.ID)
			want := map[string]string{"disabled": "PROJECT_DISABLED", "backoff": "RETRY_BACKOFF"}[kind]
			if err != nil || want == "" && len(history) != 0 || want != "" && (len(history) != 1 || history[0].Reason != want) {
				t.Fatal("incorrect diagnostic visibility", kind, history, err)
			}
		})
	}
}

func TestAcquisitionBoundsAndResamplesBlockerObservations(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	for range 20 {
		queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Spec.Placement.Labels["architecture"] = "amd64" })
	}
	for _, want := range []int{16, 20, 20} {
		result, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
		if err != nil || result.NoWorkReason != "PLACEMENT_MISMATCH" || result.Assignment != nil {
			t.Fatal("diagnostics changed no-work reason", result, err)
		}
		var count, jobs int
		if err := pool.QueryRow(ctx, "SELECT count(*),count(DISTINCT job_id) FROM job_queue_blockers").Scan(&count, &jobs); err != nil || count != want || jobs != want {
			t.Fatal("sample bound/cooldown crowded out unsampled jobs", count, jobs, want, err)
		}
	}
	var snapshots int
	if err := pool.QueryRow(ctx, "SELECT count(DISTINCT observed_at) FROM job_queue_blockers").Scan(&snapshots); err != nil || snapshots != 2 {
		t.Fatal("each poll did not share one captured database time", snapshots, err)
	}
	// Advance only the stored observations, keeping the worker heartbeat fresh.
	// This tests cooldown expiry without a real 30-second wait.
	if _, err := pool.Exec(ctx, "UPDATE job_queue_blockers SET observed_at=clock_timestamp()-interval '31 seconds'"); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{}); err != nil {
		t.Fatal(err)
	}
	var count, jobs, attempts int
	if err := pool.QueryRow(ctx, "SELECT count(*),count(DISTINCT job_id),(SELECT count(*) FROM attempts) FROM job_queue_blockers").Scan(&count, &jobs, &attempts); err != nil || count != 36 || jobs != 20 || attempts != 0 {
		t.Fatal("resampling exceeded bound or allocated work", count, jobs, attempts, err)
	}
}

func TestAcquisitionReplayDoesNotResampleAfterHistoryEviction(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	job := queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Spec.Placement.Labels["architecture"] = "amd64" })
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	if result, err := AcquireWork(ctx, pool, worker, request, AcquisitionPolicy{}); err != nil || result.NoWorkReason != "PLACEMENT_MISMATCH" {
		t.Fatal(result, err)
	}
	for range QueueBlockerHistoryLimit {
		fresh := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
		if err := saveQueueBlocker(ctx, pool, job.ID, worker.WorkerID, fresh, "PLACEMENT_MISMATCH"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, "UPDATE job_queue_blockers SET observed_at=clock_timestamp()-interval '31 seconds'"); err != nil {
		t.Fatal(err)
	}
	before, err := GetQueueBlockers(ctx, pool, job.ProjectID, job.ID)
	if err != nil || len(before) != 16 || before[15].RequestID == request.RequestID {
		t.Fatal("original observation was not evicted", before, err)
	}
	if result, err := AcquireWork(ctx, pool, worker, request, AcquisitionPolicy{}); err != nil || result.NoWorkReason != "PLACEMENT_MISMATCH" {
		t.Fatal("durable no-work replay changed", result, err)
	}
	after, err := GetQueueBlockers(ctx, pool, job.ProjectID, job.ID)
	if err != nil || len(after) != len(before) {
		t.Fatal("replay changed ring size", after, err)
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatal("evicted request appended new diagnostics", i, before[i], after[i])
		}
	}
}
