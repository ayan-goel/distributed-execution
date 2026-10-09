//go:build integration

package store

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func saveQueueBlocker(ctx context.Context, pool *pgxpool.Pool, jobID, workerID string, request AcquisitionRequest, reason string) error {
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := recordQueueBlocker(ctx, tx, jobID, workerID, request, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func TestQueueBlockerHistoryEvictionReplayAndProjectScope(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	job := queueAcquisitionJob(t, pool, nil)
	other := queueAcquisitionJob(t, pool, nil)
	ctx := context.Background()
	reasons := []string{"PLACEMENT_MISMATCH", "NO_RESOURCE_FIT", "PROJECT_QUOTA", "SWEEP_CONCURRENCY", "PROJECT_DISABLED", "RETRY_BACKOFF"}
	var requests []AcquisitionRequest
	for i := range 40 {
		request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
		requests = append(requests, request)
		if err := saveQueueBlocker(ctx, pool, job.ID, worker.WorkerID, request, reasons[i%len(reasons)]); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			// A clock correction must not keep the oldest observation forever.
			if _, err := pool.Exec(ctx, "UPDATE job_queue_blockers SET observed_at=clock_timestamp()+interval '1 day' WHERE job_id=$1", job.ID); err != nil {
				t.Fatal(err)
			}
		}
		// Interleaving jobs creates sequence gaps in each history. Retention
		// must stay per job rather than depending on global ID modulo arithmetic.
		if err := saveQueueBlocker(ctx, pool, other.ID, worker.WorkerID, request, "PROJECT_QUOTA"); err != nil {
			t.Fatal(err)
		}
	}
	history, err := GetQueueBlockers(ctx, pool, job.ProjectID, job.ID)
	if err != nil || len(history) != 16 {
		t.Fatal("history was not bounded", len(history), err)
	}
	for i, observation := range history {
		if observation.RequestID != requests[39-i].RequestID || observation.Reason != reasons[(39-i)%len(reasons)] || observation.WorkerID != worker.WorkerID || observation.SessionID != registration.SessionID || observation.AttemptCounter != 0 || observation.ObservedAt.IsZero() || observation.Sequence < 1 || i > 0 && observation.Sequence >= history[i-1].Sequence {
			t.Fatal("history order or context changed", i, observation)
		}
	}
	if err := saveQueueBlocker(ctx, pool, job.ID, worker.WorkerID, requests[39], reasons[39%len(reasons)]); err != nil {
		t.Fatal(err)
	}
	if replay, err := GetQueueBlockers(ctx, pool, job.ProjectID, job.ID); err != nil || len(replay) != 16 || replay[0] != history[0] {
		t.Fatal("retained replay changed history", replay, err)
	}
	if err := saveQueueBlocker(ctx, pool, job.ID, worker.WorkerID, requests[39], "NO_RESOURCE_FIT"); !errors.Is(err, ErrConflict) {
		t.Fatal("retained request changed its reason", err)
	}
	if _, err := GetQueueBlockers(ctx, pool, uuid.NewString(), job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign project observed history", err)
	}
	// Evicted requests are outside this ring's replay window. Acquisition replay
	// must be enforced by worker_requests before future scheduler recording.
	if err := saveQueueBlocker(ctx, pool, job.ID, worker.WorkerID, requests[0], reasons[0]); err != nil {
		t.Fatal(err)
	}
	if evicted, err := GetQueueBlockers(ctx, pool, job.ProjectID, job.ID); err != nil || len(evicted) != 16 || evicted[0].RequestID != requests[0].RequestID || evicted[0].Sequence <= history[0].Sequence {
		t.Fatal("evicted request did not get a fresh observation", evicted, err)
	}
	empty := queueAcquisitionJob(t, pool, nil)
	if observations, err := GetQueueBlockers(ctx, pool, empty.ProjectID, empty.ID); err != nil || observations == nil || len(observations) != 0 {
		t.Fatal("empty history differs from absent job", observations, err)
	}
}

func TestQueueBlockerRejectsInvalidContextAndTerminalJobs(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	job := queueAcquisitionJob(t, pool, nil)
	ctx := context.Background()
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	for _, tt := range []struct {
		job, worker, session, request, reason string
	}{
		{"bad", worker.WorkerID, request.SessionID, request.RequestID, "NO_RESOURCE_FIT"},
		{job.ID, "bad", request.SessionID, request.RequestID, "NO_RESOURCE_FIT"},
		{job.ID, worker.WorkerID, "bad", request.RequestID, "NO_RESOURCE_FIT"},
		{job.ID, worker.WorkerID, request.SessionID, "bad", "NO_RESOURCE_FIT"},
		{job.ID, worker.WorkerID, request.SessionID, request.RequestID, "SUCCESS"},
	} {
		err := saveQueueBlocker(ctx, pool, tt.job, tt.worker, AcquisitionRequest{SessionID: tt.session, RequestID: tt.request}, tt.reason)
		if !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid observation context accepted", tt, err)
		}
	}
	if _, err := RequestCancellation(ctx, pool, job.ProjectID, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := saveQueueBlocker(ctx, pool, job.ID, worker.WorkerID, request, "PROJECT_QUOTA"); !errors.Is(err, ErrInvalid) {
		t.Fatal("terminal job gained a new blocker", err)
	}
	if history, err := GetQueueBlockers(ctx, pool, job.ProjectID, job.ID); err != nil || len(history) != 0 {
		t.Fatal("rejected observation leaked history", history, err)
	}
}

func TestQueueBlockerConcurrentRequestsAndRollback(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	job := queueAcquisitionJob(t, pool, nil)
	ctx := context.Background()
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	for _, duplicates := range []bool{true, false} {
		var wg sync.WaitGroup
		failures := make(chan error, 24)
		for range 24 {
			current := request
			if !duplicates {
				current.RequestID = uuid.NewString()
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				failures <- saveQueueBlocker(ctx, pool, job.ID, worker.WorkerID, current, "NO_RESOURCE_FIT")
			}()
		}
		wg.Wait()
		close(failures)
		for err := range failures {
			if err != nil {
				t.Fatal(err)
			}
		}
		want := 16
		if duplicates {
			want = 1
		}
		if history, err := GetQueueBlockers(ctx, pool, job.ProjectID, job.ID); err != nil || len(history) != want {
			t.Fatal("concurrent history count", len(history), want, err)
		}
	}
	before, err := GetQueueBlockers(ctx, pool, job.ProjectID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	request.RequestID = uuid.NewString()
	if err := recordQueueBlocker(ctx, tx, job.ID, worker.WorkerID, request, "PROJECT_QUOTA"); err != nil {
		rollback(tx)
		t.Fatal(err)
	}
	rollback(tx)
	after, err := GetQueueBlockers(ctx, pool, job.ProjectID, job.ID)
	if err != nil || len(after) != len(before) {
		t.Fatal("rollback changed retained count", len(after), err)
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatal("rollback changed retained observation", i, before[i], after[i])
		}
	}
}
