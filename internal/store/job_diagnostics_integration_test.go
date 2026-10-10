//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestJobStatusIncludesBoundedHistoricalQueueDiagnostics(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	job := queueAcquisitionJob(t, pool, nil)
	for range 20 {
		if err := saveQueueBlocker(ctx, pool, job.ID, worker.WorkerID, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, "NO_RESOURCE_FIT"); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate an earlier wall-clock reading after retained observations. Sequence
	// order and historical evidence must survive clock rollback and later execution.
	if _, err := pool.Exec(ctx, "UPDATE job_queue_blockers SET observed_at=clock_timestamp()+interval '1 hour'"); err != nil {
		t.Fatal(err)
	}
	if result, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{}); err != nil || result.Assignment == nil {
		t.Fatal(result, err)
	}
	check := func(id string, wantCount, wantCounter int) {
		t.Helper()
		got, err := GetJob(ctx, pool, job.ProjectID, id)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(got)
		var decoded struct {
			QueueDiagnostics *struct {
				AsOf           time.Time      `json:"asOf"`
				AttemptCounter int            `json:"attemptCounter"`
				Observations   []QueueBlocker `json:"observations"`
			} `json:"queueDiagnostics"`
		}
		if err := json.Unmarshal(body, &decoded); err != nil || decoded.QueueDiagnostics == nil {
			t.Fatal("job status omits diagnostic snapshot", string(body), err)
		}
		d := decoded.QueueDiagnostics
		if d.AsOf.IsZero() || d.AttemptCounter != wantCounter || d.Observations == nil || len(d.Observations) != wantCount {
			t.Fatal("incorrect diagnostic snapshot", string(body))
		}
		if wantCount > 0 && (d.Observations[0].AttemptCounter != 0 || !d.Observations[0].ObservedAt.After(d.AsOf)) {
			t.Fatal("historical attempt or clock rollback context lost", string(body))
		}
	}
	check(job.ID, 16, 1)
	empty := queueAcquisitionJob(t, pool, nil)
	check(empty.ID, 0, 0)
	if _, err := GetJob(ctx, pool, uuid.NewString(), job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign project read diagnostics", err)
	}
}
