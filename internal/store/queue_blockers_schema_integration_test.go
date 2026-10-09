//go:build integration

package store

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestQueueBlockerSchemaBoundsAndContext(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	job := queueAcquisitionJob(t, pool, nil)
	ctx := context.Background()
	insert := `INSERT INTO job_queue_blockers(job_id,slot,worker_id,session_id,request_id,attempt_counter,reason)
		VALUES($1,$2,$3,$4,$5,$6,$7)`
	if _, err := pool.Exec(ctx, insert, job.ID, 0, worker.WorkerID, registration.SessionID, uuid.NewString(), 0, "NO_RESOURCE_FIT"); err != nil {
		t.Fatal("valid queue observation", err)
	}
	for _, tt := range []struct {
		name, job, worker, session, reason, code string
		slot, counter                            int
	}{
		{"negative slot", job.ID, worker.WorkerID, registration.SessionID, "NO_RESOURCE_FIT", "23514", -1, 0},
		{"seventeenth slot", job.ID, worker.WorkerID, registration.SessionID, "NO_RESOURCE_FIT", "23514", 16, 0},
		{"duplicate slot", job.ID, worker.WorkerID, registration.SessionID, "NO_RESOURCE_FIT", "23505", 0, 0},
		{"unknown reason", job.ID, worker.WorkerID, registration.SessionID, "SUCCESS", "23514", 1, 0},
		{"negative counter", job.ID, worker.WorkerID, registration.SessionID, "PROJECT_QUOTA", "23514", 1, -1},
		{"missing job", uuid.NewString(), worker.WorkerID, registration.SessionID, "PROJECT_QUOTA", "23503", 1, 0},
		{"foreign session", job.ID, uuid.NewString(), registration.SessionID, "PROJECT_QUOTA", "23503", 1, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, insert, tt.job, tt.slot, tt.worker, tt.session, uuid.NewString(), tt.counter, tt.reason)
			requireLineageSQLState(t, err, tt.code)
		})
	}
	for _, field := range []string{"observed_at", "id"} {
		value := "'infinity'"
		if field == "id" {
			value = "0"
		}
		_, err := pool.Exec(ctx, "UPDATE job_queue_blockers SET "+field+"="+value)
		requireLineageSQLState(t, err, "23514")
	}
}
