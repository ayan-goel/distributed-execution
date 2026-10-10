//go:build integration

package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func rollbackQueueBlockers(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	rollbackJobListing(t, ctx, tx)
	down, err := os.ReadFile("../../migrations/0018_queue_blockers.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0018_queue_blockers.up.sql'"); err != nil {
		t.Fatal(err)
	}
}

func TestQueueBlockerMigrationPreservesActiveAuthorityAndCursor(t *testing.T) {
	pool, worker, request := uploadFixture(t)
	ctx := context.Background()
	var lease, phase time.Time
	var project string
	if err := pool.QueryRow(ctx, `SELECT a.lease_expires_at,a.phase_deadline,j.project_id::text
		FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE a.id=$1`, request.Authority.AttemptID).Scan(&lease, &phase, &project); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE scheduler_state SET project_cursor=$1", project); err != nil {
		t.Fatal(err)
	}
	_, updated := schedulerCursor(t, pool)
	queued := queueAcquisitionJob(t, pool, nil)
	if err := saveQueueBlocker(ctx, pool, queued.ID, worker.WorkerID, AcquisitionRequest{SessionID: request.Authority.SessionID, RequestID: uuid.NewString()}, "NO_RESOURCE_FIT"); err != nil {
		t.Fatal(err)
	}
	const authoritySQL = `SELECT jsonb_build_object('job',to_jsonb(j),'attempt',to_jsonb(a),'reservation',to_jsonb(r))
		FROM jobs j JOIN attempts a ON a.id=j.current_attempt_id JOIN reservations r ON r.attempt_id=a.id WHERE j.id=$1`
	var before, after []byte
	if err := pool.QueryRow(ctx, authoritySQL, request.Authority.JobID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	rollbackQueueBlockers(t, ctx, tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, authoritySQL, request.Authority.JobID).Scan(&after); err != nil || string(before) != string(after) {
		t.Fatal("diagnostic migration changed authority, reservation, or job identity", err)
	}
	requireSchedulerCursor(t, pool, project, updated)
	if history, err := GetQueueBlockers(ctx, pool, queued.ProjectID, queued.ID); err != nil || len(history) != 0 {
		t.Fatal("development rollback retained old diagnostics", history, err)
	}
	result, err := CreateUpload(ctx, pool, worker, request)
	if err != nil || result.Upload == nil || result.Upload.Authority != request.Authority || !result.LeaseExpiresAt.Equal(lease) || !result.PhaseDeadline.Equal(phase) {
		t.Fatal("upgrade invalidated active upload or changed deadlines", result, err)
	}
}
