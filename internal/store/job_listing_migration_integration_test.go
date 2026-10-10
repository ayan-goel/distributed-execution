//go:build integration

package store

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func rollbackJobListing(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	down, err := os.ReadFile("../../migrations/0019_job_listing.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0019_job_listing.up.sql'"); err != nil {
		t.Fatal(err)
	}
}

func TestJobListingMigrationPreservesActiveAuthorityAndDiagnostics(t *testing.T) {
	pool, worker, request := uploadFixture(t)
	ctx := context.Background()
	queued := queueAcquisitionJob(t, pool, nil)
	if _, err := pool.Exec(ctx, "UPDATE scheduler_state SET project_cursor=$1", queued.ProjectID); err != nil {
		t.Fatal(err)
	}
	if err := saveQueueBlocker(ctx, pool, queued.ID, worker.WorkerID, AcquisitionRequest{SessionID: request.Authority.SessionID, RequestID: uuid.NewString()}, "NO_RESOURCE_FIT"); err != nil {
		t.Fatal(err)
	}
	const snapshotSQL = `SELECT jsonb_build_object('job',to_jsonb(j),'attempt',to_jsonb(a),'reservation',to_jsonb(r),
		'queued',(SELECT to_jsonb(q) FROM jobs q WHERE q.id=$2),
		'history',(SELECT jsonb_agg(to_jsonb(b) ORDER BY b.id) FROM job_queue_blockers b WHERE b.job_id=$2),
		'scheduler',(SELECT to_jsonb(s) FROM scheduler_state s))
		FROM jobs j JOIN attempts a ON a.id=j.current_attempt_id JOIN reservations r ON r.attempt_id=a.id WHERE j.id=$1`
	var before, after []byte
	if err := pool.QueryRow(ctx, snapshotSQL, request.Authority.JobID, queued.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	rollbackJobListing(t, ctx, tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('jobs_project_created_id_idx') IS NOT NULL").Scan(&exists); err != nil || exists {
		t.Fatal("rollback retained listing index", exists, err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var definition string
	var ready bool
	if err := pool.QueryRow(ctx, `SELECT pg_get_indexdef(indexrelid),indisvalid AND indisready FROM pg_index
		WHERE indexrelid='jobs_project_created_id_idx'::regclass`).Scan(&definition, &ready); err != nil || !ready || !strings.Contains(definition, "(project_id, created_at DESC, id DESC)") {
		t.Fatal("missing valid ordered project index", definition, ready, err)
	}
	if err := pool.QueryRow(ctx, snapshotSQL, request.Authority.JobID, queued.ID).Scan(&after); err != nil || string(before) != string(after) {
		t.Fatal("index migration changed job identity, active authority, diagnostics, or scheduler state", err)
	}
}
