//go:build integration

package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSchedulerCursorSchemaConstraints(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if cursor, _ := schedulerCursor(t, pool); cursor != nil {
		t.Fatal("fresh cursor already selected a project", cursor)
	}
	for _, invalid := range []struct {
		query string
		args  []any
		code  string
	}{
		{"INSERT INTO scheduler_state(singleton) VALUES(false)", nil, "23514"},
		{"INSERT INTO scheduler_state(singleton) VALUES(true)", nil, "23505"},
		{"UPDATE scheduler_state SET project_cursor=$1", []any{uuid.NewString()}, "23503"},
		{"UPDATE scheduler_state SET updated_at='infinity'", nil, "23514"},
	} {
		_, err := pool.Exec(ctx, invalid.query, invalid.args...)
		requireLineageSQLState(t, err, invalid.code)
	}
	var project string
	if err := pool.QueryRow(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES('empty',1000,1024,1) RETURNING id::text").Scan(&project); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE scheduler_state SET project_cursor=$1", project); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM projects WHERE id=$1", project); err != nil {
		t.Fatal(err)
	}
	if cursor, _ := schedulerCursor(t, pool); cursor != nil {
		t.Fatal("deleted empty project left dangling cursor", cursor)
	}
}

func TestSchedulerCursorMigrationPreservesActiveAuthority(t *testing.T) {
	pool, worker, request := uploadFixture(t)
	ctx := context.Background()
	var lease, phase time.Time
	if err := pool.QueryRow(ctx, "SELECT lease_expires_at,phase_deadline FROM attempts WHERE id=$1", request.Authority.AttemptID).Scan(&lease, &phase); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	down, err := os.ReadFile("../../migrations/0017_scheduler_cursor.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0017_scheduler_cursor.up.sql'"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if cursor, _ := schedulerCursor(t, pool); cursor != nil {
		t.Fatal("development rollback did not reset rotation", cursor)
	}
	result, err := CreateUpload(ctx, pool, worker, request)
	if err != nil || result.Upload == nil || result.Upload.Authority != request.Authority || result.State != "FINALIZING" || !result.LeaseExpiresAt.Equal(lease) || !result.PhaseDeadline.Equal(phase) {
		t.Fatal("scheduler migration changed active authority or deadlines", result, err)
	}
}
