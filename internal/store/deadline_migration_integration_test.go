//go:build integration

package store

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func rollbackActiveDeadlines(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	down, err := os.ReadFile("../../migrations/0020_active_deadlines.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0020_active_deadlines.up.sql'"); err != nil {
		t.Fatal(err)
	}
}

func TestActiveDeadlineIndexMigrationPreservesAuthority(t *testing.T) {
	pool, identity, registration := sessionFixture(t)
	ctx := context.Background()
	job, attempt := assignedForRecovery(t, pool, identity, registration, nil)
	const snapshot = `SELECT jsonb_build_object('job',to_jsonb(j),'attempt',to_jsonb(a),'reservation',to_jsonb(r))
		FROM jobs j JOIN attempts a ON a.id=j.current_attempt_id JOIN reservations r ON r.attempt_id=a.id WHERE j.id=$1`
	var before, after []byte
	if err := pool.QueryRow(ctx, snapshot, job).Scan(&before); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	rollbackActiveDeadlines(t, ctx, tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('active_attempt_deadlines') IS NOT NULL").Scan(&exists); err != nil || exists {
		t.Fatal("deadline index survived rollback", exists, err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var definition string
	var ready bool
	if err := pool.QueryRow(ctx, `SELECT pg_get_indexdef(indexrelid),indisvalid AND indisready FROM pg_index
		WHERE indexrelid='active_attempt_deadlines'::regclass`).Scan(&definition, &ready); err != nil || !ready ||
		!strings.Contains(definition, "LEAST(lease_expires_at, phase_deadline), id") || !strings.Contains(definition, "FINALIZING") {
		t.Fatal("missing valid active-deadline index", definition, ready, err)
	}
	if err := pool.QueryRow(ctx, snapshot, job).Scan(&after); err != nil || string(before) != string(after) {
		t.Fatal("index migration changed durable authority", attempt, err)
	}
}
