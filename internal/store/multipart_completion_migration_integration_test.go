//go:build integration

package store

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

func rollbackMultipartCompletions(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	down, err := os.ReadFile("../../migrations/0022_multipart_completions.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0022_multipart_completions.up.sql'"); err != nil {
		t.Fatal(err)
	}
}

func TestMultipartCompletionMigrationPreservesBackendBinding(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	r.SizeBytes = (5 << 20) + 3
	r.PartSizeBytes = 5 << 20
	r.PartCount = 2
	before, err := BindMultipartUpload(ctx, pool, id, r, "stable-backend")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	rollbackMultipartCompletions(t, ctx, tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	after, err := CreateUpload(ctx, pool, id, r)
	if err != nil || after.Upload == nil || *before.Upload != *after.Upload || before.LeaseExpiresAt != after.LeaseExpiresAt || before.PhaseDeadline != after.PhaseDeadline {
		t.Fatal("completion migration changed binding or authority", err)
	}
}
