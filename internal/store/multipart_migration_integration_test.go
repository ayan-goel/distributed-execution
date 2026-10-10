//go:build integration

package store

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

func rollbackMultipartUploads(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	down, err := os.ReadFile("../../migrations/0021_multipart_uploads.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0021_multipart_uploads.up.sql'"); err != nil {
		t.Fatal(err)
	}
}

func TestMultipartMigrationPreservesSinglePartReplayAndAuthority(t *testing.T) {
	pool, id, request := uploadFixture(t)
	ctx := context.Background()
	before, err := CreateUpload(ctx, pool, id, request)
	if err != nil || before.Upload == nil {
		t.Fatal(before, err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	rollbackMultipartUploads(t, ctx, tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	after, err := CreateUpload(ctx, pool, id, request)
	if err != nil || after.Upload == nil || *after.Upload != *before.Upload ||
		after.LeaseExpiresAt != before.LeaseExpiresAt || after.PhaseDeadline != before.PhaseDeadline {
		t.Fatal("migration changed replay identity or authority", after, err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO artifact_multipart_uploads(upload_id) VALUES($1)", before.Upload.UploadID); err == nil {
		t.Fatal("single-part declaration accepted multipart identity")
	}
}

func TestMultipartMigrationRefusesLossyDowngrade(t *testing.T) {
	pool, id, request := uploadFixture(t)
	ctx := context.Background()
	request.SizeBytes = (5 << 20) + 3
	request.PartSizeBytes = 5 << 20
	request.PartCount = 2
	before, err := BindMultipartUpload(ctx, pool, id, request, "backend-preserved")
	if err != nil || before.Upload == nil {
		t.Fatal(before, err)
	}
	down, err := os.ReadFile("../../migrations/0021_multipart_uploads.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err == nil {
		rollback(tx)
		t.Fatal("downgrade discarded multipart state")
	}
	rollback(tx)
	after, err := CreateUpload(ctx, pool, id, request)
	if err != nil || after.Upload == nil || *after.Upload != *before.Upload {
		t.Fatal("refused downgrade changed durable binding", after, err)
	}
}
