//go:build integration

package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func rollbackMultipartParts(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	down, err := os.ReadFile("../../migrations/0023_multipart_parts.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0023_multipart_parts.up.sql'"); err != nil {
		t.Fatal(err)
	}
}

func declareMultipartCompletionParts(t *testing.T, pool *pgxpool.Pool, id WorkerIdentity, r MultipartCompletionRequest) {
	t.Helper()
	for _, part := range r.Parts {
		if result, err := PrepareMultipartPart(context.Background(), pool, id, MultipartPartRequest{Authority: r.Authority, UploadID: r.UploadID, Number: part.Number, SHA256: part.SHA256}); err != nil || result.Decision != "ACCEPTED" {
			t.Fatal("bind fixture part", result, err)
		}
	}
}

func TestMultipartPartsMigrationPreservesPreparedCompletionEvidence(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	r.SizeBytes = (5 << 20) + 3
	r.PartSizeBytes = 5 << 20
	r.PartCount = 2
	bound, err := BindMultipartUpload(ctx, pool, id, r, "backend")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	rollbackMultipartParts(t, ctx, tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	request := MultipartCompletionRequest{Authority: r.Authority, RequestID: uuid.NewString(), UploadID: bound.Upload.UploadID, Parts: []MultipartCompletionPart{{Number: 1, ETag: "one", SHA256: r.SHA256}, {Number: 2, ETag: "two", SHA256: r.SHA256}}}
	hash, err := request.hash(id.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(request.Parts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO artifact_multipart_completions(upload_id,worker_id,session_id,request_id,request_hash,parts) VALUES($1,$2,$3,$4,$5,$6)`, request.UploadID, id.WorkerID, r.Authority.SessionID, request.RequestID, hash, body); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var parts int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM artifact_upload_parts WHERE upload_id=$1 AND sha256=$2", request.UploadID, r.SHA256).Scan(&parts); err != nil || parts != 2 {
		t.Fatal("upgrade lost previous completion part hashes", parts, err)
	}
	result, err := CompleteMultipartUpload(ctx, pool, id, request, func(context.Context, UploadRecord, []MultipartCompletionPart) (string, error) {
		return "preserved-version", nil
	})
	if err != nil || result.Version != "preserved-version" {
		t.Fatal("legacy intent stopped replaying", result, err)
	}
	down, err := os.ReadFile("../../migrations/0023_multipart_parts.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err == nil {
		rollback(tx)
		t.Fatal("downgrade discarded immutable part evidence")
	}
	rollback(tx)
}
