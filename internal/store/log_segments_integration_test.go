//go:build integration

package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func verifiedLogFixture(t *testing.T) (*pgxpool.Pool, WorkerIdentity, LogSegmentRequest) {
	t.Helper()
	pool, id, upload := uploadFixture(t)
	artifactID := verifiedCatalogArtifact(t, pool, id, upload.Authority, "LOG", "stdout")
	r := LogSegmentRequest{Authority: upload.Authority, RequestID: uuid.NewString(), ArtifactID: artifactID, Stream: "STDOUT", FirstSequence: 1, LastSequence: 3}
	return pool, id, r
}

func verifiedCatalogArtifact(t *testing.T, pool *pgxpool.Pool, id WorkerIdentity, authority AttemptAuthority, kind, name string) string {
	t.Helper()
	upload := uploadRequest()
	upload.Authority, upload.Kind, upload.LogicalName, upload.SizeBytes = authority, kind, name, 3
	ctx := context.Background()
	created, err := CreateUpload(ctx, pool, id, upload)
	if err != nil || created.Upload == nil {
		t.Fatal(created, err)
	}
	finalized, err := FinalizeUpload(ctx, pool, id, FinalizeUploadRequest{
		Authority: authority, RequestID: uuid.NewString(), UploadID: created.Upload.UploadID,
		Object: ArtifactObject{Key: created.Upload.ObjectKey, Version: uuid.NewString(), SizeBytes: upload.SizeBytes, SHA256: upload.SHA256},
	}, verifiedFixtureObject)
	if err != nil || finalized.Artifact == nil {
		t.Fatal(finalized, err)
	}
	return finalized.Artifact.ArtifactID
}

func TestRegisterVerifiedLogSegmentIsIdempotentAndContiguous(t *testing.T) {
	pool, id, first := verifiedLogFixture(t)
	ctx := context.Background()
	result, err := RegisterLogSegment(ctx, pool, id, first)
	if err != nil || result.Decision != "ACCEPTED" || result.State != "FINALIZING" {
		t.Fatal(result, err)
	}
	if replay, err := RegisterLogSegment(ctx, pool, id, first); err != nil || replay != result {
		t.Fatal("exact log registration replay changed", replay, err)
	}
	changed := first
	changed.LastSequence = 4
	if _, err := RegisterLogSegment(ctx, pool, id, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("changed range reused a request ID", err)
	}
	changed = first
	changed.RequestID = uuid.NewString()
	if _, err := RegisterLogSegment(ctx, pool, id, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("verified object registered twice", err)
	}
	second := LogSegmentRequest{Authority: first.Authority, RequestID: uuid.NewString(), ArtifactID: verifiedCatalogArtifact(t, pool, id, first.Authority, "LOG", "stdout"), Stream: "STDOUT", FirstSequence: 5, LastSequence: 5}
	if _, err := RegisterLogSegment(ctx, pool, id, second); !errors.Is(err, ErrConflict) {
		t.Fatal("sequence gap was accepted without evidence", err)
	}
	second.FirstSequence = 4
	if result, err := RegisterLogSegment(ctx, pool, id, second); err != nil || result.Decision != "ACCEPTED" {
		t.Fatal("contiguous segment rejected", result, err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM log_segments WHERE attempt_id=$1", first.Authority.AttemptID).Scan(&count); err != nil || count != 2 {
		t.Fatal("registration replay or sequence advance inserted the wrong count", count, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE log_segments SET last_sequence=4 WHERE attempt_id=$1", first.Authority.AttemptID); err == nil {
		t.Fatal("catalog range was mutable")
	}
}

func TestLogCatalogRejectsForeignKindAndCancellation(t *testing.T) {
	pool, id, request := verifiedLogFixture(t)
	ctx := context.Background()
	wrong := request
	wrong.ArtifactID = verifiedCatalogArtifact(t, pool, id, request.Authority, "OUTPUT", "result")
	if _, err := RegisterLogSegment(ctx, pool, id, wrong); !errors.Is(err, ErrNotFound) {
		t.Fatal("output artifact registered as a log", err)
	}
	var projectID string
	if err := pool.QueryRow(ctx, "SELECT project_id::text FROM jobs WHERE id=$1", request.Authority.JobID).Scan(&projectID); err != nil {
		t.Fatal(err)
	}
	if _, err := RequestCancellation(ctx, pool, projectID, request.Authority.JobID); err != nil {
		t.Fatal(err)
	}
	if result, err := RegisterLogSegment(ctx, pool, id, request); err != nil || result.Decision != "STOP_REQUESTED" {
		t.Fatal("cancelled attempt registered a log", result, err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM log_segments").Scan(&count); err != nil || count != 0 {
		t.Fatal("rejected registration was durable", count, err)
	}
}

func TestLogCatalogMigrationPreservesActiveAttempt(t *testing.T) {
	pool, id, request := verifiedLogFixture(t)
	ctx := context.Background()
	var lease, phase time.Time
	if err := pool.QueryRow(ctx, "SELECT lease_expires_at,phase_deadline FROM attempts WHERE id=$1", request.Authority.AttemptID).Scan(&lease, &phase); err != nil {
		t.Fatal(err)
	}
	datasetDown, err := os.ReadFile("../../migrations/0013_datasets.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../migrations/0012_log_segments.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	rollbackJobInputs(t, ctx, tx)
	if _, err := tx.Exec(ctx, string(datasetDown)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0013_datasets.up.sql'"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0012_log_segments.up.sql'"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if result, err := RegisterLogSegment(ctx, pool, id, request); err != nil || result.Decision != "ACCEPTED" {
		t.Fatal("schema upgrade changed log registration", result, err)
	}
	var afterLease, afterPhase time.Time
	if err := pool.QueryRow(ctx, "SELECT lease_expires_at,phase_deadline FROM attempts WHERE id=$1", request.Authority.AttemptID).Scan(&afterLease, &afterPhase); err != nil || !lease.Equal(afterLease) || !phase.Equal(afterPhase) {
		t.Fatal("schema upgrade changed attempt authority", err)
	}
}
