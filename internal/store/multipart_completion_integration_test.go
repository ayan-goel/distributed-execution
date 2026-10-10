//go:build integration

package store

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestMultipartCompletionIntentSurvivesStorageFailureAndReplays(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	r.SizeBytes = (5 << 20) + 3
	r.PartSizeBytes = 5 << 20
	r.PartCount = 2
	bound, err := BindMultipartUpload(ctx, pool, id, r, "bound-backend")
	if err != nil {
		t.Fatal(err)
	}
	unready := FinalizeUploadRequest{Authority: r.Authority, RequestID: uuid.NewString(), UploadID: bound.Upload.UploadID, Object: ArtifactObject{Key: bound.Upload.ObjectKey, Version: "exact-version", SizeBytes: r.SizeBytes, SHA256: r.SHA256}}
	if _, err := FinalizeUpload(ctx, pool, id, unready, func(context.Context, ArtifactObject) error {
		t.Fatal("uncompleted multipart reached byte verification")
		return nil
	}); !errors.Is(err, ErrConflict) {
		t.Fatal("multipart skipped durable completion", err)
	}
	request := MultipartCompletionRequest{Authority: r.Authority, RequestID: uuid.NewString(), UploadID: bound.Upload.UploadID, Parts: []MultipartCompletionPart{{Number: 1, ETag: "first", SHA256: r.SHA256}, {Number: 2, ETag: "last", SHA256: r.SHA256}}}
	declareMultipartCompletionParts(t, pool, id, request)
	storageFailure := errors.New("injected storage unavailable")
	_, err = CompleteMultipartUpload(ctx, pool, id, request, func(ctx context.Context, u UploadRecord, parts []MultipartCompletionPart) (string, error) {
		var intents int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM artifact_multipart_completions WHERE upload_id=$1 AND object_version IS NULL", u.UploadID).Scan(&intents); err != nil || intents != 1 {
			t.Fatal("storage ran before durable intent", err)
		}
		// A second transaction must acquire ownership immediately: storage work
		// cannot hold job locks while waiting on the network.
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer rollback(tx)
		if _, err := tx.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE NOWAIT", r.Authority.JobID); err != nil {
			t.Fatal("storage callback retained ownership locks", err)
		}
		if u.BackendUploadID != "bound-backend" || len(parts) != 2 {
			t.Fatal("completion lost durable binding")
		}
		return "", storageFailure
	})
	if !errors.Is(err, storageFailure) {
		t.Fatal("failed storage outcome was hidden", err)
	}
	down, err := os.ReadFile("../../migrations/0022_multipart_completions.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err == nil {
		rollback(tx)
		t.Fatal("downgrade discarded prepared completion intent")
	}
	rollback(tx)
	result, err := CompleteMultipartUpload(ctx, pool, id, request, func(context.Context, UploadRecord, []MultipartCompletionPart) (string, error) {
		return "exact-version", nil
	})
	if err != nil || result.Decision != "ACCEPTED" || result.Version != "exact-version" {
		t.Fatal(result, err)
	}
	result, err = CompleteMultipartUpload(ctx, pool, id, request, func(context.Context, UploadRecord, []MultipartCompletionPart) (string, error) {
		t.Fatal("replayed completion repeated storage mutation")
		return "", nil
	})
	if err != nil || result.Version != "exact-version" {
		t.Fatal(result, err)
	}
	changed := request
	changed.Parts = append([]MultipartCompletionPart(nil), request.Parts...)
	changed.Parts[1].ETag = "changed"
	if _, err := CompleteMultipartUpload(ctx, pool, id, changed, func(context.Context, UploadRecord, []MultipartCompletionPart) (string, error) {
		t.Fatal("changed replay reached storage")
		return "", nil
	}); !errors.Is(err, ErrConflict) {
		t.Fatal("changed completion did not conflict", err)
	}
	var events, artifacts int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM job_events WHERE type='MULTIPART_STORED'),(SELECT count(*) FROM artifacts)").Scan(&events, &artifacts); err != nil || events != 1 || artifacts != 0 {
		t.Fatal("completion accepted unverified artifacts or duplicated event", events, artifacts, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE artifact_multipart_completions SET object_version='replacement' WHERE upload_id=$1", request.UploadID); err == nil {
		t.Fatal("SQL rebound completion version")
	}
	wrong := unready
	wrong.Object.Version = "different-version"
	if _, err := pool.Exec(ctx, `INSERT INTO artifact_finalizations(id,upload_id,worker_id,session_id,request_id,request_hash,object_version) VALUES($1,$2,$3,$4,$5,$6,'different-version')`, uuid.NewString(), request.UploadID, id.WorkerID, request.Authority.SessionID, uuid.NewString(), r.SHA256); err == nil {
		t.Fatal("SQL finalized an unbound multipart version")
	}
	if _, err := FinalizeUpload(ctx, pool, id, wrong, func(context.Context, ArtifactObject) error {
		t.Fatal("wrong multipart version reached verification")
		return nil
	}); !errors.Is(err, ErrConflict) {
		t.Fatal("multipart substituted completed version", err)
	}
	verified, err := FinalizeUpload(ctx, pool, id, unready, func(context.Context, ArtifactObject) error { return nil })
	if err != nil || verified.Artifact == nil || verified.Artifact.Object.Version != "exact-version" {
		t.Fatal("stored multipart version could not be verified", verified, err)
	}
}

func TestMultipartStoredEventFailurePreservesPreparedIntent(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	r.SizeBytes = (5 << 20) + 3
	r.PartSizeBytes = 5 << 20
	r.PartCount = 2
	bound, err := BindMultipartUpload(ctx, pool, id, r, "backend")
	if err != nil {
		t.Fatal(err)
	}
	request := MultipartCompletionRequest{Authority: r.Authority, RequestID: uuid.NewString(), UploadID: bound.Upload.UploadID, Parts: []MultipartCompletionPart{{Number: 1, ETag: "one", SHA256: r.SHA256}, {Number: 2, ETag: "two", SHA256: r.SHA256}}}
	declareMultipartCompletionParts(t, pool, id, request)
	if _, err := pool.Exec(ctx, `CREATE FUNCTION fail_multipart_stored() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.type='MULTIPART_STORED' THEN RAISE EXCEPTION 'injected stored event failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_multipart_stored BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION fail_multipart_stored()`); err != nil {
		t.Fatal(err)
	}
	complete := func(context.Context, UploadRecord, []MultipartCompletionPart) (string, error) {
		return "recovered-version", nil
	}
	if _, err := CompleteMultipartUpload(ctx, pool, id, request, complete); err == nil {
		t.Fatal("event failure did not roll back storage version")
	}
	var version *string
	if err := pool.QueryRow(ctx, "SELECT object_version FROM artifact_multipart_completions WHERE upload_id=$1", request.UploadID).Scan(&version); err != nil || version != nil {
		t.Fatal("event failure lost prepared intent", err)
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER fail_multipart_stored ON job_events; DROP FUNCTION fail_multipart_stored()"); err != nil {
		t.Fatal(err)
	}
	result, err := CompleteMultipartUpload(ctx, pool, id, request, complete)
	if err != nil || result.Version != "recovered-version" {
		t.Fatal("prepared completion could not recover", result, err)
	}
}

func TestMultipartCompletionRechecksAuthorityAfterStorage(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	r.SizeBytes = (5 << 20) + 3
	r.PartSizeBytes = 5 << 20
	r.PartCount = 2
	bound, err := BindMultipartUpload(ctx, pool, id, r, "backend")
	if err != nil {
		t.Fatal(err)
	}
	request := MultipartCompletionRequest{Authority: r.Authority, RequestID: uuid.NewString(), UploadID: bound.Upload.UploadID, Parts: []MultipartCompletionPart{{Number: 1, ETag: "one", SHA256: r.SHA256}, {Number: 2, ETag: "two", SHA256: r.SHA256}}}
	declareMultipartCompletionParts(t, pool, id, request)
	result, err := CompleteMultipartUpload(ctx, pool, id, request, func(ctx context.Context, _ UploadRecord, _ []MultipartCompletionPart) (string, error) {
		if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", r.Authority.AttemptID); err != nil {
			t.Fatal(err)
		}
		return "stale-exact-version", nil
	})
	if err != nil || result.Decision != "FENCED" || result.Version != "" {
		t.Fatal("expired completion published storage version", result, err)
	}
	var stored *string
	if err := pool.QueryRow(ctx, "SELECT object_version FROM artifact_multipart_completions WHERE upload_id=$1", request.UploadID).Scan(&stored); err != nil || stored != nil {
		t.Fatal("fenced storage result changed metadata", err)
	}
}

func TestMultipartCompletionConcurrentReplayRecordsOneVersion(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	r.SizeBytes = (5 << 20) + 3
	r.PartSizeBytes = 5 << 20
	r.PartCount = 2
	bound, err := BindMultipartUpload(ctx, pool, id, r, "backend")
	if err != nil {
		t.Fatal(err)
	}
	request := MultipartCompletionRequest{Authority: r.Authority, RequestID: uuid.NewString(), UploadID: bound.Upload.UploadID, Parts: []MultipartCompletionPart{{Number: 1, ETag: "one", SHA256: r.SHA256}, {Number: 2, ETag: "two", SHA256: r.SHA256}}}
	declareMultipartCompletionParts(t, pool, id, request)
	var wg sync.WaitGroup
	failures := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := CompleteMultipartUpload(ctx, pool, id, request, func(context.Context, UploadRecord, []MultipartCompletionPart) (string, error) {
				return "one-exact-version", nil
			})
			if err == nil && (result.Decision != "ACCEPTED" || result.Version != "one-exact-version") {
				err = errors.New("changed completion replay")
			}
			failures <- err
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var intents, events int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM artifact_multipart_completions),(SELECT count(*) FROM job_events WHERE type='MULTIPART_STORED')").Scan(&intents, &events); err != nil || intents != 1 || events != 1 {
		t.Fatal("concurrent completion duplicated durable state", intents, events, err)
	}
}
