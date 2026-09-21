//go:build integration

package store

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func artifactFixture(t *testing.T) (*pgxpool.Pool, WorkerIdentity, FinalizeUploadRequest) {
	t.Helper()
	pool, id, request := uploadFixture(t)
	upload, err := CreateUpload(context.Background(), pool, id, request)
	if err != nil || upload.Upload == nil {
		t.Fatal(upload, err)
	}
	return pool, id, FinalizeUploadRequest{Authority: request.Authority, RequestID: uuid.NewString(), UploadID: upload.Upload.UploadID, Object: ArtifactObject{Key: upload.Upload.ObjectKey, Version: "version-one", SizeBytes: request.SizeBytes, SHA256: request.SHA256}}
}

func verifiedFixtureObject(context.Context, ArtifactObject) error { return nil }

func TestFinalizeConcurrentVerificationProducesOneImmutableArtifact(t *testing.T) {
	pool, id, r := artifactFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var lease, phase time.Time
	if err := pool.QueryRow(ctx, "SELECT lease_expires_at,phase_deadline FROM attempts").Scan(&lease, &phase); err != nil {
		t.Fatal(err)
	}
	const callers = 16
	started, release := make(chan struct{}, callers), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	results, failures := make(chan FinalizeUploadResult, callers), make(chan error, callers)
	for range callers {
		go func() {
			result, err := FinalizeUpload(ctx, pool, id, r, func(ctx context.Context, object ArtifactObject) error {
				if object != r.Object {
					return errors.New("verifier received different object")
				}
				started <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			results <- result
			failures <- err
		}()
	}
	for range callers {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("preflight did not release locks")
		}
	}
	once.Do(func() { close(release) })
	var artifact *ArtifactRecord
	for range callers {
		result, err := <-results, <-failures
		if err != nil || result.Decision != "ACCEPTED" || result.Artifact == nil {
			t.Fatal(result, err)
		}
		if artifact == nil {
			artifact = result.Artifact
		}
		if *result.Artifact != *artifact || artifact.Object != r.Object {
			t.Fatal("concurrent verification changed artifact", result)
		}
	}
	replay := r
	replay.RequestID = uuid.NewString()
	never := func(context.Context, ArtifactObject) error {
		t.Error("verified replay accessed storage")
		return errors.New("must not verify again")
	}
	if result, err := FinalizeUpload(ctx, pool, id, replay, never); err != nil || result.Artifact == nil || *result.Artifact != *artifact {
		t.Fatal("upload identity was not deduplicated", result, err)
	}
	var artifacts, events int
	var jobState, reservation string
	var afterLease, afterPhase time.Time
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifacts),(SELECT count(*) FROM job_events WHERE type='ARTIFACT_VERIFIED'),j.state,r.state,a.lease_expires_at,a.phase_deadline FROM jobs j JOIN attempts a ON a.id=j.current_attempt_id JOIN reservations r ON r.attempt_id=a.id`).Scan(&artifacts, &events, &jobState, &reservation, &afterLease, &afterPhase); err != nil || artifacts != 1 || events != 1 || jobState != "ACTIVE" || reservation != "active" || !lease.Equal(afterLease) || !phase.Equal(afterPhase) {
		t.Fatal("verification published or renewed execution", err)
	}
	for _, sql := range []string{"UPDATE artifacts SET object_version='changed'", "UPDATE artifact_finalizations SET request_hash=repeat('b',64)"} {
		if _, err := pool.Exec(ctx, sql); err == nil {
			t.Fatal("immutable artifact identity changed")
		}
	}
}

func TestArtifactForeignKeyBindsFinalizationToExactVersion(t *testing.T) {
	pool, id, r := artifactFixture(t)
	ctx := context.Background()
	_, _ = FinalizeUpload(ctx, pool, id, r, func(context.Context, ArtifactObject) error { return errors.New("pending") })
	_, err := pool.Exec(ctx, `INSERT INTO artifacts(id,upload_id,finalization_id,object_version) SELECT gen_random_uuid(),upload_id,id,'wrong-version' FROM artifact_finalizations`)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "23503" {
		t.Fatal("artifact version was not bound to its finalization", err)
	}
}

func TestCompetingVerifiedVersionsCannotRebindAnUpload(t *testing.T) {
	pool, id, first := artifactFixture(t)
	second := first
	second.RequestID = uuid.NewString()
	second.Object.Version = "other-version"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started, release := make(chan struct{}, 2), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	type outcome struct {
		result FinalizeUploadResult
		err    error
	}
	results := make(chan outcome, 2)
	for _, request := range []FinalizeUploadRequest{first, second} {
		go func() {
			result, err := FinalizeUpload(ctx, pool, id, request, func(ctx context.Context, _ ArtifactObject) error {
				started <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			results <- outcome{result, err}
		}()
	}
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("verification failed to start")
		}
	}
	once.Do(func() { close(release) })
	one, two := <-results, <-results
	if one.err != nil {
		one, two = two, one
	}
	if one.err != nil || one.result.Artifact == nil || !errors.Is(two.err, ErrConflict) || two.result.Artifact != nil {
		t.Fatal("multiple verified versions accepted", one, two)
	}
	var version string
	if err := pool.QueryRow(ctx, "SELECT object_version FROM artifacts").Scan(&version); err != nil || version != one.result.Artifact.Object.Version {
		t.Fatal("winner's exact version changed", err)
	}
}

func TestFinalizationRequestBudgetSerializesLastSlotAndAllowsReplay(t *testing.T) {
	pool, id, r := artifactFixture(t)
	ctx := context.Background()
	first, err := FinalizeUpload(ctx, pool, id, r, verifiedFixtureObject)
	if err != nil || first.Artifact == nil {
		t.Fatal(first, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO artifact_finalizations(id,upload_id,worker_id,session_id,request_id,request_hash,object_version) SELECT gen_random_uuid(),upload_id,worker_id,session_id,gen_random_uuid(),request_hash,object_version FROM artifact_finalizations CROSS JOIN generate_series(1,$1::int)`, MaxAttemptFinalizations-2); err != nil {
		t.Fatal(err)
	}
	failures := make(chan error, 2)
	for range 2 {
		next := r
		next.RequestID = uuid.NewString()
		go func() { _, err := FinalizeUpload(ctx, pool, id, next, verifiedFixtureObject); failures <- err }()
	}
	one, two := <-failures, <-failures
	if !((one == nil && errors.Is(two, ErrUploadLimit)) || (two == nil && errors.Is(one, ErrUploadLimit))) {
		t.Fatal("finalization budget oversubscribed", one, two)
	}
	if replay, err := FinalizeUpload(ctx, pool, id, r, verifiedFixtureObject); err != nil || replay.Artifact == nil || *replay.Artifact != *first.Artifact {
		t.Fatal("full budget blocked exact replay", replay, err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM artifact_finalizations").Scan(&count); err != nil || count != MaxAttemptFinalizations {
		t.Fatal(count, err)
	}
}

func TestFinalizationRejectsSessionTakeoverDuringVerification(t *testing.T) {
	pool, id, r := artifactFixture(t)
	ctx := context.Background()
	_, err := FinalizeUpload(ctx, pool, id, r, func(ctx context.Context, _ ArtifactObject) error {
		p := workerProvision()
		next := Registration{RequestID: uuid.NewString(), SessionID: uuid.NewString(), ProtocolVersion: 1, Resources: p.Resources, Slots: p.Slots, Labels: p.Labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}}
		if err := ApproveSessionTakeover(ctx, pool, id.WorkerID, r.Authority.SessionID, next.SessionID); err != nil {
			return err
		}
		_, err := RegisterSession(ctx, pool, id, next)
		return err
	})
	if !errors.Is(err, ErrFenced) {
		t.Fatal("old session registered artifact after takeover", err)
	}
}

func TestFinalizeRejectsChangedDeclarationAndPreservesFailedVerificationIntent(t *testing.T) {
	pool, id, r := artifactFixture(t)
	ctx := context.Background()
	for _, change := range []func(*FinalizeUploadRequest){
		func(r *FinalizeUploadRequest) { r.Object.Key += "other" },
		func(r *FinalizeUploadRequest) { r.Object.SizeBytes++ },
		func(r *FinalizeUploadRequest) { r.Object.SHA256 = string(make([]byte, 64)) },
	} {
		bad := r
		change(&bad)
		if _, err := FinalizeUpload(ctx, pool, id, bad, func(context.Context, ArtifactObject) error {
			t.Error("invalid declaration reached verifier")
			return nil
		}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	unavailable := errors.New("verification unavailable")
	if _, err := FinalizeUpload(ctx, pool, id, r, func(context.Context, ArtifactObject) error { return unavailable }); !errors.Is(err, unavailable) {
		t.Fatal(err)
	}
	var requests, artifacts int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM artifact_finalizations),(SELECT count(*) FROM artifacts)").Scan(&requests, &artifacts); err != nil || requests != 1 || artifacts != 0 {
		t.Fatal(requests, artifacts, err)
	}
	changed := r
	changed.Object.Version = "version-two"
	if _, err := FinalizeUpload(ctx, pool, id, changed, verifiedFixtureObject); !errors.Is(err, ErrConflict) {
		t.Fatal("failed request was rebound", err)
	}
	if result, err := FinalizeUpload(ctx, pool, id, r, verifiedFixtureObject); err != nil || result.Artifact == nil {
		t.Fatal("same-intent retry failed", result, err)
	}
	changed.RequestID = uuid.NewString()
	if _, err := FinalizeUpload(ctx, pool, id, changed, verifiedFixtureObject); !errors.Is(err, ErrConflict) {
		t.Fatal("verified upload selected another version", err)
	}
}

func TestFinalizeRechecksAuthorityAfterVerificationWithoutHoldingLocks(t *testing.T) {
	for _, tc := range []struct {
		name, sql, decision string
		failure             error
	}{
		{"lease", "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second'", "FENCED", nil},
		{"phase", "UPDATE attempts SET phase_deadline=clock_timestamp()-interval '1 second'", "STOP_REQUESTED", nil},
		{"cancel", "UPDATE jobs SET state='CANCELLING',cancel_requested=true", "STOP_REQUESTED", nil},
		{"credential", "UPDATE worker_credentials SET revoked_at=clock_timestamp()", "", ErrUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, id, r := artifactFixture(t)
			result, err := FinalizeUpload(context.Background(), pool, id, r, func(ctx context.Context, _ ArtifactObject) error {
				ctx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				_, err := pool.Exec(ctx, tc.sql)
				return err
			})
			if !errors.Is(err, tc.failure) || result.Decision != tc.decision || result.Artifact != nil {
				t.Fatal("stale verification registered artifact", result, err)
			}
			var artifacts int
			if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM artifacts").Scan(&artifacts); err != nil || artifacts != 0 {
				t.Fatal("rejected verification left artifact", artifacts, err)
			}
		})
	}
}

func TestFinalizeEventFailureRollsBackVerifiedRecord(t *testing.T) {
	pool, id, r := artifactFixture(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_artifact_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.type='ARTIFACT_VERIFIED' THEN RAISE EXCEPTION 'injected artifact event failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_artifact_event BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION reject_artifact_event()`); err != nil {
		t.Fatal(err)
	}
	var before, after int64
	if err := pool.QueryRow(ctx, "SELECT event_sequence FROM jobs").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := FinalizeUpload(ctx, pool, id, r, verifiedFixtureObject); err == nil {
		t.Fatal("event failure accepted artifact")
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT event_sequence,(SELECT count(*) FROM artifacts) FROM jobs").Scan(&after, &count); err != nil || before != after || count != 0 {
		t.Fatal("partial artifact commit", count, before, after, err)
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER reject_artifact_event ON job_events"); err != nil {
		t.Fatal(err)
	}
	if result, err := FinalizeUpload(ctx, pool, id, r, verifiedFixtureObject); err != nil || result.Artifact == nil {
		t.Fatal(result, err)
	}
}

func TestArtifactMigrationPreservesExistingUpload(t *testing.T) {
	pool, id, r := artifactFixture(t)
	ctx := context.Background()
	down, err := os.ReadFile("../../migrations/0010_verified_artifacts.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	completionDown, err := os.ReadFile("../../migrations/0011_attempt_completions.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(completionDown)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0011_attempt_completions.up.sql'"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0010_verified_artifacts.up.sql'"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if result, err := FinalizeUpload(ctx, pool, id, r, verifiedFixtureObject); err != nil || result.Artifact == nil || result.Artifact.Object != r.Object {
		t.Fatal("upgrade changed existing upload", result, err)
	}
}
