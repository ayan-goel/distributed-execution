//go:build integration

package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func completedOutputFixture(t *testing.T) (*pgxpool.Pool, WorkerIdentity, CompletionRequest) {
	t.Helper()
	pool, id, upload := artifactFixture(t)
	artifact, err := FinalizeUpload(context.Background(), pool, id, upload, verifiedFixtureObject)
	if err != nil || artifact.Artifact == nil {
		t.Fatal(artifact, err)
	}
	r := completionRequest()
	r.Authority = upload.Authority
	r.Outputs = []CompletionOutput{{Name: "result", ArtifactID: artifact.Artifact.ArtifactID}}
	r.PayloadSHA256, err = CompletionDigest(r)
	if err != nil {
		t.Fatal(err)
	}
	return pool, id, r
}

func signCompletion(t *testing.T, r *CompletionRequest) {
	t.Helper()
	hash, err := CompletionDigest(*r)
	if err != nil {
		t.Fatal(err)
	}
	r.PayloadSHA256 = hash
}

func TestCompletionMetricsMustMatchVerifiedSourceArtifact(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	queueAcquisitionJob(t, pool, func(job *spec.Job) {
		job.Spec.Outputs = []spec.Output{{Name: "metrics", Path: "/outputs/metrics.json", MaxBytes: MaxCompletionMetricsBytes, Required: true}}
	})
	acquired, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || acquired.Assignment == nil {
		t.Fatal(acquired, err)
	}
	a := acquired.Assignment.Authority
	finalizeUploadFixture(t, pool, id, a)
	body := []byte(`{"score":9007199254740993}`)
	hash := sha256.Sum256(body)
	upload := uploadRequest()
	upload.Authority = a
	upload.LogicalName = "metrics"
	upload.SizeBytes = int64(len(body))
	upload.SHA256 = hex.EncodeToString(hash[:])
	pending, err := CreateUpload(ctx, pool, id, upload)
	if err != nil || pending.Upload == nil {
		t.Fatal(pending, err)
	}
	artifact, err := FinalizeUpload(ctx, pool, id, FinalizeUploadRequest{Authority: a, RequestID: uuid.NewString(), UploadID: pending.Upload.UploadID, Object: ArtifactObject{Key: pending.Upload.ObjectKey, Version: "metrics-version", SizeBytes: upload.SizeBytes, SHA256: upload.SHA256}}, verifiedFixtureObject)
	if err != nil || artifact.Artifact == nil {
		t.Fatal(artifact, err)
	}
	r := completionRequest()
	r.Authority = a
	r.Outputs = []CompletionOutput{{Name: "metrics", ArtifactID: artifact.Artifact.ArtifactID}}
	for _, badBody := range [][]byte{nil, []byte(`{"score":42}`)} {
		r.MetricsJSON = badBody
		signCompletion(t, &r)
		if _, err := CompleteAttempt(ctx, pool, id, r); !errors.Is(err, ErrInvalid) {
			t.Fatal("metrics detached from source bytes", err)
		}
	}
	r.MetricsJSON = body
	signCompletion(t, &r)
	result, err := CompleteAttempt(ctx, pool, id, r)
	if err != nil || result.State != "SUCCEEDED" {
		t.Fatal(result, err)
	}
	var manifest resultManifest
	if err := json.Unmarshal(result.Manifest, &manifest); err != nil || manifest.Metrics["score"] != "9007199254740993" || manifest.MetricsArtifactID != artifact.Artifact.ArtifactID {
		t.Fatal("metrics precision/source identity lost", err)
	}
	var score string
	if err := pool.QueryRow(ctx, "SELECT accepted_manifest->'metrics'->>'score' FROM jobs").Scan(&score); err != nil || score != "9007199254740993" {
		t.Fatal("JSONB rounded accepted metric", score, err)
	}
}

func TestNonretryableFailureNeverPublishesCanonicalManifest(t *testing.T) {
	pool, id, r := completedOutputFixture(t)
	ctx := context.Background()
	r.Reason = "OUTPUT_INVALID"
	r.Outputs = nil
	signCompletion(t, &r)
	if result, err := CompleteAttempt(ctx, pool, id, r); err != nil || result.State != "FAILED" || len(result.Manifest) != 0 {
		t.Fatal(result, err)
	}
	var state string
	var accepted *string
	if err := pool.QueryRow(ctx, "SELECT state,accepted_attempt_id::text FROM jobs").Scan(&state, &accepted); err != nil || state != "FAILED" || accepted != nil {
		t.Fatal("failed attempt became canonical", state, accepted, err)
	}
}

func TestHistoricalCompletionReplayCannotChangeReplacementAttempt(t *testing.T) {
	pool, id, r := completedOutputFixture(t)
	ctx := context.Background()
	r.Reason = "TRANSFER_FAILED"
	r.Outputs = nil
	signCompletion(t, &r)
	if result, err := CompleteAttempt(ctx, pool, id, r); err != nil || result.State != "FAILED" {
		t.Fatal(result, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET next_eligible_at=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	next, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: r.Authority.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || next.Assignment == nil || next.Assignment.Authority.AttemptID == r.Authority.AttemptID {
		t.Fatal("retry did not receive new attempt", next, err)
	}
	if replay, err := CompleteAttempt(ctx, pool, id, r); err != nil || replay.State != "FAILED" || replay.Decision != "ACCEPTED" {
		t.Fatal("historical replay lost result", replay, err)
	}
	var current string
	if err := pool.QueryRow(ctx, "SELECT current_attempt_id::text FROM jobs").Scan(&current); err != nil || current != next.Assignment.Authority.AttemptID {
		t.Fatal("old completion changed current owner", current, err)
	}
	reused := r
	reused.Authority = next.Assignment.Authority
	reused.ExitCode = nil
	reused.Reason = "RUNTIME_UNAVAILABLE"
	signCompletion(t, &reused)
	if _, err := CompleteAttempt(ctx, pool, id, reused); !errors.Is(err, ErrConflict) {
		t.Fatal("completion UUID rebound across attempts", err)
	}
}

func TestCompletionRejectsUnverifiedMissingAndChangedEvidence(t *testing.T) {
	pool, id, r := completedOutputFixture(t)
	ctx := context.Background()
	for _, change := range []func(*CompletionRequest){
		func(r *CompletionRequest) { r.Outputs = nil },
		func(r *CompletionRequest) {
			r.Outputs = []CompletionOutput{{Name: "result", ArtifactID: uuid.NewString()}}
		},
		func(r *CompletionRequest) {
			r.Outputs = []CompletionOutput{{Name: "other", ArtifactID: r.Outputs[0].ArtifactID}}
		},
		func(r *CompletionRequest) { r.MetricsJSON = []byte(`{"loss":0.5}`) },
	} {
		bad := r
		change(&bad)
		signCompletion(t, &bad)
		if _, err := CompleteAttempt(ctx, pool, id, bad); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid completion published", err)
		}
	}
	bad := r
	bad.PayloadSHA256 = strings.Repeat("f", 64)
	if _, err := CompleteAttempt(ctx, pool, id, bad); !errors.Is(err, ErrInvalid) {
		t.Fatal("incorrect digest accepted", err)
	}
	bad = r
	code := int32(3)
	bad.ExitCode = &code
	bad.Reason = "APPLICATION_EXIT"
	signCompletion(t, &bad)
	if _, err := CompleteAttempt(ctx, pool, id, bad); !errors.Is(err, ErrConflict) {
		t.Fatal("completion changed recorded exit", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM attempt_completions").Scan(&count); err != nil || count != 0 {
		t.Fatal("invalid requests left terminal state", count, err)
	}
}

func TestCompletionCancellationOrderingAndAcknowledgement(t *testing.T) {
	t.Run("intent-first", func(t *testing.T) {
		pool, id, r := completedOutputFixture(t)
		ctx := context.Background()
		if _, err := pool.Exec(ctx, "UPDATE jobs SET state='CANCELLING',cancel_requested=true"); err != nil {
			t.Fatal(err)
		}
		if result, err := CompleteAttempt(ctx, pool, id, r); err != nil || result.Decision != "STOP_REQUESTED" || len(result.Manifest) != 0 {
			t.Fatal("success overtook cancellation", result, err)
		}
		r.Reason = "USER_CANCELLED"
		signCompletion(t, &r)
		if result, err := CompleteAttempt(ctx, pool, id, r); err != nil || result.Decision != "ACCEPTED" || result.State != "CANCELLED" || len(result.Manifest) != 0 {
			t.Fatal("stop acknowledgement did not cancel", result, err)
		}
		var state, reservation string
		if err := pool.QueryRow(ctx, "SELECT j.state,r.state FROM jobs j JOIN attempts a ON a.job_id=j.id JOIN reservations r ON r.attempt_id=a.id").Scan(&state, &reservation); err != nil || state != "CANCELLED" || reservation != "released" {
			t.Fatal(state, reservation, err)
		}
	})
	t.Run("success-first", func(t *testing.T) {
		pool, id, r := completedOutputFixture(t)
		ctx := context.Background()
		if result, err := CompleteAttempt(ctx, pool, id, r); err != nil || result.State != "SUCCEEDED" {
			t.Fatal(result, err)
		}
		changed, err := pool.Exec(ctx, "UPDATE jobs SET state='CANCELLING',cancel_requested=true WHERE state='ACTIVE'")
		if err != nil || changed.RowsAffected() != 0 {
			t.Fatal("cancellation changed successful job", err)
		}
		if _, err := pool.Exec(ctx, "UPDATE jobs SET state='CANCELLING',cancel_requested=true"); err == nil {
			t.Fatal("database allowed terminal rewrite")
		}
	})
}

func TestFailureCompletionRetriesAndQuarantinesUncertainStop(t *testing.T) {
	for _, stopped := range []bool{true, false} {
		t.Run(map[bool]string{true: "stopped", false: "uncertain"}[stopped], func(t *testing.T) {
			pool, id, r := completedOutputFixture(t)
			ctx := context.Background()
			r.Reason = "TRANSFER_FAILED"
			r.Stopped = stopped
			r.Outputs = nil
			r.LogsComplete = false
			signCompletion(t, &r)
			result, err := CompleteAttempt(ctx, pool, id, r)
			if err != nil || result.State != "FAILED" || len(result.Manifest) != 0 {
				t.Fatal(result, err)
			}
			var state, reservation string
			var cleanup bool
			var delay float64
			if err := pool.QueryRow(ctx, `SELECT j.state,r.state,a.cleanup_pending,extract(epoch FROM j.next_eligible_at-clock_timestamp()) FROM jobs j JOIN attempts a ON a.job_id=j.id JOIN reservations r ON r.attempt_id=a.id`).Scan(&state, &reservation, &cleanup, &delay); err != nil || state != "RETRY_WAIT" || cleanup == stopped || delay <= 0 {
				t.Fatal(state, reservation, cleanup, delay, err)
			}
			want := "quarantined"
			if stopped {
				want = "released"
			}
			if reservation != want {
				t.Fatal("incorrect physical capacity release", reservation)
			}
		})
	}
}

func TestCompletionReplaySurvivesLeaseExpiryAndSessionReplacement(t *testing.T) {
	pool, id, r := completedOutputFixture(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()+interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	first, err := CompleteAttempt(ctx, pool, id, r)
	if err != nil || first.State != "SUCCEEDED" {
		t.Fatal(first, err)
	}
	time.Sleep(1100 * time.Millisecond)
	p := workerProvision()
	next := Registration{RequestID: uuid.NewString(), SessionID: uuid.NewString(), ProtocolVersion: 1, Resources: p.Resources, Slots: p.Slots, Labels: p.Labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}}
	if err := ApproveSessionTakeover(ctx, pool, id.WorkerID, r.Authority.SessionID, next.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSession(ctx, pool, id, next); err != nil {
		t.Fatal(err)
	}
	if replay, err := CompleteAttempt(ctx, pool, id, r); err != nil || replay.Decision != "ACCEPTED" || string(replay.Manifest) != string(first.Manifest) {
		t.Fatal("terminal replay needed live execution authority", replay, err)
	}
	if err := RevokeWorkerCredential(ctx, pool, id.WorkerID, id.CredentialID); err != nil {
		t.Fatal(err)
	}
	if _, err := CompleteAttempt(ctx, pool, id, r); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked credential retrieved completion", err)
	}
}

func TestCompletionCannotClaimCompleteLogsWithPendingUploads(t *testing.T) {
	pool, id, r := completedOutputFixture(t)
	ctx := context.Background()
	upload := uploadRequest()
	upload.Authority = r.Authority
	upload.Kind = "LOG"
	upload.LogicalName = "stdout"
	if result, err := CreateUpload(ctx, pool, id, upload); err != nil || result.Upload == nil {
		t.Fatal(result, err)
	}
	if _, err := CompleteAttempt(ctx, pool, id, r); !errors.Is(err, ErrInvalid) {
		t.Fatal("pending logs marked complete", err)
	}
	r.LogsComplete = false
	r.Gaps = []CompletionLogGap{{Stream: "stdout", First: 1, Last: 5}}
	signCompletion(t, &r)
	result, err := CompleteAttempt(ctx, pool, id, r)
	if err != nil || result.State != "SUCCEEDED" {
		t.Fatal(result, err)
	}
	var manifest resultManifest
	if err := json.Unmarshal(result.Manifest, &manifest); err != nil || manifest.LogsComplete || len(manifest.Gaps) != 1 || len(manifest.Outputs) != 1 || manifest.Outputs[0].Object.Version != "version-one" || manifest.Attempts[0].State != "SUCCEEDED" {
		t.Fatal("manifest lost verified provenance or log gaps", err)
	}
}

func TestCompletionEventAndManifestFailuresRollBackAllState(t *testing.T) {
	for _, trigger := range []string{
		`CREATE FUNCTION reject_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.type='ATTEMPT_COMPLETED' THEN RAISE EXCEPTION 'injected completion event failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_completion BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION reject_completion()`,
		`CREATE FUNCTION corrupt_manifest() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='SUCCEEDED' THEN NEW.accepted_manifest='{}'; END IF; RETURN NEW; END $$; CREATE TRIGGER corrupt_manifest BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION corrupt_manifest()`,
	} {
		pool, id, r := completedOutputFixture(t)
		ctx := context.Background()
		if _, err := pool.Exec(ctx, trigger); err != nil {
			t.Fatal(err)
		}
		if _, err := CompleteAttempt(ctx, pool, id, r); err == nil {
			t.Fatal("broken publication committed")
		}
		var jobState, attemptState, reservation string
		var records, refs int
		if err := pool.QueryRow(ctx, `SELECT j.state,a.state,r.state,(SELECT count(*) FROM attempt_completions),(SELECT count(*) FROM completion_artifacts) FROM jobs j JOIN attempts a ON a.id=j.current_attempt_id JOIN reservations r ON r.attempt_id=a.id`).Scan(&jobState, &attemptState, &reservation, &records, &refs); err != nil || jobState != "ACTIVE" || attemptState != "FINALIZING" || reservation != "active" || records != 0 || refs != 0 {
			t.Fatal("partial completion survived rollback", jobState, attemptState, reservation, records, refs, err)
		}
	}
}

func TestCompletionRechecksExpiryAfterReferenceLockWait(t *testing.T) {
	pool, id, r := completedOutputFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `CREATE FUNCTION wait_completion_reference() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(7331901); RETURN NEW; END $$; CREATE TRIGGER wait_completion_reference BEFORE INSERT ON completion_artifacts FOR EACH ROW EXECUTE FUNCTION wait_completion_reference()`); err != nil {
		t.Fatal(err)
	}
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err := blocker.Exec(ctx, "SELECT pg_advisory_xact_lock(7331901)"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()+interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	app := "completion_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	waiting, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer waiting.Close()
	type outcome struct {
		result CompletionResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() { result, err := CompleteAttempt(ctx, waiting, id, r); done <- outcome{result, err} }()
	until := time.Now().Add(time.Second)
	for {
		var blocked bool
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'INSERT INTO completion_artifacts%')", app).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		if time.Now().After(until) {
			t.Fatal("completion did not reach reference lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil || result.result.Decision != "FENCED" || len(result.result.Manifest) != 0 {
		t.Fatal("publication used stale preflight time", result)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM attempt_completions").Scan(&count); err != nil || count != 0 {
		t.Fatal("expired publication retained completion", count, err)
	}
}

func TestCompletionMigrationPreservesVerifiedArtifact(t *testing.T) {
	pool, id, r := completedOutputFixture(t)
	ctx := context.Background()
	down, err := os.ReadFile("../../migrations/0011_attempt_completions.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0011_attempt_completions.up.sql'"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if result, err := CompleteAttempt(ctx, pool, id, r); err != nil || result.State != "SUCCEEDED" {
		t.Fatal("upgrade lost publication eligibility", result, err)
	}
}

func TestCompletionPublishesOneResultAndReplaysTerminalOutcome(t *testing.T) {
	pool, id, r := completedOutputFixture(t)
	ctx := context.Background()
	const callers = 16
	type outcome struct {
		result CompletionResult
		err    error
	}
	results := make(chan outcome, callers)
	for range callers {
		go func() { result, err := CompleteAttempt(ctx, pool, id, r); results <- outcome{result, err} }()
	}
	var first CompletionResult
	for i := 0; i < callers; i++ {
		o := <-results
		if o.err != nil || o.result.Decision != "ACCEPTED" || o.result.State != "SUCCEEDED" || len(o.result.Manifest) == 0 {
			t.Fatal(o)
		}
		if i == 0 {
			first = o.result
		}
		if string(o.result.Manifest) != string(first.Manifest) {
			t.Fatal("completion replay changed manifest")
		}
	}
	var jobState, attemptState, reservation string
	var current *string
	var completions, events int
	if err := pool.QueryRow(ctx, `SELECT j.state,a.state,r.state,j.current_attempt_id::text,(SELECT count(*) FROM attempt_completions),(SELECT count(*) FROM job_events WHERE type='ATTEMPT_COMPLETED') FROM jobs j JOIN attempts a ON a.id=j.accepted_attempt_id JOIN reservations r ON r.attempt_id=a.id`).Scan(&jobState, &attemptState, &reservation, &current, &completions, &events); err != nil || jobState != "SUCCEEDED" || attemptState != "SUCCEEDED" || reservation != "released" || current != nil || completions != 1 || events != 1 {
		t.Fatal(jobState, attemptState, reservation, current, completions, events, err)
	}
	changed := r
	changed.LogsComplete = false
	changed.PayloadSHA256, _ = CompletionDigest(changed)
	if _, err := CompleteAttempt(ctx, pool, id, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("terminal completion changed", err)
	}
	changed = r
	changed.CompletionID = uuid.NewString()
	if _, err := CompleteAttempt(ctx, pool, id, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("terminal completion identity changed", err)
	}
}
