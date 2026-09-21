//go:build integration

package store

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func uploadFixture(t *testing.T) (*pgxpool.Pool, WorkerIdentity, UploadRequest) {
	t.Helper()
	pool, id, registration := readyAcquisitionWorker(t)
	queueAcquisitionJob(t, pool, func(job *spec.Job) {
		job.Spec.Outputs = []spec.Output{{Name: "result", Path: "/outputs/result", MaxBytes: MaxUploadBytes, Required: true}}
	})
	acquired, err := AcquireWork(context.Background(), pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || acquired.Assignment == nil {
		t.Fatal(acquired, err)
	}
	a := acquired.Assignment.Authority
	finalizeUploadFixture(t, pool, id, a)
	r := uploadRequest()
	r.Authority = a
	return pool, id, r
}

func TestUploadMigrationPreservesAnExistingActiveAttempt(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	// Restore the actual previous schema while retaining its active job/session.
	// Reapplying the new migration must not replace ownership or extend authority.
	down, err := os.ReadFile("../../migrations/0009_artifact_uploads.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0009_artifact_uploads.up.sql'"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var lease, phase time.Time
	if err := pool.QueryRow(ctx, "SELECT lease_expires_at,phase_deadline FROM attempts WHERE id=$1", r.Authority.AttemptID).Scan(&lease, &phase); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	result, err := CreateUpload(ctx, pool, id, r)
	if err != nil || result.Upload == nil || result.Upload.Authority != r.Authority || result.State != "FINALIZING" || !result.LeaseExpiresAt.Equal(lease) || !result.PhaseDeadline.Equal(phase) {
		t.Fatal("schema upgrade changed active ownership", result, err)
	}
}

func seedUploadCopies(t *testing.T, pool *pgxpool.Pool, record UploadRecord, request UploadRequest, size int64, count int) {
	t.Helper()
	request.SizeBytes = size
	hash, err := request.hash(request.Authority.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(context.Background(), `INSERT INTO artifact_uploads(upload_id,project_id,job_id,attempt_id,worker_id,session_id,generation,request_id,request_hash,kind,logical_name,size_bytes,sha256,part_count)
        SELECT gen_random_uuid(),project_id,job_id,attempt_id,worker_id,session_id,generation,gen_random_uuid(),$2,kind,logical_name,$3,sha256,part_count FROM artifact_uploads CROSS JOIN generate_series(1,$4::int) WHERE upload_id=$1`, record.UploadID, hash, size, count)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUploadCountAndByteLimitsSerializeCompetingDeclarations(t *testing.T) {
	for _, limit := range []string{"count", "bytes"} {
		t.Run(limit, func(t *testing.T) {
			pool, id, r := uploadFixture(t)
			ctx := context.Background()
			r.SizeBytes = 0
			first, err := CreateUpload(ctx, pool, id, r)
			if err != nil || first.Upload == nil {
				t.Fatal(first, err)
			}
			if limit == "count" {
				seedUploadCopies(t, pool, *first.Upload, r, 0, MaxAttemptUploadCount-2)
			} else {
				seedUploadCopies(t, pool, *first.Upload, r, MaxUploadBytes, int(MaxAttemptUploadBytes/MaxUploadBytes)-1)
				seedUploadCopies(t, pool, *first.Upload, r, MaxUploadBytes-1, 1)
			}
			failures := make(chan error, 2)
			for range 2 {
				next := r
				next.RequestID = uuid.NewString()
				if limit == "bytes" {
					next.SizeBytes = 1
				}
				go func() { _, err := CreateUpload(ctx, pool, id, next); failures <- err }()
			}
			one, two := <-failures, <-failures
			if !((one == nil && errors.Is(two, ErrUploadLimit)) || (two == nil && errors.Is(one, ErrUploadLimit))) {
				t.Fatal("upload budget oversubscribed", one, two)
			}
			var count, bytes int64
			if err := pool.QueryRow(ctx, "SELECT count(*),sum(size_bytes)::bigint FROM artifact_uploads").Scan(&count, &bytes); err != nil {
				t.Fatal(err)
			}
			if limit == "count" && count != MaxAttemptUploadCount || limit == "bytes" && bytes != MaxAttemptUploadBytes {
				t.Fatal(count, bytes)
			}
			if replay, err := CreateUpload(ctx, pool, id, r); err != nil || replay.Upload == nil || *replay.Upload != *first.Upload {
				t.Fatal("exact replay consumed budget", replay, err)
			}
			var events int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_events WHERE type='UPLOAD_CREATED'").Scan(&events); err != nil || events != 2 {
				t.Fatal("rejected or replayed upload created an event", events, err)
			}
		})
	}
}

func TestUploadSchemaBindsProjectAndCompleteAttemptIdentity(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	first, err := CreateUpload(ctx, pool, id, r)
	if err != nil || first.Upload == nil {
		t.Fatal(first, err)
	}
	var otherProject, otherSession string
	if err := pool.QueryRow(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES('other',1000,1024,1) RETURNING id::text").Scan(&otherProject); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO worker_sessions(worker_id,generation,fenced_at) VALUES($1,999,clock_timestamp()) RETURNING id::text", id.WorkerID).Scan(&otherSession); err != nil {
		t.Fatal(err)
	}
	for _, change := range []struct {
		project, session string
		generation       int64
	}{
		{otherProject, r.Authority.SessionID, r.Authority.Generation},
		{first.Upload.ProjectID, otherSession, r.Authority.Generation},
		{first.Upload.ProjectID, r.Authority.SessionID, r.Authority.Generation + 1},
	} {
		_, err := pool.Exec(ctx, `INSERT INTO artifact_uploads(upload_id,project_id,job_id,attempt_id,worker_id,session_id,generation,request_id,request_hash,kind,logical_name,size_bytes,sha256,part_count)
            SELECT gen_random_uuid(),$2,job_id,attempt_id,worker_id,$3,$4,gen_random_uuid(),request_hash,kind,logical_name,size_bytes,sha256,part_count FROM artifact_uploads WHERE upload_id=$1`, first.Upload.UploadID, change.project, change.session, change.generation)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "23503" {
			t.Fatal("database accepted mismatched upload scope", err)
		}
	}
}

func TestUploadIsIndependentOfSchedulerLocksAndRejectsTakenOverSession(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err := blocker.Exec(ctx, "SELECT pg_advisory_xact_lock(1146310734); SELECT id FROM workers FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if result, err := CreateUpload(bounded, pool, id, r); err != nil || result.Upload == nil {
		t.Fatal("upload serialized behind scheduler/worker locks", result, err)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	p := workerProvision()
	next := Registration{RequestID: uuid.NewString(), SessionID: uuid.NewString(), ProtocolVersion: 1, Resources: p.Resources, Slots: p.Slots, Labels: p.Labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}}
	if err := ApproveSessionTakeover(ctx, pool, id.WorkerID, r.Authority.SessionID, next.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSession(ctx, pool, id, next); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateUpload(ctx, pool, id, r); !errors.Is(err, ErrFenced) {
		t.Fatal("old session reminted upload authority", err)
	}
}

func finalizeUploadFixture(t *testing.T, pool *pgxpool.Pool, id WorkerIdentity, a AttemptAuthority) {
	t.Helper()
	code := int32(0)
	for _, r := range []PhaseReport{
		{Authority: a, EventID: uuid.NewString(), Phase: "STARTING"},
		{Authority: a, EventID: uuid.NewString(), Phase: "FINALIZING", ContainerID: strings.Repeat("a", 64), ExitCode: &code},
	} {
		if result, err := ReportPhase(context.Background(), pool, id, r); err != nil || result.Decision != "ACCEPTED" {
			t.Fatal(result, err)
		}
	}
}

func TestUploadConcurrentReplayPreservesOneRecordAndAuthority(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	var lease, phase time.Time
	if err := pool.QueryRow(ctx, "SELECT lease_expires_at,phase_deadline FROM attempts WHERE id=$1", r.Authority.AttemptID).Scan(&lease, &phase); err != nil {
		t.Fatal(err)
	}
	replies := make(chan UploadResult, 16)
	failures := make(chan error, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := CreateUpload(ctx, pool, id, r)
			replies <- result
			failures <- err
		}()
	}
	wg.Wait()
	close(replies)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first *UploadRecord
	for reply := range replies {
		if reply.Decision != "ACCEPTED" || reply.Upload == nil || reply.State != "FINALIZING" {
			t.Fatal(reply)
		}
		if first == nil {
			first = reply.Upload
		}
		if *first != *reply.Upload || !reply.LeaseExpiresAt.Equal(lease) || !reply.PhaseDeadline.Equal(phase) || !reply.ServerTime.Before(lease) {
			t.Fatal("replay changed upload or authority", reply)
		}
	}
	want := "projects/" + first.ProjectID + "/jobs/" + r.Authority.JobID + "/attempts/" + r.Authority.AttemptID + "/uploads/" + first.UploadID
	if first.ObjectKey != want {
		t.Fatal("upload key is not scoped", first.ObjectKey)
	}
	var count, events int
	var jobState, reservation string
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM artifact_uploads),(SELECT count(*) FROM job_events WHERE type='UPLOAD_CREATED'),j.state,r.state FROM jobs j JOIN reservations r ON r.attempt_id=j.current_attempt_id").Scan(&count, &events, &jobState, &reservation); err != nil || count != 1 || events != 1 || jobState != "ACTIVE" || reservation != "active" {
		t.Fatal(count, events, jobState, reservation, err)
	}
	changed := r
	changed.SHA256 = strings.Repeat("b", 64)
	if _, err := CreateUpload(ctx, pool, id, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("changed replay accepted", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE artifact_uploads SET sha256=sha256"); err != nil {
		t.Fatal("database rejected unchanged declaration", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE artifact_uploads SET sha256=repeat('b',64)"); err == nil {
		t.Fatal("database allowed upload declaration mutation")
	}
	if _, err := pool.Exec(ctx, "UPDATE artifact_uploads SET object_key='shared/result'"); err == nil {
		t.Fatal("database allowed shared writable key")
	}
}

func TestUploadRechecksFencingCancellationExpiryAndCredentialOnReplay(t *testing.T) {
	for _, tc := range []struct{ name, sql, decision string }{
		{"lease", "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second'", "FENCED"},
		{"phase", "UPDATE attempts SET phase_deadline=clock_timestamp()-interval '1 second'", "STOP_REQUESTED"},
		{"cancel", "UPDATE jobs SET state='CANCELLING',cancel_requested=true", "STOP_REQUESTED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, id, r := uploadFixture(t)
			ctx := context.Background()
			if result, err := CreateUpload(ctx, pool, id, r); err != nil || result.Upload == nil {
				t.Fatal(result, err)
			}
			if _, err := pool.Exec(ctx, tc.sql); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				result, err := CreateUpload(ctx, pool, id, r)
				if err != nil || result.Decision != tc.decision || result.Upload != nil {
					t.Fatal("stale upload authorized", result, err)
				}
				r.RequestID = uuid.NewString()
			}
			var count int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM artifact_uploads").Scan(&count); err != nil || count != 1 {
				t.Fatal("rejected request left pending metadata", count, err)
			}
		})
	}
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	stale := r
	stale.Authority.Generation++
	if result, err := CreateUpload(ctx, pool, id, stale); err != nil || result.Decision != "FENCED" || result.Upload != nil || result.State != "" {
		t.Fatal(result, err)
	}
	if _, err := CreateUpload(ctx, pool, id, r); err != nil {
		t.Fatal(err)
	}
	if err := RevokeWorkerCredential(ctx, pool, id.WorkerID, id.CredentialID); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateUpload(ctx, pool, id, r); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked credential reused upload", err)
	}
}

func TestUploadDeclarationFailureAndEventFailureLeaveNoPendingRecord(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	bad := r
	bad.LogicalName = "undeclared"
	if _, err := CreateUpload(ctx, pool, id, bad); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_upload_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.type='UPLOAD_CREATED' THEN RAISE EXCEPTION 'injected upload event failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_upload_event BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION reject_upload_event()`); err != nil {
		t.Fatal(err)
	}
	var before, after int64
	if err := pool.QueryRow(ctx, "SELECT event_sequence FROM jobs").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateUpload(ctx, pool, id, r); err == nil {
		t.Fatal("event failure accepted upload")
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT event_sequence,(SELECT count(*) FROM artifact_uploads) FROM jobs").Scan(&after, &count); err != nil || count != 0 || before != after {
		t.Fatal("failed upload leaked mutation", count, before, after, err)
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER reject_upload_event ON job_events"); err != nil {
		t.Fatal(err)
	}
	if result, err := CreateUpload(ctx, pool, id, r); err != nil || result.Upload == nil {
		t.Fatal(result, err)
	}
}

func TestUploadRequestUUIDCannotNameTwoAttempts(t *testing.T) {
	pool, id, first := uploadFixture(t)
	ctx := context.Background()
	queueAcquisitionJob(t, pool, func(job *spec.Job) {
		job.Spec.Outputs = []spec.Output{{Name: "result", Path: "/outputs/result", MaxBytes: MaxUploadBytes}}
	})
	second, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: first.Authority.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || second.Assignment == nil {
		t.Fatal(second, err)
	}
	finalizeUploadFixture(t, pool, id, second.Assignment.Authority)
	other := first
	other.Authority = second.Assignment.Authority
	failures := make(chan error, 2)
	for _, r := range []UploadRequest{first, other} {
		go func() { _, err := CreateUpload(ctx, pool, id, r); failures <- err }()
	}
	one, two := <-failures, <-failures
	if !((one == nil && errors.Is(two, ErrConflict)) || (two == nil && errors.Is(one, ErrConflict))) {
		t.Fatal(one, two)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM artifact_uploads").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
}

func TestUploadUsesFreshDatabaseTimeAfterOwnershipLockWait(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err = blocker.Exec(ctx, "SELECT id FROM jobs FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	app := "upload_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	waiting, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer waiting.Close()
	type outcome struct {
		result UploadResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() { result, err := CreateUpload(ctx, waiting, id, r); done <- outcome{result, err} }()
	until := time.Now().Add(time.Second)
	for {
		var blocked bool
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')", app).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		if time.Now().After(until) {
			t.Fatal("upload did not wait for job lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err = blocker.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()"); err != nil {
		t.Fatal(err)
	}
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil || result.result.Decision != "FENCED" || result.result.Upload != nil {
		t.Fatal("stale lock waiter acquired upload", result)
	}
}
