//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func assignedForRecovery(t *testing.T, pool *pgxpool.Pool, identity WorkerIdentity, r Registration, change func(*spec.Job)) (string, string) {
	t.Helper()
	ctx := context.Background()
	if _, err := RegisterSession(ctx, pool, identity, r); err != nil {
		t.Fatal(err)
	}
	j, hash := admittedExample(t)
	if change != nil {
		change(&j)
	}
	job, err := SubmitJob(ctx, pool, uuid.NewString(), hash, j)
	if err != nil {
		t.Fatal(err)
	}
	attempt := uuid.NewString()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, `INSERT INTO attempts(id,job_id,attempt_number,worker_id,session_id,generation,lease_expires_at,phase_deadline)
	VALUES($1,$2,1,$3,$4,1,clock_timestamp()+interval '30 seconds',clock_timestamp()+interval '300 seconds')`, attempt, job.ID, identity.WorkerID, r.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO reservations(attempt_id,worker_id,cpu_millis,memory_mib,scratch_mib) VALUES($1,$2,$3,$4,$5)", attempt, identity.WorkerID, j.Spec.Resources.CPUMillis, j.Spec.Resources.MemoryMiB, j.Spec.Resources.ScratchMiB); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "UPDATE jobs SET state='ACTIVE',current_attempt_id=$2,attempt_counter=1 WHERE id=$1", job.ID, attempt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return job.ID, attempt
}

func TestRecoveryUsesFreshTimeAfterWaitingForJobLock(t *testing.T) {
	pool, id, r := sessionFixture(t)
	ctx := context.Background()
	_, attempt := assignedForRecovery(t, pool, id, r, nil)
	if _, err := pool.Exec(ctx, "UPDATE workers SET last_heartbeat_at=clock_timestamp()-interval '31 seconds' WHERE id=$1", id.WorkerID); err != nil {
		t.Fatal(err)
	}
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(lock)
	if _, err := lock.Exec(ctx, "SELECT id FROM jobs FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	app := "recovery_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	waiting, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer waiting.Close()
	next := r
	next.RequestID = uuid.NewString()
	next.SessionID = uuid.NewString()
	done := make(chan error, 1)
	go func() { _, err := RegisterSession(ctx, waiting, id, next); done <- err }()
	deadline := time.Now().Add(time.Second)
	for {
		var blocked bool
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'SELECT j.id%')", app).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("registration never waited on the locked job")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// End the lease after observing the recovery transaction blocked. Its start
	// time is now provably older than expiry without relying on a timed sleep.
	if _, err := lock.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp() WHERE id=$1", attempt); err != nil {
		t.Fatal(err)
	}
	if err := lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal("recovery evaluated stale transaction-start time", err)
	}
}

func TestRecoveryRequiresInactiveSessionAndExpiredLeases(t *testing.T) {
	pool, id, r := sessionFixture(t)
	ctx := context.Background()
	jobID, attempt := assignedForRecovery(t, pool, id, r, nil)
	next := r
	next.RequestID = uuid.NewString()
	next.SessionID = uuid.NewString()
	if _, err := pool.Exec(ctx, "UPDATE workers SET last_heartbeat_at=clock_timestamp()-interval '31 seconds' WHERE id=$1", id.WorkerID); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSession(ctx, pool, id, next); !errors.Is(err, ErrSessionActive) {
		t.Fatal("live lease was fenced without approval", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", attempt); err != nil {
		t.Fatal(err)
	}
	s, err := RegisterSession(ctx, pool, id, next)
	if err != nil || s.Generation != 2 || !s.CleanupRequired {
		t.Fatal("expired-session recovery failed", err)
	}
	assertRecoveredAttempt(t, pool, jobID, attempt, "RETRY_WAIT", "LOST")
	if _, err := RegisterSession(ctx, pool, id, r); !errors.Is(err, ErrFenced) {
		t.Fatal("old request regained authority", err)
	}
	if again, err := RegisterSession(ctx, pool, id, next); err != nil || again != s {
		t.Fatal("recovery replay changed authority", err)
	}
}

func assertRecoveredAttempt(t *testing.T, pool *pgxpool.Pool, jobID, attempt, wantJob, wantAttempt string) {
	t.Helper()
	var jobState, attemptState, reservationState string
	var cleanup bool
	var current *string
	var delay float64
	if err := pool.QueryRow(context.Background(), `SELECT j.state,a.state,r.state,a.cleanup_pending,j.current_attempt_id::text,extract(epoch FROM j.next_eligible_at-a.finished_at)
	FROM jobs j JOIN attempts a ON a.job_id=j.id JOIN reservations r ON r.attempt_id=a.id WHERE j.id=$1 AND a.id=$2`, jobID, attempt).Scan(&jobState, &attemptState, &reservationState, &cleanup, &current, &delay); err != nil {
		t.Fatal(err)
	}
	if jobState != wantJob || attemptState != wantAttempt || reservationState != "quarantined" || !cleanup || current != nil {
		t.Fatal("incomplete recovery transition", jobState, attemptState, reservationState, cleanup, current)
	}
	if wantJob == "RETRY_WAIT" && (delay < 2.5 || delay > 5) {
		t.Fatal("retry jitter outside configured first-attempt range", delay)
	}
}

func TestExplicitTakeoverFencesLiveAttemptsAndHonorsRetryCancellation(t *testing.T) {
	for _, tc := range []struct {
		name, state, attempt string
		change               func(*spec.Job)
		cancel               bool
	}{
		{"retry", "RETRY_WAIT", "LOST", nil, false},
		{"exhausted", "FAILED", "LOST", func(j *spec.Job) { j.Spec.Retry.MaxAttempts = 1 }, false},
		{"not opted in", "FAILED", "LOST", func(j *spec.Job) { j.Spec.Retry.On = nil }, false},
		{"cancelled", "CANCELLED", "CANCELLED", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, id, r := sessionFixture(t)
			ctx := context.Background()
			job, attempt := assignedForRecovery(t, pool, id, r, tc.change)
			if tc.cancel {
				if _, err := pool.Exec(ctx, "UPDATE jobs SET state='CANCELLING',cancel_requested=true WHERE id=$1", job); err != nil {
					t.Fatal(err)
				}
			}
			next := r
			next.RequestID = uuid.NewString()
			next.SessionID = uuid.NewString()
			for range 2 {
				if err := ApproveSessionTakeover(ctx, pool, id.WorkerID, r.SessionID, next.SessionID); err != nil {
					t.Fatal(err)
				}
			}
			wrong := next
			wrong.SessionID = uuid.NewString()
			if _, err := RegisterSession(ctx, pool, id, wrong); !errors.Is(err, ErrSessionActive) {
				t.Fatal("approval authorized wrong incarnation", err)
			}
			if _, err := RegisterSession(ctx, pool, id, next); err != nil {
				t.Fatal(err)
			}
			assertRecoveredAttempt(t, pool, job, attempt, tc.state, tc.attempt)
			if err := ApproveSessionTakeover(ctx, pool, id.WorkerID, r.SessionID, next.SessionID); err != nil {
				t.Fatal("approval replay failed", err)
			}
			if _, err := pool.Exec(ctx, "UPDATE worker_sessions SET fenced_at=NULL WHERE id=$1", r.SessionID); err == nil {
				t.Fatal("fencing was reversible")
			}
		})
	}
}

func TestTakeoverRollsBackFencingOnEventFailure(t *testing.T) {
	pool, id, r := sessionFixture(t)
	ctx := context.Background()
	job, attempt := assignedForRecovery(t, pool, id, r, nil)
	next := r
	next.RequestID = uuid.NewString()
	next.SessionID = uuid.NewString()
	if err := ApproveSessionTakeover(ctx, pool, id.WorkerID, r.SessionID, next.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_recovery_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected event failure'; END $$;
	CREATE TRIGGER reject_recovery_event BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION reject_recovery_event()`); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSession(ctx, pool, id, next); err == nil {
		t.Fatal("event failure ignored")
	}
	var state, current string
	var fenced, used *time.Time
	if err := pool.QueryRow(ctx, "SELECT state,current_attempt_id::text FROM jobs WHERE id=$1", job).Scan(&state, &current); err != nil || state != "ACTIVE" || current != attempt {
		t.Fatal("failed recovery lost ownership", err)
	}
	if err := pool.QueryRow(ctx, "SELECT fenced_at FROM worker_sessions WHERE id=$1", r.SessionID).Scan(&fenced); err != nil || fenced != nil {
		t.Fatal("failed recovery fenced original session", err)
	}
	if err := pool.QueryRow(ctx, "SELECT used_at FROM worker_takeovers WHERE worker_id=$1", id.WorkerID).Scan(&used); err != nil || used != nil {
		t.Fatal("failed recovery consumed approval", err)
	}
}
