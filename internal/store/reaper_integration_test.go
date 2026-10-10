//go:build integration

package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestExpiredAttemptReaperRecordsRetryOrTerminalFailureOnce(t *testing.T) {
	for _, tc := range []struct {
		name, jobState, attemptState string
		change                       func(*spec.Job)
		cancel                       bool
	}{
		{"retry", "RETRY_WAIT", "LOST", nil, false},
		{"exhausted", "FAILED", "LOST", func(j *spec.Job) { j.Spec.Retry.MaxAttempts = 1 }, false},
		{"not opted in", "FAILED", "LOST", func(j *spec.Job) { j.Spec.Retry.On = nil }, false},
		{"cancelled", "CANCELLED", "CANCELLED", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, identity, registration := sessionFixture(t)
			ctx := context.Background()
			job, attempt := assignedForRecovery(t, pool, identity, registration, tc.change)
			if tc.cancel {
				if _, err := pool.Exec(ctx, "UPDATE jobs SET state='CANCELLING',cancel_requested=true WHERE id=$1", job); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", attempt); err != nil {
				t.Fatal(err)
			}
			count, err := ReapExpiredAttempts(ctx, pool, 64)
			if err != nil || count != 1 {
				t.Fatal("expired attempt was not terminalized", count, err)
			}
			assertRecoveredAttempt(t, pool, job, attempt, tc.jobState, tc.attemptState)
			var workerState string
			var reconciled bool
			if err := pool.QueryRow(ctx, "SELECT state,reconciliation_complete FROM workers WHERE id=$1", identity.WorkerID).Scan(&workerState, &reconciled); err != nil || workerState != "REGISTERING" || reconciled {
				t.Fatal("uncertain host remained eligible after lease loss", workerState, reconciled, err)
			}
			var reason string
			var events int
			if err := pool.QueryRow(ctx, `SELECT a.reason,(SELECT count(*) FROM job_events WHERE job_id=$1 AND attempt_id=$2 AND type IN ('ATTEMPT_LOST','ATTEMPT_CANCELLED')) FROM attempts a WHERE a.id=$2`, job, attempt).Scan(&reason, &events); err != nil || events != 1 {
				t.Fatal("reaper did not record one durable loss event", reason, events, err)
			}
			if tc.cancel && reason != "USER_CANCELLED" || !tc.cancel && reason != "WORKER_LOST" {
				t.Fatal("incorrect reaper failure classification", reason)
			}
			if count, err := ReapExpiredAttempts(ctx, pool, 64); err != nil || count != 0 {
				t.Fatal("reaper repeated a terminal transition", count, err)
			}
		})
	}
}

func TestExpiredAttemptReaperRollsBackFailedEvent(t *testing.T) {
	testReaperRollsBackFailedEvent(t, false)
}

func TestPhaseDeadlineReaperRollsBackFailedEvent(t *testing.T) {
	testReaperRollsBackFailedEvent(t, true)
}

func testReaperRollsBackFailedEvent(t *testing.T, phaseTimeout bool) {
	t.Helper()
	pool, identity, registration := sessionFixture(t)
	ctx := context.Background()
	job, attempt := assignedForRecovery(t, pool, identity, registration, nil)
	expire := "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1"
	if phaseTimeout {
		expire = "UPDATE attempts SET phase_deadline=clock_timestamp()-interval '1 second' WHERE id=$1"
	}
	if _, err := pool.Exec(ctx, expire, attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_reaper_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected event failure'; END $$;
	CREATE TRIGGER reject_reaper_event BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION reject_reaper_event()`); err != nil {
		t.Fatal(err)
	}
	if _, err := ReapExpiredAttempts(ctx, pool, 64); err == nil {
		t.Fatal("reaper accepted a transition without its event")
	}
	var jobState, attemptState, reservation string
	if err := pool.QueryRow(ctx, `SELECT j.state,a.state,r.state FROM jobs j JOIN attempts a ON a.id=j.current_attempt_id JOIN reservations r ON r.attempt_id=a.id WHERE j.id=$1`, job).Scan(&jobState, &attemptState, &reservation); err != nil || jobState != "ACTIVE" || attemptState != "ASSIGNED" || reservation != "active" {
		t.Fatal("failed event partially terminalized an attempt", jobState, attemptState, reservation, err)
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER reject_reaper_event ON job_events"); err != nil {
		t.Fatal(err)
	}
	if count, err := ReapExpiredAttempts(ctx, pool, 64); err != nil || count != 1 {
		t.Fatal("retry did not recover the original expired attempt", count, err)
	}
}

func TestExpiredAttemptReaperRechecksLeaseAfterWaitingForJobLock(t *testing.T) {
	testReaperRechecksDeadlineAfterJobLock(t, false)
}

func TestPhaseDeadlineReaperRechecksAdvancedPhaseAfterWaitingForJobLock(t *testing.T) {
	testReaperRechecksDeadlineAfterJobLock(t, true)
}

func testReaperRechecksDeadlineAfterJobLock(t *testing.T, phaseTransition bool) {
	t.Helper()
	pool, identity, registration := sessionFixture(t)
	ctx := context.Background()
	job, attempt := assignedForRecovery(t, pool, identity, registration, nil)
	var oldExpiry time.Time
	initial := "UPDATE attempts SET lease_expires_at=clock_timestamp()+interval '1 second' WHERE id=$1 RETURNING lease_expires_at"
	if phaseTransition {
		initial = "UPDATE attempts SET state='STARTING',phase_deadline=clock_timestamp()+interval '1 second' WHERE id=$1 RETURNING phase_deadline"
	}
	if err := pool.QueryRow(ctx, initial, attempt).Scan(&oldExpiry); err != nil {
		t.Fatal(err)
	}
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(lock)
	if _, err := lock.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job); err != nil {
		t.Fatal(err)
	}
	// A renewal or phase transition wins the job lock before old expiry but has
	// not committed when the reaper scans. Only the post-lock deadlines apply.
	advance := "UPDATE attempts SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE id=$1"
	if phaseTransition {
		advance = "UPDATE attempts SET state='RUNNING',phase_deadline=clock_timestamp()+interval '30 seconds' WHERE id=$1"
	}
	if _, err := lock.Exec(ctx, advance, attempt); err != nil {
		t.Fatal(err)
	}
	for {
		var expired bool
		if err := pool.QueryRow(ctx, "SELECT clock_timestamp()>$1", oldExpiry).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cfg := pool.Config()
	app := "reaper_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	waiting, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer waiting.Close()
	type outcome struct {
		count int
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		count, err := ReapExpiredAttempts(ctx, waiting, 64)
		done <- outcome{count, err}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		var blocked bool
		// The first job lock may cover fail-fast siblings too. Observe this
		// dedicated reaper connection instead of coupling the gate to SQL text.
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')", app).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reaper did not wait behind the renewal's job lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if result := <-done; result.err != nil || result.count != 0 {
		t.Fatal("reaper fenced a renewed lease", result)
	}
	var state, reservation string
	wantState := "ASSIGNED"
	if phaseTransition {
		wantState = "RUNNING"
	}
	if err := pool.QueryRow(ctx, "SELECT a.state,r.state FROM attempts a JOIN reservations r ON r.attempt_id=a.id WHERE a.id=$1", attempt).Scan(&state, &reservation); err != nil || state != wantState || reservation != "active" {
		t.Fatal("reaper changed renewed authority", state, reservation, err)
	}
}
