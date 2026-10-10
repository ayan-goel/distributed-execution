//go:build integration

package store

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestPhaseDeadlineReaperClassifiesEarliestExpiryWithoutRetry(t *testing.T) {
	for _, tc := range []struct {
		name, phase, leaseOffset, phaseOffset, reason, state, event string
		cancel                                                      bool
	}{
		{"assigned", "ASSIGNED", "30 seconds", "-1 second", "STARTUP_TIMEOUT", "FAILED", "ATTEMPT_TIMED_OUT", false},
		{"starting", "STARTING", "30 seconds", "-1 second", "STARTUP_TIMEOUT", "FAILED", "ATTEMPT_TIMED_OUT", false},
		{"running", "RUNNING", "30 seconds", "-1 second", "EXECUTION_TIMEOUT", "FAILED", "ATTEMPT_TIMED_OUT", false},
		{"finalizing", "FINALIZING", "30 seconds", "-1 second", "FINALIZATION_TIMEOUT", "FAILED", "ATTEMPT_TIMED_OUT", false},
		{"phase expired before lease", "RUNNING", "-1 second", "-2 seconds", "EXECUTION_TIMEOUT", "FAILED", "ATTEMPT_TIMED_OUT", false},
		{"lease expired before phase", "RUNNING", "-2 seconds", "-1 second", "WORKER_LOST", "LOST", "ATTEMPT_LOST", false},
		{"equal deadlines", "RUNNING", "-1 second", "-1 second", "EXECUTION_TIMEOUT", "FAILED", "ATTEMPT_TIMED_OUT", false},
		{"cancel beats timeout", "RUNNING", "30 seconds", "-1 second", "USER_CANCELLED", "CANCELLED", "ATTEMPT_CANCELLED", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, identity, registration := sessionFixture(t)
			ctx := context.Background()
			job, attempt := assignedForRecovery(t, pool, identity, registration, nil)
			// One database time sample makes equal-deadline precedence exact.
			if _, err := pool.Exec(ctx, `UPDATE attempts SET state=$2,
				lease_expires_at=statement_timestamp()+$3::interval,
				phase_deadline=statement_timestamp()+$4::interval WHERE id=$1`, attempt, tc.phase, tc.leaseOffset, tc.phaseOffset); err != nil {
				t.Fatal(err)
			}
			if tc.cancel {
				var project string
				if err := pool.QueryRow(ctx, "SELECT project_id::text FROM jobs WHERE id=$1", job).Scan(&project); err != nil {
					t.Fatal(err)
				}
				if _, err := RequestCancellation(ctx, pool, project, job); err != nil {
					t.Fatal(err)
				}
			}
			count, err := ReapExpiredAttempts(ctx, pool, 1)
			if err != nil || count != 1 {
				t.Fatal("deadline was not reaped", count, err)
			}
			wantJob := tc.state
			if tc.reason == "WORKER_LOST" {
				wantJob = "RETRY_WAIT"
			}
			assertRecoveredAttempt(t, pool, job, attempt, wantJob, tc.state)
			var reason, event, workerState string
			var events, completions int
			var reconciled bool
			if err := pool.QueryRow(ctx, `SELECT a.reason,
				(SELECT type FROM job_events WHERE attempt_id=a.id ORDER BY sequence DESC LIMIT 1),
				(SELECT count(*) FROM job_events WHERE attempt_id=a.id),
				(SELECT count(*) FROM attempt_completions WHERE attempt_id=a.id),
				w.state,w.reconciliation_complete FROM attempts a JOIN workers w ON w.id=a.worker_id WHERE a.id=$1`, attempt).
				Scan(&reason, &event, &events, &completions, &workerState, &reconciled); err != nil {
				t.Fatal(err)
			}
			if reason != tc.reason || event != tc.event || events != 1 || completions != 0 || workerState != "REGISTERING" || reconciled {
				t.Fatal("incorrect durable deadline outcome", reason, event, events, completions, workerState, reconciled)
			}
			if count, err := ReapExpiredAttempts(ctx, pool, 1); err != nil || count != 0 {
				t.Fatal("terminal deadline was processed twice", count, err)
			}
			late := completionRequest()
			late.Authority = AttemptAuthority{JobID: job, AttemptID: attempt, WorkerID: identity.WorkerID, SessionID: registration.SessionID, Generation: 1}
			signCompletion(t, &late)
			if result, err := CompleteAttempt(ctx, pool, identity, late); err != nil || result.Decision != "ALREADY_TERMINAL" || len(result.Manifest) != 0 {
				t.Fatal("late success changed a deadline outcome", result, err)
			}
			var accepted bool
			if err := pool.QueryRow(ctx, "SELECT accepted_attempt_id IS NOT NULL OR accepted_manifest IS NOT NULL FROM jobs WHERE id=$1", job).Scan(&accepted); err != nil || accepted {
				t.Fatal("deadline outcome published late success", accepted, err)
			}
		})
	}
}

func TestPhaseDeadlineClassificationSurvivesSessionTakeover(t *testing.T) {
	pool, identity, registration := sessionFixture(t)
	ctx := context.Background()
	job, attempt := assignedForRecovery(t, pool, identity, registration, nil)
	if _, err := pool.Exec(ctx, "UPDATE attempts SET state='RUNNING',phase_deadline=clock_timestamp()-interval '1 second' WHERE id=$1", attempt); err != nil {
		t.Fatal(err)
	}
	next := registration
	next.RequestID, next.SessionID = uuid.NewString(), uuid.NewString()
	if err := ApproveSessionTakeover(ctx, pool, identity.WorkerID, registration.SessionID, next.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSession(ctx, pool, identity, next); err != nil {
		t.Fatal(err)
	}
	assertRecoveredAttempt(t, pool, job, attempt, "FAILED", "FAILED")
	var reason string
	if err := pool.QueryRow(ctx, "SELECT reason FROM attempts WHERE id=$1", attempt).Scan(&reason); err != nil || reason != "EXECUTION_TIMEOUT" {
		t.Fatal("takeover reclassified a known timeout as worker loss", reason, err)
	}
}
