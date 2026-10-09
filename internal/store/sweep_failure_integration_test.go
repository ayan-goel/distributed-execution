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

func TestSweepFailFastOnlyAfterPermanentFailure(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		failFast, cancelRunning bool
		retry, lost, takeover   bool
	}{
		{name: "queued siblings only", failFast: true},
		{name: "also cancel active sibling", failFast: true, cancelRunning: true},
		{name: "disabled"},
		{name: "retry preserves siblings", failFast: true, cancelRunning: true, retry: true},
		{name: "lost child", failFast: true, lost: true},
		{name: "lost child also cancels active sibling", failFast: true, cancelRunning: true, lost: true},
		{name: "lost retry preserves siblings", failFast: true, cancelRunning: true, retry: true, lost: true},
		{name: "session takeover cancels queued siblings", failFast: true, takeover: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, id, registration := readyAcquisitionWorker(t)
			ctx := context.Background()
			job, _ := admittedExample(t)
			job.Spec.Placement.Labels["architecture"] = "arm64"
			job.Spec.Retry.MaxAttempts = 1
			if tc.retry {
				job.Spec.Retry.MaxAttempts = 2
				job.Spec.Retry.On = []string{"TRANSFER_FAILED", "WORKER_LOST"}
			}
			sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep",
				Metadata: spec.Metadata{Name: "failure-grid", Project: "research"},
				Spec: spec.SweepSpec{JobTemplate: job, Matrix: map[string][]string{"SEED": {"1", "2", "3"}},
					MaxConcurrent: 2, FailFast: tc.failFast, CancelRunningOnFailure: tc.cancelRunning}}
			created, err := SubmitSweepResolved(ctx, pool, uuid.NewString(), strings.Repeat("f", 64), sweep, nil)
			if err != nil {
				t.Fatal(err)
			}
			acquire := func() AttemptAuthority {
				t.Helper()
				result, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
				if err != nil || result.Assignment == nil {
					t.Fatal(result, err)
				}
				return result.Assignment.Authority
			}
			failed, active := acquire(), acquire()
			if tc.cancelRunning && failed.JobID < active.JobID {
				failed, active = active, failed
			}
			unrelated := queueAcquisitionJob(t, pool, nil)
			request := CompletionRequest{Authority: failed, CompletionID: uuid.NewString(), Reason: "TRANSFER_FAILED", Stopped: true, LogsComplete: true}
			signCompletion(t, &request)
			if tc.failFast && tc.cancelRunning && !tc.retry && !tc.lost && !tc.takeover {
				assertSweepFailureRollback(t, pool, id, request)
				assertSweepFailureLockOrder(t, pool, id, request, active.JobID)
			}
			if tc.takeover {
				next := registration
				next.RequestID, next.SessionID = uuid.NewString(), uuid.NewString()
				if err := ApproveSessionTakeover(ctx, pool, id.WorkerID, registration.SessionID, next.SessionID); err != nil {
					t.Fatal(err)
				}
				if _, err := RegisterSession(ctx, pool, id, next); err != nil {
					t.Fatal(err)
				}
			} else if tc.lost {
				if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", failed.AttemptID); err != nil {
					t.Fatal(err)
				}
				if count, err := ReapExpiredAttempts(ctx, pool, 1); err != nil || count != 1 {
					t.Fatal("failed child was not reaped", count, err)
				}
			} else {
				for range 2 {
					if result, err := CompleteAttempt(ctx, pool, id, request); err != nil || result.Decision != "ACCEPTED" || result.State != "FAILED" {
						t.Fatal("failure or its replay rejected", result, err)
					}
				}
			}
			triggered := tc.failFast && !tc.retry
			for _, childID := range created.ChildIDs {
				want, cancelled, events := "QUEUED", triggered, 0
				if triggered {
					want, events = "CANCELLED", 1
				}
				if childID == failed.JobID {
					want, cancelled, events = "FAILED", false, 0
					if tc.retry {
						want = "RETRY_WAIT"
					}
				} else if childID == active.JobID {
					want, cancelled, events = "ACTIVE", false, 0
					if tc.takeover {
						want = "FAILED"
					}
					if triggered && tc.cancelRunning {
						want, cancelled, events = "CANCELLING", true, 1
					}
				}
				var state string
				var stop bool
				var count int
				if err := pool.QueryRow(ctx, `SELECT state,cancel_requested,(SELECT count(*) FROM job_events WHERE job_id=j.id AND type='SWEEP_FAIL_FAST') FROM jobs j WHERE id=$1`, childID).Scan(&state, &stop, &count); err != nil || state != want || stop != cancelled || count != events {
					t.Fatalf("child %s: state=%s stop=%v events=%d; want %s %v %d; %v", childID, state, stop, count, want, cancelled, events, err)
				}
			}
			var reservation, unrelatedState string
			wantReservation := "active"
			if tc.takeover {
				wantReservation = "quarantined"
			}
			if err := pool.QueryRow(ctx, "SELECT state FROM reservations WHERE attempt_id=$1", active.AttemptID).Scan(&reservation); err != nil || reservation != wantReservation {
				t.Fatal("sibling capacity released without physical stop", reservation, err)
			}
			if err := pool.QueryRow(ctx, "SELECT state FROM jobs WHERE id=$1", unrelated.ID).Scan(&unrelatedState); err != nil || unrelatedState != "QUEUED" {
				t.Fatal("unrelated job changed", unrelatedState, err)
			}
			if !tc.failFast {
				if next := acquire(); next.JobID != created.ChildIDs[2] {
					t.Fatal("terminal child did not release sweep capacity", next)
				}
			}
			if triggered && tc.cancelRunning && !tc.lost && !tc.takeover {
				grants, err := RenewLeases(ctx, pool, id, LeaseRenewal{SessionID: registration.SessionID, RequestID: uuid.NewString(), Attempts: []AttemptAuthority{active}})
				if err != nil || len(grants) != 1 || grants[0].Decision != "STOP_REQUESTED" {
					t.Fatal("active sibling did not receive stop intent", grants, err)
				}
				stop := CompletionRequest{Authority: active, CompletionID: uuid.NewString(), Reason: "USER_CANCELLED", Stopped: true, LogsComplete: true}
				signCompletion(t, &stop)
				if result, err := CompleteAttempt(ctx, pool, id, stop); err != nil || result.State != "CANCELLED" {
					t.Fatal("physical stop acknowledgement rejected", result, err)
				}
				if err := pool.QueryRow(ctx, "SELECT state FROM reservations WHERE attempt_id=$1", active.AttemptID).Scan(&reservation); err != nil || reservation != "released" {
					t.Fatal("confirmed stop did not release capacity", reservation, err)
				}
			}
		})
	}
}

func assertSweepFailureRollback(t *testing.T, pool *pgxpool.Pool, id WorkerIdentity, request CompletionRequest) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "ALTER TABLE job_events ADD CONSTRAINT reject_fail_fast CHECK (type<>'SWEEP_FAIL_FAST')"); err != nil {
		t.Fatal(err)
	}
	if _, err := CompleteAttempt(ctx, pool, id, request); err == nil {
		t.Fatal("sibling event failure did not roll back completion")
	}
	var unchanged bool
	if err := pool.QueryRow(ctx, `SELECT j.state='ACTIVE' AND a.state='ASSIGNED' AND r.state='active'
		AND NOT EXISTS(SELECT 1 FROM attempt_completions WHERE attempt_id=a.id)
		AND NOT EXISTS(SELECT 1 FROM jobs WHERE cancel_requested)
		FROM jobs j JOIN attempts a ON a.id=j.current_attempt_id JOIN reservations r ON r.attempt_id=a.id WHERE j.id=$1`, request.Authority.JobID).Scan(&unchanged); err != nil || !unchanged {
		t.Fatal("partial failure committed", unchanged, err)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE job_events DROP CONSTRAINT reject_fail_fast"); err != nil {
		t.Fatal(err)
	}
}

func assertSweepFailureLockOrder(t *testing.T, pool *pgxpool.Pool, id WorkerIdentity, request CompletionRequest, lowerJobID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err := blocker.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", lowerJobID); err != nil {
		t.Fatal(err)
	}
	config := pool.Config()
	app := "sweep_failure_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	config.ConnConfig.RuntimeParams["application_name"] = app
	waiting, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer waiting.Close()
	done := make(chan error, 1)
	go func() {
		_, err := CompleteAttempt(ctx, waiting, id, request)
		done <- err
	}()
	for {
		var blocked bool
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')", app).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Emulate the remainder of a sorted renewal batch. Completion must not hold
	// the higher failed job while waiting for this lower sibling's job lock.
	if _, err := blocker.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE NOWAIT", request.Authority.JobID); err != nil {
		t.Fatal("completion inverted renewal job order", err)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
