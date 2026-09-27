//go:build integration

package store

import (
	"context"
	"sync"
	"testing"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

func TestRequestCancellationTransitionsAndReplay(t *testing.T) {
	for _, initial := range []string{"QUEUED", "RETRY_WAIT", "ACTIVE"} {
		t.Run(initial, func(t *testing.T) {
			pool, worker, session := readyAcquisitionWorker(t)
			ctx := context.Background()
			job := queueAcquisitionJob(t, pool, func(job *spec.Job) {
				job.Spec.Retry.MaxAttempts = 2
				job.Spec.Retry.On = []string{"WORKER_LOST"}
			})
			var attempt string
			if initial != "QUEUED" {
				assigned, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: session.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
				if err != nil || assigned.Assignment == nil {
					t.Fatal(assigned, err)
				}
				attempt = assigned.Assignment.Authority.AttemptID
			}
			if initial == "RETRY_WAIT" {
				if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", attempt); err != nil {
					t.Fatal(err)
				}
				if _, err := ReapExpiredAttempts(ctx, pool, 64); err != nil {
					t.Fatal(err)
				}
			}
			want := "CANCELLED"
			if initial == "ACTIVE" {
				want = "CANCELLING"
			}
			for range 2 {
				result, err := RequestCancellation(ctx, pool, job.ProjectID, job.ID)
				if err != nil || result.State != want {
					t.Fatal(result, err)
				}
			}
			var state string
			var cancelled bool
			var events, audits int
			err := pool.QueryRow(ctx, `SELECT state,cancel_requested,
				(SELECT count(*) FROM job_events WHERE job_id=$1 AND type='CANCEL_REQUESTED'),
				(SELECT count(*) FROM audit_events WHERE project_id=$2 AND action='JOB_CANCEL_REQUESTED')
				FROM jobs WHERE id=$1`, job.ID, job.ProjectID).Scan(&state, &cancelled, &events, &audits)
			if err != nil || state != want || !cancelled || events != 1 || audits != 1 {
				t.Fatal(state, cancelled, events, audits, err)
			}
			if initial == "ACTIVE" {
				var current, reservation string
				if err := pool.QueryRow(ctx, "SELECT current_attempt_id::text FROM jobs WHERE id=$1", job.ID).Scan(&current); err != nil || current != attempt {
					t.Fatal("active attempt was detached", current, err)
				}
				if err := pool.QueryRow(ctx, "SELECT state FROM reservations WHERE attempt_id=$1", attempt).Scan(&reservation); err != nil || reservation != "active" {
					t.Fatal("capacity released before stop confirmation", reservation, err)
				}
			}
		})
	}
}

func TestCancellationAndCompletionCommitOrder(t *testing.T) {
	for range 6 {
		pool, worker, completion := completedOutputFixture(t)
		ctx := context.Background()
		var projectID string
		if err := pool.QueryRow(ctx, "SELECT project_id::text FROM jobs WHERE id=$1", completion.Authority.JobID).Scan(&projectID); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		var cancelErr, completeErr error
		var result CompletionResult
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, cancelErr = RequestCancellation(ctx, pool, projectID, completion.Authority.JobID)
		}()
		go func() {
			defer wg.Done()
			<-start
			result, completeErr = CompleteAttempt(ctx, pool, worker, completion)
		}()
		close(start)
		wg.Wait()
		if cancelErr != nil || completeErr != nil {
			t.Fatal(cancelErr, completeErr)
		}
		var state string
		var cancelled bool
		var accepted *string
		if err := pool.QueryRow(ctx, "SELECT state,cancel_requested,accepted_attempt_id::text FROM jobs WHERE id=$1", completion.Authority.JobID).Scan(&state, &cancelled, &accepted); err != nil {
			t.Fatal(err)
		}
		switch result.Decision {
		case "ACCEPTED":
			if result.State != "SUCCEEDED" || state != "SUCCEEDED" || cancelled || accepted == nil || *accepted != completion.Authority.AttemptID {
				t.Fatal("success did not win cleanly", result, state, cancelled, accepted)
			}
		case "STOP_REQUESTED":
			if state != "CANCELLING" || !cancelled || accepted != nil {
				t.Fatal("cancellation did not fence success", result, state, cancelled, accepted)
			}
			completion.Reason = "USER_CANCELLED"
			completion.Outputs = nil
			signCompletion(t, &completion)
			stopped, err := CompleteAttempt(ctx, pool, worker, completion)
			if err != nil || stopped.State != "CANCELLED" {
				t.Fatal(stopped, err)
			}
		default:
			t.Fatal("unexpected completion race result", result)
		}
	}
}

func TestCancellationEventFailureRollsBackIntent(t *testing.T) {
	pool, _, _ := readyAcquisitionWorker(t)
	ctx := context.Background()
	job := queueAcquisitionJob(t, pool, nil)
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_cancel_event() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
		IF NEW.type='CANCEL_REQUESTED' THEN RAISE EXCEPTION 'event failure'; END IF;
		RETURN NEW;
	END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "CREATE TRIGGER reject_cancel_event BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION reject_cancel_event()"); err != nil {
		t.Fatal(err)
	}
	if _, err := RequestCancellation(ctx, pool, job.ProjectID, job.ID); err == nil {
		t.Fatal("cancellation committed without its event")
	}
	var state string
	var cancelled bool
	var audits int
	if err := pool.QueryRow(ctx, "SELECT state,cancel_requested,(SELECT count(*) FROM audit_events WHERE action='JOB_CANCEL_REQUESTED') FROM jobs WHERE id=$1", job.ID).Scan(&state, &cancelled, &audits); err != nil || state != "QUEUED" || cancelled || audits != 0 {
		t.Fatal("failed event left cancellation intent", state, cancelled, audits, err)
	}
}
