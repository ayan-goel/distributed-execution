//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPhaseLifecycleReplayAndDeadlines(t *testing.T) {
	pool, id, _, assigned := leaseFixture(t)
	ctx := context.Background()
	report := PhaseReport{Authority: assigned.Authority, EventID: uuid.NewString(), Phase: "STARTING"}
	var wg sync.WaitGroup
	failures := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := ReportPhase(ctx, pool, id, report)
			if e == nil && (r.Decision != "ACCEPTED" || r.State != "STARTING") {
				e = errors.New("unexpected phase reply")
			}
			failures <- e
		}()
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		if e != nil {
			t.Fatal(e)
		}
	}
	read := func() (string, time.Time, time.Time, *string, *int32) {
		var state string
		var lease, phase time.Time
		var container *string
		var exit *int32
		if e := pool.QueryRow(ctx, "SELECT state,lease_expires_at,phase_deadline,container_id,exit_code FROM attempts WHERE id=$1", assigned.Authority.AttemptID).Scan(&state, &lease, &phase, &container, &exit); e != nil {
			t.Fatal(e)
		}
		return state, lease, phase, container, exit
	}
	state, lease, phase, container, exit := read()
	if state != "STARTING" || !lease.Equal(assigned.LeaseExpiresAt) || !phase.Equal(assigned.PhaseDeadline) || container != nil || exit != nil {
		t.Fatal("startup acknowledgement extended authority", state, lease, phase)
	}
	starting := report
	report = PhaseReport{Authority: assigned.Authority, EventID: uuid.NewString(), Phase: "RUNNING", ContainerID: strings.Repeat("a", 64)}
	var before time.Time
	if e := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&before); e != nil {
		t.Fatal(e)
	}
	if r, e := ReportPhase(ctx, pool, id, report); e != nil || r.State != "RUNNING" || r.Decision != "ACCEPTED" {
		t.Fatal(r, e)
	}
	_, lease, phase, container, exit = read()
	var after time.Time
	if e := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&after); e != nil {
		t.Fatal(e)
	}
	if phase.After(after.Add(time.Duration(assigned.Job.Spec.Timeouts.ExecutionSeconds) * time.Second)) {
		t.Fatal("execution deadline exceeds configured timeout", phase, after)
	}
	if !lease.Equal(assigned.LeaseExpiresAt) || container == nil || *container != report.ContainerID || exit != nil || phase.Before(before.Add(time.Duration(assigned.Job.Spec.Timeouts.ExecutionSeconds)*time.Second)) {
		t.Fatal("incorrect execution deadline/binding", lease, phase, container, exit)
	}
	running := report
	for range 2 {
		if r, e := ReportPhase(ctx, pool, id, report); e != nil || r.State != "RUNNING" {
			t.Fatal(r, e)
		}
		report.EventID = uuid.NewString()
	}
	_, _, again, _, _ := read()
	if !again.Equal(phase) {
		t.Fatal("repeated phase extended deadline", phase, again)
	}
	code := int32(7)
	report = PhaseReport{Authority: assigned.Authority, EventID: uuid.NewString(), Phase: "FINALIZING", ContainerID: running.ContainerID, ExitCode: &code}
	if e := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&before); e != nil {
		t.Fatal(e)
	}
	if r, e := ReportPhase(ctx, pool, id, report); e != nil || r.State != "FINALIZING" || r.Decision != "ACCEPTED" {
		t.Fatal(r, e)
	}
	state, lease, phase, container, exit = read()
	if e := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&after); e != nil {
		t.Fatal(e)
	}
	if phase.After(after.Add(time.Duration(assigned.Job.Spec.Timeouts.FinalizationSeconds) * time.Second)) {
		t.Fatal("finalization deadline exceeds configured timeout", phase, after)
	}
	if state != "FINALIZING" || !lease.Equal(assigned.LeaseExpiresAt) || exit == nil || *exit != 7 || phase.Before(before.Add(time.Duration(assigned.Job.Spec.Timeouts.FinalizationSeconds)*time.Second)) {
		t.Fatal(state, lease, phase, exit)
	}
	for _, old := range []PhaseReport{starting, running, report} {
		if r, e := ReportPhase(ctx, pool, id, old); e != nil || r.State != "FINALIZING" || r.Decision != "ACCEPTED" {
			t.Fatal("old replay regressed phase", r, e)
		}
	}
	_, _, again, _, _ = read()
	if !again.Equal(phase) {
		t.Fatal("old replay changed finalization deadline", phase, again)
	}
	var events int
	if e := pool.QueryRow(ctx, "SELECT count(*) FROM job_events WHERE type='PHASE_CHANGED'").Scan(&events); e != nil || events != 3 {
		t.Fatal(events, e)
	}
	var reservation, jobState string
	if e := pool.QueryRow(ctx, "SELECT r.state,j.state FROM reservations r JOIN attempts a ON a.id=r.attempt_id JOIN jobs j ON j.id=a.job_id").Scan(&reservation, &jobState); e != nil || reservation != "active" || jobState != "ACTIVE" {
		t.Fatal("phase released capacity or completed job", reservation, jobState, e)
	}
	if _, e := pool.Exec(ctx, "UPDATE attempts SET container_id=$2 WHERE id=$1", assigned.Authority.AttemptID, strings.Repeat("b", 64)); e == nil {
		t.Fatal("database allowed rebinding container identity")
	}
}

func TestPhaseConcurrentCrossAttemptEventConflict(t *testing.T) {
	pool, id, _, first := leaseFixture(t)
	ctx := context.Background()
	queueAcquisitionJob(t, pool, nil)
	second, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: first.Authority.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || second.Assignment == nil {
		t.Fatal(second, err)
	}
	event := uuid.NewString()
	failures := make(chan error, 2)
	for _, authority := range []AttemptAuthority{first.Authority, second.Assignment.Authority} {
		go func() {
			_, err := ReportPhase(ctx, pool, id, PhaseReport{Authority: authority, EventID: event, Phase: "STARTING"})
			failures <- err
		}()
	}
	one, two := <-failures, <-failures
	if !((one == nil && errors.Is(two, ErrConflict)) || (two == nil && errors.Is(one, ErrConflict))) {
		t.Fatal(one, two)
	}
	var started, events, history int
	if err = pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM attempts WHERE state='STARTING'),(SELECT count(*) FROM job_events WHERE type='PHASE_CHANGED'),(SELECT count(*) FROM attempt_phase_reports)").Scan(&started, &events, &history); err != nil || started != 1 || events != 1 || history != 1 {
		t.Fatal("conflict leaked partial transition", started, events, history, err)
	}
}

func TestPhaseTerminalAttemptAndSchedulerIndependence(t *testing.T) {
	pool, id, _, assigned := leaseFixture(t)
	ctx := context.Background()
	r := PhaseReport{Authority: assigned.Authority, EventID: uuid.NewString(), Phase: "STARTING"}
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err = blocker.Exec(ctx, "SELECT pg_advisory_xact_lock(1146310734); SELECT id FROM workers FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if result, err := ReportPhase(bounded, pool, id, r); err != nil || result.Decision != "ACCEPTED" {
		t.Fatal("phase depends on scheduler/worker locks", result, err)
	}
	if err = blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	if _, _, err = lockLeaseBatch(ctx, tx, []AttemptAuthority{assigned.Authority}); err != nil {
		t.Fatal(err)
	}
	if err = fenceLostAttempt(ctx, tx, recoveryAttempt{JobID: assigned.Authority.JobID, ID: assigned.Authority.AttemptID}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if result, err := ReportPhase(ctx, pool, id, r); err != nil || result.Decision != "ALREADY_TERMINAL" || result.State != "LOST" {
			t.Fatal(result, err)
		}
		r.EventID = uuid.NewString()
	}
	var history int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM attempt_phase_reports").Scan(&history); err != nil || history != 1 {
		t.Fatal("rejected report was committed", history, err)
	}
}

func TestPhaseTransitionsConflictAndFastExit(t *testing.T) {
	pool, id, _, assigned := leaseFixture(t)
	ctx := context.Background()
	report := PhaseReport{Authority: assigned.Authority, EventID: uuid.NewString(), Phase: "RUNNING", ContainerID: strings.Repeat("a", 64)}
	if _, e := ReportPhase(ctx, pool, id, report); !errors.Is(e, ErrConflict) {
		t.Fatal("running skipped startup acknowledgement", e)
	}
	report.Phase = "STARTING"
	report.ContainerID = ""
	if _, e := ReportPhase(ctx, pool, id, report); e != nil {
		t.Fatal(e)
	}
	changed := report
	changed.Phase = "RUNNING"
	changed.ContainerID = strings.Repeat("a", 64)
	if _, e := ReportPhase(ctx, pool, id, changed); !errors.Is(e, ErrConflict) {
		t.Fatal("changed event payload accepted", e)
	}
	code := int32(0)
	final := PhaseReport{Authority: assigned.Authority, EventID: uuid.NewString(), Phase: "FINALIZING", ContainerID: strings.Repeat("a", 64), ExitCode: &code}
	if r, e := ReportPhase(ctx, pool, id, final); e != nil || r.State != "FINALIZING" || r.Decision != "ACCEPTED" {
		t.Fatal("fast exit rejected", r, e)
	}
	changed.EventID = uuid.NewString()
	if _, e := ReportPhase(ctx, pool, id, changed); !errors.Is(e, ErrConflict) {
		t.Fatal("fresh event moved phase backward", e)
	}
	final.EventID = uuid.NewString()
	final.ContainerID = strings.Repeat("b", 64)
	if _, e := ReportPhase(ctx, pool, id, final); !errors.Is(e, ErrConflict) {
		t.Fatal("container binding changed", e)
	}
	final.ContainerID = strings.Repeat("a", 64)
	code = 1
	if _, e := ReportPhase(ctx, pool, id, final); !errors.Is(e, ErrConflict) {
		t.Fatal("exit code changed", e)
	}
}

func TestPhaseExpiryCancellationAndStaleAuthority(t *testing.T) {
	for _, tc := range []struct{ name, sql, decision string }{
		{"lease expired", "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second'", "FENCED"},
		{"phase expired", "UPDATE attempts SET phase_deadline=clock_timestamp()-interval '1 second'", "STOP_REQUESTED"},
		{"cancelled", "UPDATE jobs SET state='CANCELLING',cancel_requested=true", "STOP_REQUESTED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, id, _, assigned := leaseFixture(t)
			ctx := context.Background()
			r := PhaseReport{Authority: assigned.Authority, EventID: uuid.NewString(), Phase: "STARTING"}
			if _, e := ReportPhase(ctx, pool, id, r); e != nil {
				t.Fatal(e)
			}
			if _, e := pool.Exec(ctx, tc.sql); e != nil {
				t.Fatal(e)
			}
			for range 2 {
				if result, e := ReportPhase(ctx, pool, id, r); e != nil || result.Decision != tc.decision {
					t.Fatal(result, e)
				}
				r.EventID = uuid.NewString()
			}
		})
	}
	pool, id, _, assigned := leaseFixture(t)
	ctx := context.Background()
	r := PhaseReport{Authority: assigned.Authority, EventID: uuid.NewString(), Phase: "STARTING"}
	r.Authority.Generation++
	if result, e := ReportPhase(ctx, pool, id, r); e != nil || result.Decision != "FENCED" || result.State != "" {
		t.Fatal(result, e)
	}
	r.Authority = assigned.Authority
	r.Authority.JobID = uuid.NewString()
	if result, e := ReportPhase(ctx, pool, id, r); e != nil || result.Decision != "FENCED" {
		t.Fatal(result, e)
	}
	r.Authority = assigned.Authority
	if e := RevokeWorkerCredential(ctx, pool, id.WorkerID, id.CredentialID); e != nil {
		t.Fatal(e)
	}
	if _, e := ReportPhase(ctx, pool, id, r); !errors.Is(e, ErrUnauthorized) {
		t.Fatal(e)
	}
}

func TestPhaseEventFailureRollsBackTransition(t *testing.T) {
	pool, id, _, assigned := leaseFixture(t)
	ctx := context.Background()
	if _, e := pool.Exec(ctx, `CREATE FUNCTION reject_phase_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected event failure'; END $$; CREATE TRIGGER reject_phase_event BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION reject_phase_event()`); e != nil {
		t.Fatal(e)
	}
	r := PhaseReport{Authority: assigned.Authority, EventID: uuid.NewString(), Phase: "STARTING"}
	if _, e := ReportPhase(ctx, pool, id, r); e == nil {
		t.Fatal("event failure accepted transition")
	}
	var state string
	var count int
	if e := pool.QueryRow(ctx, "SELECT state FROM attempts").Scan(&state); e != nil || state != "ASSIGNED" {
		t.Fatal(state, e)
	}
	if e := pool.QueryRow(ctx, "SELECT count(*) FROM attempt_phase_reports").Scan(&count); e != nil || count != 0 {
		t.Fatal(count, e)
	}
	if _, e := pool.Exec(ctx, "DROP TRIGGER reject_phase_event ON job_events"); e != nil {
		t.Fatal(e)
	}
	if result, e := ReportPhase(ctx, pool, id, r); e != nil || result.Decision != "ACCEPTED" {
		t.Fatal(result, e)
	}
}

func TestPhaseUsesFreshTimeAfterObservedJobLock(t *testing.T) {
	pool, id, _, assigned := leaseFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blocker, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer rollback(blocker)
	if _, e = blocker.Exec(ctx, "SELECT id FROM jobs FOR UPDATE"); e != nil {
		t.Fatal(e)
	}
	cfg := pool.Config()
	app := "phase_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	waiting, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer waiting.Close()
	r := PhaseReport{Authority: assigned.Authority, EventID: uuid.NewString(), Phase: "STARTING"}
	type outcome struct {
		result MutationResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() { r, e := ReportPhase(ctx, waiting, id, r); done <- outcome{r, e} }()
	until := time.Now().Add(time.Second)
	for {
		var blocked bool
		if e := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')", app).Scan(&blocked); e != nil {
			t.Fatal(e)
		}
		if blocked {
			break
		}
		if time.Now().After(until) {
			t.Fatal("phase did not wait for job lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, e = blocker.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()"); e != nil {
		t.Fatal(e)
	}
	if e = blocker.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	result := <-done
	if result.err != nil || result.result.Decision != "FENCED" {
		t.Fatal("phase accepted expired authority", result)
	}
}
