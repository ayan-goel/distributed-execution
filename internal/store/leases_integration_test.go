//go:build integration

package store

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
	"sync"
	"testing"
	"time"
)

func leaseFixture(t *testing.T) (*pgxpool.Pool, WorkerIdentity, LeaseRenewal, WorkAssignment) {
	t.Helper()
	pool, id, r := readyAcquisitionWorker(t)
	queueAcquisitionJob(t, pool, nil)
	result, err := AcquireWork(context.Background(), pool, id, AcquisitionRequest{SessionID: r.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || result.Assignment == nil {
		t.Fatal(result, err)
	}
	return pool, id, LeaseRenewal{SessionID: r.SessionID, RequestID: uuid.NewString(), Attempts: []AttemptAuthority{result.Assignment.Authority}}, *result.Assignment
}

func TestLeaseRenewalIndependentOfSchedulerAndWorkerLocks(t *testing.T) {
	pool, id, r, _ := leaseFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// Draining stops acquisition but must leave existing attempts able to finish.
	if _, err := pool.Exec(ctx, "UPDATE workers SET drain_requested=true"); err != nil {
		t.Fatal(err)
	}
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err = blocker.Exec(ctx, "SELECT pg_advisory_xact_lock(1146310734); SELECT id FROM workers FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	grants, err := RenewLeases(ctx, pool, id, r)
	if err != nil || len(grants) != 1 || grants[0].Decision != "ACCEPTED" {
		t.Fatal("renewal depended on scheduler/worker locks", grants, err)
	}
}

func TestLeaseRenewalRollbackOnHistoryFailure(t *testing.T) {
	pool, id, r, _ := leaseFixture(t)
	ctx := context.Background()
	queueAcquisitionJob(t, pool, nil)
	second, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: r.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || second.Assignment == nil {
		t.Fatal(second, err)
	}
	r.Attempts = append(r.Attempts, second.Assignment.Authority)
	readExpiries := func() []time.Time {
		rows, e := pool.Query(ctx, "SELECT lease_expires_at FROM attempts ORDER BY id")
		if e != nil {
			t.Fatal(e)
		}
		times, e := pgx.CollectRows(rows, pgx.RowTo[time.Time])
		if e != nil {
			t.Fatal(e)
		}
		return times
	}
	before := readExpiries()
	if _, err = pool.Exec(ctx, `CREATE FUNCTION reject_lease_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected history failure'; END $$;
	CREATE TRIGGER reject_lease_history BEFORE UPDATE ON worker_lease_requests FOR EACH ROW EXECUTE FUNCTION reject_lease_history()`); err != nil {
		t.Fatal(err)
	}
	grants, err := RenewLeases(ctx, pool, id, r)
	if err == nil || len(grants) != 0 {
		t.Fatal("failed transaction exposed grants", grants, err)
	}
	after := readExpiries()
	for i := range before {
		if !before[i].Equal(after[i]) {
			t.Fatal("partial lease extension survived rollback", before, after)
		}
	}
	var count int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM worker_lease_requests").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if _, err = pool.Exec(ctx, "DROP TRIGGER reject_lease_history ON worker_lease_requests"); err != nil {
		t.Fatal(err)
	}
	if grants, err = RenewLeases(ctx, pool, id, r); err != nil || len(grants) != 2 || grants[0].Decision != "ACCEPTED" || grants[1].Decision != "ACCEPTED" {
		t.Fatal(grants, err)
	}
}

func TestLeaseRenewalUnknownBatchAndCorruptReplay(t *testing.T) {
	pool, id, r, _ := leaseFixture(t)
	ctx := context.Background()
	r.Attempts[0].AttemptID = uuid.NewString()
	r.Attempts[0].JobID = uuid.NewString()
	failures := make(chan error, 8)
	for range 8 {
		go func() {
			g, e := RenewLeases(ctx, pool, id, r)
			if e == nil && (len(g) != 1 || g[0].Decision != "FENCED") {
				e = errors.New("unknown authority accepted")
			}
			failures <- e
		}()
	}
	for range 8 {
		if err := <-failures; err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM worker_lease_requests").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE worker_lease_requests SET results='[]'"); err != nil {
		t.Fatal(err)
	}
	if grants, err := RenewLeases(ctx, pool, id, r); !errors.Is(err, ErrInvalid) || len(grants) != 0 {
		t.Fatal(grants, err)
	}
}

func TestLeaseRenewalOverlappingBatchesAndOriginalDeadline(t *testing.T) {
	pool, id, r, _ := leaseFixture(t)
	ctx := context.Background()
	queueAcquisitionJob(t, pool, nil)
	second, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: r.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || second.Assignment == nil {
		t.Fatal(second, err)
	}
	r.Attempts = append(r.Attempts, second.Assignment.Authority)
	failures := make(chan error, 16)
	for i := range 16 {
		batch := LeaseRenewal{SessionID: r.SessionID, RequestID: uuid.NewString(), Attempts: []AttemptAuthority{r.Attempts[i%2], r.Attempts[(i+1)%2]}}
		go func() {
			g, e := RenewLeases(ctx, pool, id, batch)
			if e == nil && (len(g) != 2 || g[0].Decision != "ACCEPTED" || g[1].Decision != "ACCEPTED") {
				e = errors.New("overlapping batch rejected")
			}
			failures <- e
		}()
	}
	for range 16 {
		if err := <-failures; err != nil {
			t.Fatal(err)
		}
	}
	grants, err := RenewLeases(ctx, pool, id, r)
	if err != nil {
		t.Fatal(err)
	}
	// Model an older accepted grant expiring while a newer grant is still live;
	// moving saved times avoids a 30-second sleep without changing current state.
	if _, err = pool.Exec(ctx, `UPDATE worker_lease_requests SET results=(SELECT jsonb_agg(g || jsonb_build_object('ServerTime',statement_timestamp()-interval '31 seconds','LeaseExpiresAt',statement_timestamp()-interval '1 second')) FROM jsonb_array_elements(results) g) WHERE request_id=$1`, r.RequestID); err != nil {
		t.Fatal(err)
	}
	again, err := RenewLeases(ctx, pool, id, r)
	if err != nil || again[0].Decision != "FENCED" || again[1].Decision != "FENCED" {
		t.Fatal(again, err)
	}
	var actual time.Time
	if err = pool.QueryRow(ctx, "SELECT lease_expires_at FROM attempts WHERE id=$1", r.Attempts[0].AttemptID).Scan(&actual); err != nil || !actual.Equal(grants[0].LeaseExpiresAt) {
		t.Fatal("replay changed current lease", actual, err)
	}
}

func TestLeaseRenewalLosesToObservedSessionTakeover(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	queueAcquisitionJob(t, pool, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	assigned, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || assigned.Assignment == nil {
		t.Fatal(assigned, err)
	}
	r := LeaseRenewal{SessionID: registration.SessionID, RequestID: uuid.NewString(), Attempts: []AttemptAuthority{assigned.Assignment.Authority}}
	next := registration
	next.RequestID = uuid.NewString()
	next.SessionID = uuid.NewString()
	if err = ApproveSessionTakeover(ctx, pool, id.WorkerID, registration.SessionID, next.SessionID); err != nil {
		t.Fatal(err)
	}
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err = blocker.Exec(ctx, "SELECT id FROM jobs FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	waitingPool := func(prefix string) (*pgxpool.Pool, string) {
		cfg := pool.Config()
		app := prefix + strings.ReplaceAll(uuid.NewString(), "-", "")
		cfg.ConnConfig.RuntimeParams["application_name"] = app
		p, e := pgxpool.NewWithConfig(ctx, cfg)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(p.Close)
		return p, app
	}
	observe := func(app string) {
		until := time.Now().Add(time.Second)
		for {
			var blocked bool
			if e := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')", app).Scan(&blocked); e != nil {
				t.Fatal(e)
			}
			if blocked {
				return
			}
			if time.Now().After(until) {
				t.Fatal("operation did not wait on locked job", app)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	takeover, app := waitingPool("takeover_")
	done := make(chan error, 1)
	go func() { _, e := RegisterSession(ctx, takeover, id, next); done <- e }()
	observe(app)
	renewal, app := waitingPool("renew_")
	renewed := make(chan error, 1)
	go func() { _, e := RenewLeases(ctx, renewal, id, r); renewed <- e }()
	observe(app)
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = <-renewed; !errors.Is(err, ErrFenced) {
		t.Fatal("old session retained authority", err)
	}
	if _, err = RenewLeases(ctx, pool, id, r); !errors.Is(err, ErrFenced) {
		t.Fatal(err)
	}
	var state string
	if err = pool.QueryRow(ctx, "SELECT state FROM attempts WHERE id=$1", r.Attempts[0].AttemptID).Scan(&state); err != nil || state != "LOST" {
		t.Fatal(state, err)
	}
}

func TestLeaseRenewalConcurrentReplayAndFreshRequest(t *testing.T) {
	pool, id, request, assigned := leaseFixture(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()+interval '10 seconds'"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan []LeaseGrant, 16)
	failures := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() { defer wg.Done(); r, e := RenewLeases(ctx, pool, id, request); results <- r; failures <- e }()
	}
	wg.Wait()
	close(results)
	close(failures)
	for e := range failures {
		if e != nil {
			t.Fatal(e)
		}
	}
	var expiry time.Time
	for result := range results {
		if len(result) != 1 || result[0].Decision != "ACCEPTED" || result[0].Authority != assigned.Authority {
			t.Fatal(result)
		}
		if expiry.IsZero() {
			expiry = result[0].LeaseExpiresAt
		}
		if !result[0].LeaseExpiresAt.Equal(expiry) || !result[0].PhaseDeadline.Equal(assigned.PhaseDeadline) {
			t.Fatal("replay extended grant", result)
		}
	}
	var count int
	if e := pool.QueryRow(ctx, "SELECT count(*) FROM worker_lease_requests").Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
	old := request
	request.RequestID = uuid.NewString()
	fresh, e := RenewLeases(ctx, pool, id, request)
	if e != nil || !fresh[0].LeaseExpiresAt.After(expiry) {
		t.Fatal(fresh, e)
	}
	replay, e := RenewLeases(ctx, pool, id, old)
	if e != nil || !replay[0].LeaseExpiresAt.Equal(expiry) {
		t.Fatal("old retry borrowed new grant", replay, e)
	}
	if e := RevokeWorkerCredential(ctx, pool, id.WorkerID, id.CredentialID); e != nil {
		t.Fatal(e)
	}
	if _, e := RenewLeases(ctx, pool, id, old); !errors.Is(e, ErrUnauthorized) {
		t.Fatal(e)
	}
}

func TestLeaseRenewalRejectionsAndPayloadConflict(t *testing.T) {
	for _, tc := range []struct{ name, sql, decision string }{
		{"expired", "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second'", "FENCED"},
		{"phase expired", "UPDATE attempts SET phase_deadline=clock_timestamp()-interval '1 second'", "STOP_REQUESTED"},
		{"cancelled", "UPDATE jobs SET cancel_requested=true,state='CANCELLING'", "STOP_REQUESTED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, id, r, _ := leaseFixture(t)
			ctx := context.Background()
			if _, e := RenewLeases(ctx, pool, id, r); e != nil {
				t.Fatal(e)
			}
			if _, e := pool.Exec(ctx, tc.sql); e != nil {
				t.Fatal(e)
			}
			for range 2 {
				result, e := RenewLeases(ctx, pool, id, r)
				if e != nil || result[0].Decision != tc.decision || !result[0].LeaseExpiresAt.IsZero() {
					t.Fatal(result, e)
				}
				r.RequestID = uuid.NewString()
			}
		})
	}
	pool, id, r, _ := leaseFixture(t)
	ctx := context.Background()
	if _, e := RenewLeases(ctx, pool, id, r); e != nil {
		t.Fatal(e)
	}
	r.Attempts[0].Generation++
	if _, e := RenewLeases(ctx, pool, id, r); !errors.Is(e, ErrConflict) {
		t.Fatal("changed request accepted", e)
	}
	r.RequestID = uuid.NewString()
	result, e := RenewLeases(ctx, pool, id, r)
	if e != nil || result[0].Decision != "FENCED" {
		t.Fatal(result, e)
	}
	r.Attempts = append(r.Attempts, r.Attempts[0])
	if _, e := RenewLeases(ctx, pool, id, r); !errors.Is(e, ErrInvalid) {
		t.Fatal("duplicate tuple accepted", e)
	}
}

func TestLeaseRenewalBatchHasIndependentDecisionsAndStableOrder(t *testing.T) {
	pool, id, r, _ := leaseFixture(t)
	ctx := context.Background()
	queueAcquisitionJob(t, pool, nil)
	second, e := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: r.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if e != nil || second.Assignment == nil {
		t.Fatal(second, e)
	}
	r.Attempts = append(r.Attempts, second.Assignment.Authority)
	if _, e := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", r.Attempts[0].AttemptID); e != nil {
		t.Fatal(e)
	}
	grants, e := RenewLeases(ctx, pool, id, r)
	if e != nil || len(grants) != 2 || grants[0].Decision != "FENCED" || grants[1].Decision != "ACCEPTED" {
		t.Fatal(grants, e)
	}
	r.Attempts[0], r.Attempts[1] = r.Attempts[1], r.Attempts[0]
	again, e := RenewLeases(ctx, pool, id, r)
	if e != nil || again[0].Authority != r.Attempts[0] || again[0].Decision != "ACCEPTED" || !again[0].LeaseExpiresAt.Equal(grants[1].LeaseExpiresAt) || again[1].Decision != "FENCED" {
		t.Fatal(again, e)
	}
}

func TestLeaseRenewalUsesFreshTimeAfterObservedJobLock(t *testing.T) {
	pool, id, r, _ := leaseFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blocker, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer rollback(blocker)
	if _, e := blocker.Exec(ctx, "SELECT id FROM jobs FOR UPDATE"); e != nil {
		t.Fatal(e)
	}
	cfg := pool.Config()
	app := "renew_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	waiting, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer waiting.Close()
	type outcome struct {
		grants []LeaseGrant
		err    error
	}
	done := make(chan outcome, 1)
	go func() { g, e := RenewLeases(ctx, waiting, id, r); done <- outcome{g, e} }()
	until := time.Now().Add(time.Second)
	for {
		var blocked bool
		if e := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'SELECT id%')", app).Scan(&blocked); e != nil {
			t.Fatal(e)
		}
		if blocked {
			break
		}
		if time.Now().After(until) {
			t.Fatal("renewal did not wait on the job lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, e := blocker.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()"); e != nil {
		t.Fatal(e)
	}
	if e := blocker.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	result := <-done
	if result.err != nil || result.grants[0].Decision != "FENCED" {
		t.Fatal("renewal resurrected expiry", result)
	}
}
