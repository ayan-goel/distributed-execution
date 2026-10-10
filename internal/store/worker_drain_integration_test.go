//go:build integration

package store

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestWorkerDrainPreservesExistingAuthorityAndAuditsOnce(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	job := queueAcquisitionJob(t, pool, nil)
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	assigned, err := AcquireWork(ctx, pool, worker, request, AcquisitionPolicy{})
	if err != nil || assigned.Assignment == nil {
		t.Fatal(assigned, err)
	}
	token, _, err := IssueToken(ctx, pool, "research", RoleOperator)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Authenticate(ctx, pool, token)
	if err != nil {
		t.Fatal(err)
	}
	queueAcquisitionJob(t, pool, nil)
	var wg sync.WaitGroup
	failures := make(chan error, 12)
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := RequestWorkerDrain(ctx, pool, p, worker.WorkerID)
			if err == nil && (!result.DrainRequested || result.WorkerID != worker.WorkerID || result.State != "DRAINING") {
				err = errors.New("incorrect drain result")
			}
			failures <- err
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if result, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{}); err != nil || result.Assignment != nil || result.NoWorkReason != "WORKER_NOT_READY" {
		t.Fatal("drained worker acquired new work", result, err)
	}
	if replay, err := AcquireWork(ctx, pool, worker, request, AcquisitionPolicy{}); err != nil || replay.Assignment == nil || replay.Assignment.Authority != assigned.Assignment.Authority {
		t.Fatal("drain revoked existing assignment", replay, err)
	}
	grants, err := RenewLeases(ctx, pool, worker, LeaseRenewal{SessionID: registration.SessionID, RequestID: uuid.NewString(), Attempts: []AttemptAuthority{assigned.Assignment.Authority}})
	if err != nil || len(grants) != 1 || grants[0].Decision != "ACCEPTED" {
		t.Fatal("drain prevented existing renewal", grants, err)
	}
	heartbeat, err := RecordHeartbeat(ctx, pool, worker, HeartbeatReport{RequestID: uuid.NewString(), SessionID: registration.SessionID, Sequence: 2, RuntimeHealthy: true, ReconciliationComplete: true})
	if err != nil || !heartbeat.Drain || len(heartbeat.Stop) != 0 {
		t.Fatal("drain stopped existing attempt", heartbeat, err)
	}
	var state, reservation string
	var events, audits int
	if err := pool.QueryRow(ctx, `SELECT j.state,r.state,(SELECT count(*) FROM worker_audit_events WHERE worker_id=$2 AND action='WORKER_DRAIN_REQUESTED'),(SELECT count(*) FROM audit_events WHERE token_id=$3 AND action=$4) FROM jobs j JOIN reservations r ON r.attempt_id=j.current_attempt_id WHERE j.id=$1`, job.ID, worker.WorkerID, p.TokenID, "WORKER_DRAIN_REQUESTED:"+worker.WorkerID).Scan(&state, &reservation, &events, &audits); err != nil || state != "ACTIVE" || reservation != "active" || events != 1 || audits != 1 {
		t.Fatal(state, reservation, events, audits, err)
	}
	for _, unhealthy := range []string{"REGISTERING", "SUSPECT", "OFFLINE", "QUARANTINED"} {
		if _, err := pool.Exec(ctx, "UPDATE workers SET state=$2,drain_requested=false WHERE id=$1", worker.WorkerID, unhealthy); err != nil {
			t.Fatal(err)
		}
		if result, err := RequestWorkerDrain(ctx, pool, p, worker.WorkerID); err != nil || result.State != unhealthy {
			t.Fatal("drain erased health evidence", result, err)
		}
	}
	if err := RevokeToken(ctx, pool, "research", p.TokenID); err != nil {
		t.Fatal(err)
	}
	if _, err := RequestWorkerDrain(ctx, pool, p, worker.WorkerID); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked principal mutated drain", err)
	}
	if _, err := RequestWorkerDrain(ctx, pool, Principal{Role: RoleRead}, worker.WorkerID); !errors.Is(err, ErrUnauthorized) {
		t.Fatal(err)
	}
}

func TestWorkerDrainScopeAndAuditFailureRollback(t *testing.T) {
	pool, worker, _ := readyAcquisitionWorker(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES('other',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	principal := func(project string) Principal {
		t.Helper()
		token, _, err := IssueToken(ctx, pool, project, RoleOperator)
		if err != nil {
			t.Fatal(err)
		}
		p, err := Authenticate(ctx, pool, token)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	p, foreign := principal("research"), principal("other")
	if _, err := RequestWorkerDrain(ctx, pool, foreign, worker.WorkerID); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign worker drain accepted", err)
	}
	if _, err := RequestWorkerDrain(ctx, pool, p, uuid.NewString()); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing worker accepted", err)
	}
	for _, table := range []string{"worker_audit_events", "audit_events"} {
		if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_drain_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END $$;
			CREATE TRIGGER reject_drain_audit BEFORE INSERT ON `+table+` FOR EACH ROW EXECUTE FUNCTION reject_drain_audit()`); err != nil {
			t.Fatal(err)
		}
		if _, err := RequestWorkerDrain(ctx, pool, p, worker.WorkerID); err == nil {
			t.Fatal("drain committed without audit")
		}
		var state string
		var drain bool
		var audits int
		if err := pool.QueryRow(ctx, `SELECT state,drain_requested,(SELECT count(*) FROM worker_audit_events WHERE action='WORKER_DRAIN_REQUESTED')+(SELECT count(*) FROM audit_events WHERE action LIKE 'WORKER_DRAIN_REQUESTED:%') FROM workers WHERE id=$1`, worker.WorkerID).Scan(&state, &drain, &audits); err != nil || drain || state != "READY" || audits != 0 {
			t.Fatal("partial drain survived rollback", state, drain, audits, err)
		}
		if _, err := pool.Exec(ctx, "DROP TRIGGER reject_drain_audit ON "+table+"; DROP FUNCTION reject_drain_audit()"); err != nil {
			t.Fatal(err)
		}
	}
	// Project membership grants operator maintenance access to a shared host.
	// The flag is host-wide; it must not create project-specific schedulability.
	if _, err := pool.Exec(ctx, "INSERT INTO worker_projects(worker_id,project_id) VALUES($1,$2)", worker.WorkerID, foreign.ProjectID); err != nil {
		t.Fatal(err)
	}
	if result, err := RequestWorkerDrain(ctx, pool, foreign, worker.WorkerID); err != nil || !result.DrainRequested {
		t.Fatal(result, err)
	}
}
