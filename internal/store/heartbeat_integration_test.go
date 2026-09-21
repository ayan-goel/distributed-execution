//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestHeartbeatReadinessHealthDrainAndOrderedReplay(t *testing.T) {
	pool, id, r := sessionFixture(t)
	ctx := context.Background()
	if _, err := RegisterSession(ctx, pool, id, r); err != nil {
		t.Fatal(err)
	}
	h := HeartbeatReport{RequestID: uuid.NewString(), SessionID: r.SessionID, Sequence: 1, RuntimeHealthy: true}
	result, err := RecordHeartbeat(ctx, pool, id, h)
	if err != nil || !result.Reconcile {
		t.Fatal("unreconciled worker became ready", err)
	}
	h.Sequence++
	h.RequestID = uuid.NewString()
	h.ReconciliationComplete = true
	result, err = RecordHeartbeat(ctx, pool, id, h)
	if err != nil || result.Reconcile || result.Drain {
		t.Fatal("clean worker not ready", err)
	}
	var before, after time.Time
	if err := pool.QueryRow(ctx, "SELECT last_heartbeat_at FROM workers WHERE id=$1", id.WorkerID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordHeartbeat(ctx, pool, id, h); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT last_heartbeat_at FROM workers WHERE id=$1", id.WorkerID).Scan(&after); err != nil || !before.Equal(after) {
		t.Fatal("duplicate report refreshed liveness", err)
	}
	old := h
	h.Sequence++
	h.RequestID = uuid.NewString()
	h.RuntimeHealthy = false
	if _, err := RecordHeartbeat(ctx, pool, id, h); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordHeartbeat(ctx, pool, id, old); !errors.Is(err, ErrStaleHeartbeat) {
		t.Fatal("old healthy report overwrote unhealthy state", err)
	}
	changed := h
	changed.RuntimeHealthy = true
	if _, err := RecordHeartbeat(ctx, pool, id, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("changed sequence replay accepted", err)
	}
	var state string
	if err := pool.QueryRow(ctx, "SELECT state FROM workers WHERE id=$1", id.WorkerID).Scan(&state); err != nil || state != "QUARANTINED" {
		t.Fatal("unhealthy worker remained eligible", state, err)
	}
	h.Sequence++
	h.RequestID = uuid.NewString()
	h.RuntimeHealthy = true
	h.DiskPressure = true
	if result, err := RecordHeartbeat(ctx, pool, id, h); err != nil || !result.Drain {
		t.Fatal("disk-pressure worker allowed acquisition", err)
	}
	h.Sequence++
	h.RequestID = uuid.NewString()
	h.DiskPressure = false
	if _, err := pool.Exec(ctx, "UPDATE workers SET drain_requested=true WHERE id=$1", id.WorkerID); err != nil {
		t.Fatal(err)
	}
	if result, err := RecordHeartbeat(ctx, pool, id, h); err != nil || !result.Drain {
		t.Fatal("operator drain ignored", err)
	}
}

func TestHeartbeatQuarantineRequiresOldInventoryCleanup(t *testing.T) {
	pool, id, r := sessionFixture(t)
	ctx := context.Background()
	job, attempt := assignedForRecovery(t, pool, id, r, nil)
	next := r
	next.RequestID = uuid.NewString()
	next.SessionID = uuid.NewString()
	if err := ApproveSessionTakeover(ctx, pool, id.WorkerID, r.SessionID, next.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSession(ctx, pool, id, next); err != nil {
		t.Fatal(err)
	}
	a := AttemptAuthority{JobID: job, AttemptID: attempt, Generation: 1, WorkerID: id.WorkerID, SessionID: r.SessionID}
	h := HeartbeatReport{RequestID: uuid.NewString(), SessionID: next.SessionID, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true, Inventory: []ExecutionRecord{{Authority: a, ContainerID: strings.Repeat("a", 64), Running: true}}}
	result, err := RecordHeartbeat(ctx, pool, id, h)
	if err != nil || !result.Reconcile || len(result.Stop) != 1 || result.Stop[0] != a {
		t.Fatal("old execution did not prevent readiness", err)
	}
	var state string
	if err := pool.QueryRow(ctx, "SELECT state FROM reservations WHERE attempt_id=$1", attempt).Scan(&state); err != nil || state != "quarantined" {
		t.Fatal("uncertain capacity was released", err)
	}
	h.Sequence++
	h.RequestID = uuid.NewString()
	h.Inventory = nil
	result, err = RecordHeartbeat(ctx, pool, id, h)
	if err != nil || result.Reconcile || result.Drain {
		t.Fatal("confirmed cleanup did not enable worker", err)
	}
	var cleanup bool
	if err := pool.QueryRow(ctx, "SELECT r.state,a.cleanup_pending FROM reservations r JOIN attempts a ON a.id=r.attempt_id WHERE a.id=$1", attempt).Scan(&state, &cleanup); err != nil || state != "released" || cleanup {
		t.Fatal("confirmed cleanup did not release old capacity", err)
	}
	h.SessionID = r.SessionID
	if _, err := RecordHeartbeat(ctx, pool, id, h); !errors.Is(err, ErrFenced) {
		t.Fatal("old incarnation heartbeated", err)
	}
}

func TestHeartbeatDoesNotInferCompletionFromMissingInventory(t *testing.T) {
	pool, id, r := sessionFixture(t)
	ctx := context.Background()
	job, attempt := assignedForRecovery(t, pool, id, r, nil)
	var originalLease, timeAfterHeartbeat time.Time
	if err := pool.QueryRow(ctx, "SELECT lease_expires_at FROM attempts WHERE id=$1", attempt).Scan(&originalLease); err != nil {
		t.Fatal(err)
	}
	h := HeartbeatReport{RequestID: uuid.NewString(), SessionID: r.SessionID, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}
	if _, err := RecordHeartbeat(ctx, pool, id, h); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT lease_expires_at FROM attempts WHERE id=$1", attempt).Scan(&timeAfterHeartbeat); err != nil || !originalLease.Equal(timeAfterHeartbeat) {
		t.Fatal("heartbeat extended execution authority", err)
	}
	var state, current, reservation string
	if err := pool.QueryRow(ctx, "SELECT j.state,j.current_attempt_id::text,r.state FROM jobs j JOIN reservations r ON r.attempt_id=j.current_attempt_id WHERE j.id=$1", job).Scan(&state, &current, &reservation); err != nil || state != "ACTIVE" || current != attempt || reservation != "active" {
		t.Fatal("inventory absence changed authority", err)
	}
	a := AttemptAuthority{JobID: job, AttemptID: attempt, Generation: 1, WorkerID: id.WorkerID, SessionID: r.SessionID}
	h.Sequence++
	h.RequestID = uuid.NewString()
	h.Inventory = []ExecutionRecord{{Authority: a, ContainerID: strings.Repeat("a", 64), Running: true}}
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", attempt); err != nil {
		t.Fatal(err)
	}
	if result, err := RecordHeartbeat(ctx, pool, id, h); err != nil || len(result.Stop) != 1 || !result.Reconcile {
		t.Fatal("expired execution did not get stop instruction", err)
	}
}

func TestHeartbeatCannotReleaseItsOwnUncertainReservation(t *testing.T) {
	pool, id, r := sessionFixture(t)
	ctx := context.Background()
	job, attempt := assignedForRecovery(t, pool, id, r, nil)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, "UPDATE attempts SET state='LOST',cleanup_pending=true WHERE id=$1", attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "UPDATE reservations SET state='quarantined' WHERE attempt_id=$1", attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "UPDATE jobs SET state='RETRY_WAIT',current_attempt_id=NULL WHERE id=$1", job); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	h := HeartbeatReport{RequestID: uuid.NewString(), SessionID: r.SessionID, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}
	if result, err := RecordHeartbeat(ctx, pool, id, h); err != nil || !result.Reconcile || !result.Drain {
		t.Fatal("same-session snapshot freed uncertain capacity", err)
	}
	var state string
	if err := pool.QueryRow(ctx, "SELECT state FROM reservations WHERE attempt_id=$1", attempt).Scan(&state); err != nil || state != "quarantined" {
		t.Fatal("same-session reservation released", err)
	}
}

func TestHeartbeatCleanupAndSequenceRollbackTogether(t *testing.T) {
	pool, id, r := sessionFixture(t)
	ctx := context.Background()
	_, attempt := assignedForRecovery(t, pool, id, r, nil)
	next := r
	next.RequestID = uuid.NewString()
	next.SessionID = uuid.NewString()
	if err := ApproveSessionTakeover(ctx, pool, id.WorkerID, r.SessionID, next.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSession(ctx, pool, id, next); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_heartbeat_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected state event failure'; END $$;
	CREATE TRIGGER reject_heartbeat_event BEFORE INSERT ON worker_audit_events FOR EACH ROW EXECUTE FUNCTION reject_heartbeat_event()`); err != nil {
		t.Fatal(err)
	}
	h := HeartbeatReport{RequestID: uuid.NewString(), SessionID: next.SessionID, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}
	if _, err := RecordHeartbeat(ctx, pool, id, h); err == nil {
		t.Fatal("state audit failure ignored")
	}
	var state string
	var seq int64
	if err := pool.QueryRow(ctx, "SELECT state FROM reservations WHERE attempt_id=$1", attempt).Scan(&state); err != nil || state != "quarantined" {
		t.Fatal("failed heartbeat released capacity", err)
	}
	if err := pool.QueryRow(ctx, "SELECT heartbeat_sequence FROM worker_sessions WHERE id=$1", next.SessionID).Scan(&seq); err != nil || seq != 0 {
		t.Fatal("failed heartbeat consumed sequence", err)
	}
}
