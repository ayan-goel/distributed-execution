//go:build integration

package store

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func sessionFixture(t *testing.T) (*pgxpool.Pool, WorkerIdentity, Registration) {
	t.Helper()
	ctx := context.Background()
	pool := testPool(t)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES('research',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	p := workerProvision()
	identity, err := ProvisionWorker(ctx, pool, p)
	if err != nil {
		t.Fatal(err)
	}
	r := Registration{RequestID: uuid.NewString(), SessionID: uuid.NewString(), ProtocolVersion: 1, Resources: p.Resources, Slots: p.Slots, Labels: p.Labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.soft"}}
	return pool, identity, r
}

func TestConcurrentSessionRegistrationAndReplay(t *testing.T) {
	pool, identity, r := sessionFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := RegisterSession(ctx, pool, identity, r)
			if err == nil && (s.SessionID != r.SessionID || s.Generation != 1 || !s.CleanupRequired) {
				err = errors.New("incorrect registration response")
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"worker_sessions", "worker_registrations"} {
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 1 {
			t.Fatal("duplicate session state", table, count, err)
		}
	}
	var state string
	if err := pool.QueryRow(ctx, "SELECT state FROM workers WHERE id=$1", identity.WorkerID).Scan(&state); err != nil || state != "REGISTERING" {
		t.Fatal("unreconciled worker became eligible", err)
	}
	changed := r
	changed.Slots--
	if _, err := RegisterSession(ctx, pool, identity, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("changed replay accepted", err)
	}
	r.RequestID = uuid.NewString()
	if s, err := RegisterSession(ctx, pool, identity, r); err != nil || s.SessionID != r.SessionID {
		t.Fatal("same incarnation with new request created authority", err)
	}
	r.SessionID = uuid.NewString()
	r.RequestID = uuid.NewString()
	if _, err := RegisterSession(ctx, pool, identity, r); !errors.Is(err, ErrSessionActive) {
		t.Fatal("concurrent incarnation accepted", err)
	}
}

func TestRegistrationEnforcesPolicyAndCurrentCredentials(t *testing.T) {
	pool, identity, original := sessionFixture(t)
	ctx := context.Background()
	for _, change := range []func(*Registration){
		func(r *Registration) { r.Resources.CPUMillis++ },
		func(r *Registration) { r.Slots++ },
		func(r *Registration) { r.Labels = map[string]string{"os": "linux", "architecture": "amd64"} },
		func(r *Registration) { r.ProtocolVersion = 2 },
		func(r *Registration) { r.Capabilities = []string{"docker.v1", "gpu"} },
	} {
		r := original
		change(&r)
		if _, err := RegisterSession(ctx, pool, identity, r); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid worker claim accepted", err)
		}
	}
	if err := RevokeWorkerCredential(ctx, pool, identity.WorkerID, identity.CredentialID); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSession(ctx, pool, identity, original); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("cached identity bypassed revocation", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM worker_sessions").Scan(&count); err != nil || count != 0 {
		t.Fatal("invalid registration persisted", err)
	}
}

func TestRacingIncarnationsAndRegistrationRollback(t *testing.T) {
	pool, identity, r := sessionFixture(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_session_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END $$;
	CREATE TRIGGER reject_session_audit BEFORE INSERT ON worker_audit_events FOR EACH ROW EXECUTE FUNCTION reject_session_audit()`); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSession(ctx, pool, identity, r); err == nil {
		t.Fatal("injected failure ignored")
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM worker_sessions").Scan(&count); err != nil || count != 0 {
		t.Fatal("session survived audit failure", err)
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER reject_session_audit ON worker_audit_events"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		next := r
		next.RequestID = uuid.NewString()
		next.SessionID = uuid.NewString()
		wg.Add(1)
		go func() { defer wg.Done(); _, err := RegisterSession(ctx, pool, identity, next); errs <- err }()
	}
	wg.Wait()
	close(errs)
	success, conflict := 0, 0
	for err := range errs {
		if err == nil {
			success++
		} else if errors.Is(err, ErrSessionActive) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal("competing registrations did not serialize", success, conflict)
	}
}
