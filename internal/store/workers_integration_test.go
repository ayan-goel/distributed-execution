//go:build integration

package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
)

func TestWorkerProvisioningAuthenticationAndRevocation(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES ('research',4000,8192,4),('other',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	p := workerProvision()
	identity, err := ProvisionWorker(ctx, pool, p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := AuthenticateWorker(ctx, pool, p.CertificateSHA256)
	if err != nil || got != identity {
		t.Fatal("wrong worker identity", err)
	}
	var state, project string
	var cpu int64
	if err := pool.QueryRow(ctx, `SELECT w.state,a.cpu_limit,p.name FROM workers w JOIN worker_authorizations a ON a.worker_id=w.id JOIN worker_projects wp ON wp.worker_id=w.id JOIN projects p ON p.id=wp.project_id WHERE w.id=$1`, identity.WorkerID).Scan(&state, &cpu, &project); err != nil || state != "REGISTERING" || cpu != 4000 || project != "research" {
		t.Fatal("incorrect host authorization", err)
	}
	if _, err := AuthenticateWorker(ctx, pool, sha256.Sum256([]byte("unprovisioned"))); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("unknown certificate accepted", err)
	}
	p.Name = "linux-b"
	p.CertificateSHA256 = sha256.Sum256([]byte("another certificate"))
	other, err := ProvisionWorker(ctx, pool, p)
	if err != nil {
		t.Fatal(err)
	}
	if err := RevokeWorkerCredential(ctx, pool, other.WorkerID, identity.CredentialID); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-host revocation accepted", err)
	}
	for range 2 {
		if err := RevokeWorkerCredential(ctx, pool, identity.WorkerID, identity.CredentialID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := AuthenticateWorker(ctx, pool, workerProvision().CertificateSHA256); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked certificate accepted", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM worker_audit_events WHERE worker_id=$1", identity.WorkerID).Scan(&count); err != nil || count != 2 {
		t.Fatal("expected one provisioning and one revocation event", count, err)
	}
}

func TestWorkerProvisioningAndRevocationRollbackWithAuditFailure(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES ('research',4000,8192,4);
	CREATE FUNCTION reject_worker_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END $$;
	CREATE TRIGGER reject_worker_audit BEFORE INSERT ON worker_audit_events FOR EACH ROW EXECUTE FUNCTION reject_worker_audit()`); err != nil {
		t.Fatal(err)
	}
	p := workerProvision()
	if _, err := ProvisionWorker(ctx, pool, p); err == nil {
		t.Fatal("injected failure ignored")
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM workers").Scan(&count); err != nil || count != 0 {
		t.Fatal("worker survived failed audit", err)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE worker_audit_events DISABLE TRIGGER reject_worker_audit"); err != nil {
		t.Fatal(err)
	}
	identity, err := ProvisionWorker(ctx, pool, p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE worker_audit_events ENABLE TRIGGER reject_worker_audit"); err != nil {
		t.Fatal(err)
	}
	if err := RevokeWorkerCredential(ctx, pool, identity.WorkerID, identity.CredentialID); err == nil {
		t.Fatal("revocation succeeded without audit")
	}
	if _, err := AuthenticateWorker(ctx, pool, p.CertificateSHA256); err != nil {
		t.Fatal("failed revocation was not rolled back", err)
	}
}

func TestWorkerProvisioningIsAtomicAndConcurrentDuplicatesDoNotCreateHosts(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES ('research',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	p := workerProvision()
	p.Projects = append(p.Projects, "missing")
	if _, err := ProvisionWorker(ctx, pool, p); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing project accepted", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM workers").Scan(&count); err != nil || count != 0 {
		t.Fatal("failed provisioning left host", err)
	}
	p = workerProvision()
	results := make(chan error, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := ProvisionWorker(ctx, pool, p); results <- err }()
	}
	wg.Wait()
	close(results)
	succeeded := 0
	for err := range results {
		if err == nil {
			succeeded++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if succeeded != 1 {
		t.Fatal("duplicate certificate provisioned", succeeded)
	}
	for _, table := range []string{"workers", "worker_authorizations", "worker_credentials", "worker_projects", "worker_audit_events"} {
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 1 {
			t.Fatal("provisioning is not atomic", table, count, err)
		}
	}
}
