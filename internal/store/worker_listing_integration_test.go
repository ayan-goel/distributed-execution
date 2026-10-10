//go:build integration

package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestWorkerListingScopesAndAccountsForQuarantinedCapacity(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	var project string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM projects WHERE name='research'").Scan(&project); err != nil {
		t.Fatal(err)
	}
	page, err := ListWorkers(ctx, pool, project, "", 1)
	if err != nil || len(page.Workers) != 1 || page.Workers[0].ID != worker.WorkerID || page.HasMore || page.AsOf.IsZero() {
		t.Fatal(page, err)
	}
	initial := page.Workers[0]
	if initial.State != "READY" || initial.LastHeartbeatAt == nil || !initial.RuntimeHealthy || !initial.ReconciliationComplete || initial.DiskPressure || initial.Reserved.Slots != 0 || initial.Available != initial.Capacity {
		t.Fatal(initial)
	}
	queueAcquisitionJob(t, pool, nil)
	assigned, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || assigned.Assignment == nil {
		t.Fatal(assigned, err)
	}
	page, err = ListWorkers(ctx, pool, project, "", 100)
	if err != nil || page.Workers[0].Reserved.CPUMillis != assigned.Assignment.Job.Spec.Resources.CPUMillis {
		t.Fatal(page, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", assigned.Assignment.Authority.AttemptID); err != nil {
		t.Fatal(err)
	}
	if count, err := ReapExpiredAttempts(ctx, pool, 64); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	page, err = ListWorkers(ctx, pool, project, "", 100)
	if err != nil || page.Workers[0].Reserved.Slots != 1 || page.Workers[0].Available.Slots != initial.Capacity.Slots-1 {
		t.Fatal("quarantine appeared free", page, err)
	}
	// Lower advertised capacity can coexist with older quarantined reservations.
	// Available headroom is zero in that dimension, while reserved stays visible.
	if _, err := pool.Exec(ctx, "UPDATE workers SET cpu_millis=1 WHERE id=$1", worker.WorkerID); err != nil {
		t.Fatal(err)
	}
	page, err = ListWorkers(ctx, pool, project, "", 100)
	if err != nil || page.Workers[0].Available.CPUMillis != 0 || page.Workers[0].Reserved.CPUMillis <= page.Workers[0].Capacity.CPUMillis {
		t.Fatal(page, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE reservations SET state='released' WHERE attempt_id=$1", assigned.Assignment.Authority.AttemptID); err != nil {
		t.Fatal(err)
	}
	page, err = ListWorkers(ctx, pool, project, "", 100)
	if err != nil || page.Workers[0].Reserved.Slots != 0 || page.Workers[0].Available != page.Workers[0].Capacity {
		t.Fatal("released capacity stayed reserved", page, err)
	}
	foreign, err := ListWorkers(ctx, pool, uuid.NewString(), "", 100)
	if err != nil || foreign.Workers == nil || len(foreign.Workers) != 0 || foreign.HasMore {
		t.Fatal("foreign worker disclosed", foreign, err)
	}
	for _, tc := range []struct {
		project, after string
		limit          int
	}{{"invalid", "", 1}, {project, "invalid", 1}, {project, "", 0}, {project, "", 101}} {
		if _, err := ListWorkers(ctx, nil, tc.project, tc.after, tc.limit); !errors.Is(err, ErrInvalid) {
			t.Fatal(tc, err)
		}
	}
}

func TestWorkerListingBoundedPagesSurviveRemovedMembership(t *testing.T) {
	pool, _, _ := readyAcquisitionWorker(t)
	ctx := context.Background()
	var project string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM projects WHERE name='research'").Scan(&project); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 15; i++ {
		p := workerProvision()
		p.Name = fmt.Sprintf("large-worker-%02d", i)
		p.CertificateSHA256 = sha256.Sum256([]byte(p.Name))
		for label := 0; label < 62; label++ {
			p.Labels[fmt.Sprintf("large-%02d", label)] = strings.Repeat("<", 256)
		}
		if _, err := ProvisionWorker(ctx, pool, p); err != nil {
			t.Fatal(err)
		}
	}
	page, err := ListWorkers(ctx, pool, project, "", 100)
	if err != nil || !page.HasMore || len(page.Workers) == 0 || len(page.Workers) >= 16 || page.NextID == "" {
		t.Fatal(page, err)
	}
	seen := map[string]bool{}
	position := ""
	for {
		encoded, err := json.Marshal(page)
		if err != nil || len(encoded) > MaxWorkerPageBytes {
			t.Fatal("page exceeds encoded bound", len(encoded), err)
		}
		for _, worker := range page.Workers {
			if worker.ID <= position || seen[worker.ID] {
				t.Fatal("worker skipped ordering", worker.ID, position)
			}
			if worker.State == "REGISTERING" && worker.LastHeartbeatAt != nil {
				t.Fatal("invented heartbeat", worker)
			}
			seen[worker.ID] = true
			position = worker.ID
		}
		if !page.HasMore {
			if page.NextID != "" {
				t.Fatal("terminal cursor")
			}
			break
		}
		if page.NextID != position {
			t.Fatal("cursor skipped unreturned worker")
		}
		// Removal of the previous anchor cannot strand the remaining live page.
		if _, err := pool.Exec(ctx, "DELETE FROM worker_projects WHERE project_id=$1 AND worker_id=$2", project, position); err != nil {
			t.Fatal(err)
		}
		page, err = ListWorkers(ctx, pool, project, position, 100)
		if err != nil || len(page.Workers) == 0 {
			t.Fatal(page, err)
		}
	}
	if len(seen) != 16 {
		t.Fatal("incomplete traversal", len(seen))
	}
}

func TestWorkerListingSharedHostReportsGlobalReservations(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES('other',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	var other string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM projects WHERE name='other'").Scan(&other); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO worker_projects(worker_id,project_id) VALUES($1,$2)", worker.WorkerID, other); err != nil {
		t.Fatal(err)
	}
	queueAcquisitionJob(t, pool, nil)
	assigned, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || assigned.Assignment == nil {
		t.Fatal(assigned, err)
	}
	page, err := ListWorkers(ctx, pool, other, "", 1)
	if err != nil || len(page.Workers) != 1 || page.Workers[0].Reserved.Slots != 1 {
		t.Fatal("other project reservation hidden", page, err)
	}
}
