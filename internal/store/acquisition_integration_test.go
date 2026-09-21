//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func readyAcquisitionWorker(t *testing.T) (*pgxpool.Pool, WorkerIdentity, Registration) {
	t.Helper()
	pool, id, registration := sessionFixture(t)
	registration.Capabilities[len(registration.Capabilities)-1] = "scratch.quota"
	ctx := context.Background()
	if _, err := RegisterSession(ctx, pool, id, registration); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordHeartbeat(ctx, pool, id, HeartbeatReport{RequestID: uuid.NewString(), SessionID: registration.SessionID, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}); err != nil {
		t.Fatal(err)
	}
	return pool, id, registration
}

func queueAcquisitionJob(t *testing.T, pool *pgxpool.Pool, change func(*spec.Job)) JobRecord {
	t.Helper()
	job, _ := admittedExample(t)
	job.Spec.Placement.Labels["architecture"] = "arm64"
	if change != nil {
		change(&job)
	}
	_, hash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	record, err := SubmitJob(context.Background(), pool, uuid.NewString(), hash, job)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestAcquisitionConcurrentReplayAndCapacity(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	first := queueAcquisitionJob(t, pool, nil)
	for range 5 {
		queueAcquisitionJob(t, pool, nil)
	}
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	results := make(chan AcquisitionResult, 16)
	failures := make(chan error, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{})
			results <- result
			failures <- err
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	attempt := ""
	for result := range results {
		if result.Assignment == nil || result.Assignment.Authority.JobID != first.ID {
			t.Fatal("wrong queue assignment", result)
		}
		if attempt == "" {
			attempt = result.Assignment.Authority.AttemptID
		}
		if attempt != result.Assignment.Authority.AttemptID {
			t.Fatal("replay allocated another attempt")
		}
	}
	failures = make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
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
	var count int
	var cpu, memory, scratch int64
	if err := pool.QueryRow(ctx, "SELECT count(*),sum(cpu_millis),sum(memory_mib),sum(scratch_mib) FROM reservations WHERE state='active'").Scan(&count, &cpu, &memory, &scratch); err != nil {
		t.Fatal(err)
	}
	if count != 2 || cpu != 4000 || memory != 8192 || scratch != 16384 {
		t.Fatal("concurrent acquisition overbooked capacity", count, cpu, memory, scratch)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_events WHERE type='ASSIGNED'").Scan(&count); err != nil || count != 2 {
		t.Fatal("assignment events not atomic", count, err)
	}
}

func TestAcquisitionNoWorkReplayDoesNotConsumeLaterJob(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	result, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{})
	if err != nil || result.NoWorkReason != "QUEUE_EMPTY" {
		t.Fatal(result, err)
	}
	queueAcquisitionJob(t, pool, nil)
	if result, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{}); err != nil || result.NoWorkReason != "QUEUE_EMPTY" {
		t.Fatal("no-work replay changed", result, err)
	}
	request.RequestID = uuid.NewString()
	if result, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{}); err != nil || result.Assignment == nil {
		t.Fatal("fresh request missed queued job", result, err)
	}
}

func TestAcquisitionReplayNeverRenewsOrRevivesAuthority(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	job := queueAcquisitionJob(t, pool, nil)
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	first, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{})
	if err != nil || first.Assignment == nil {
		t.Fatal(first, err)
	}
	replay, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{})
	if err != nil || replay.Assignment == nil || !replay.Assignment.LeaseExpiresAt.Equal(first.Assignment.LeaseExpiresAt) || replay.Assignment.ServerTime.Before(first.Assignment.ServerTime) {
		t.Fatal("replay renewed lease", replay, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1", job.ID); err != nil {
		t.Fatal(err)
	}
	if result, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{}); err != nil || result.Decision != "FENCED" || result.Assignment != nil {
		t.Fatal("expired replay granted authority", result, err)
	}
	if err := RevokeWorkerCredential(ctx, pool, id.WorkerID, id.CredentialID); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{}); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked credential replay accepted", err)
	}
}

func TestAcquisitionEligibilityAndRollback(t *testing.T) {
	for _, test := range []struct {
		name, sql, reason string
		policy            AcquisitionPolicy
	}{
		{name: "draining", sql: "UPDATE workers SET drain_requested=true", reason: "WORKER_NOT_READY"},
		{name: "stale heartbeat", sql: "UPDATE workers SET last_heartbeat_at=clock_timestamp()-interval '16 seconds'", reason: "WORKER_NOT_READY"},
		{name: "project slots", sql: "UPDATE projects SET concurrency_quota=1", reason: "PROJECT_QUOTA"},
		{name: "placement", sql: "UPDATE workers SET labels='{}'", reason: "PLACEMENT_MISMATCH"},
		{name: "strict scratch", sql: `UPDATE workers SET capabilities=(capabilities-'scratch.quota') || '{"scratch.soft":true}'`, reason: "PLACEMENT_MISMATCH"},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool, id, registration := readyAcquisitionWorker(t)
			ctx := context.Background()
			queueAcquisitionJob(t, pool, nil)
			if test.name == "project slots" {
				if result, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{}); err != nil || result.Assignment == nil {
					t.Fatal(result, err)
				}
				queueAcquisitionJob(t, pool, nil)
			}
			if _, err := pool.Exec(ctx, test.sql); err != nil {
				t.Fatal(err)
			}
			result, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, test.policy)
			if err != nil || result.NoWorkReason != test.reason {
				t.Fatal("eligibility gate bypassed", result, err)
			}
		})
	}
	t.Run("audit rollback", func(t *testing.T) {
		pool, id, registration := readyAcquisitionWorker(t)
		ctx := context.Background()
		job := queueAcquisitionJob(t, pool, nil)
		if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_assignment() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected assignment failure'; END $$;
        CREATE TRIGGER reject_assignment BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION reject_assignment()`); err != nil {
			t.Fatal(err)
		}
		request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
		if _, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{}); err == nil {
			t.Fatal("audit failure ignored")
		}
		var state string
		var count int
		if err := pool.QueryRow(ctx, "SELECT state FROM jobs WHERE id=$1", job.ID).Scan(&state); err != nil || state != "QUEUED" {
			t.Fatal("partial assignment committed", state, err)
		}
		for _, table := range []string{"attempts", "reservations", "worker_requests"} {
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
				t.Fatal("partial assignment survived", table, count, err)
			}
		}
	})
}

func TestAcquisitionEnforcesEachCapacityDimension(t *testing.T) {
	for _, dimension := range []string{"cpu", "memory", "scratch", "slots"} {
		t.Run(dimension, func(t *testing.T) {
			pool, id, registration := readyAcquisitionWorker(t)
			ctx := context.Background()
			if _, err := pool.Exec(ctx, "UPDATE projects SET cpu_quota=100000,memory_quota_mib=100000,concurrency_quota=100"); err != nil {
				t.Fatal(err)
			}
			if dimension == "slots" {
				if _, err := pool.Exec(ctx, "UPDATE workers SET slots=2"); err != nil {
					t.Fatal(err)
				}
			}
			for range 5 {
				queueAcquisitionJob(t, pool, func(j *spec.Job) {
					j.Spec.Resources = spec.Resources{CPUMillis: 1, MemoryMiB: 1, ScratchMiB: 1}
					switch dimension {
					case "cpu":
						j.Spec.Resources.CPUMillis = 2000
					case "memory":
						j.Spec.Resources.MemoryMiB = 4096
					case "scratch":
						j.Spec.Resources.ScratchMiB = 8192
					}
				})
			}
			assigned := 0
			for range 5 {
				result, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
				if err != nil {
					t.Fatal(err)
				}
				if result.Assignment != nil {
					assigned++
				} else if result.NoWorkReason != "NO_RESOURCE_FIT" {
					t.Fatal(result)
				}
			}
			if assigned != 2 {
				t.Fatal("capacity limit ignored", dimension, assigned)
			}
		})
	}
}

func TestAcquisitionProjectQuotaAcrossWorkers(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	provision := workerProvision()
	provision.CertificateSHA256 = [32]byte{9}
	other, err := ProvisionWorker(ctx, pool, provision)
	if err != nil {
		t.Fatal(err)
	}
	otherRegistration := registration
	otherRegistration.SessionID = uuid.NewString()
	otherRegistration.RequestID = uuid.NewString()
	if _, err := RegisterSession(ctx, pool, other, otherRegistration); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordHeartbeat(ctx, pool, other, HeartbeatReport{SessionID: otherRegistration.SessionID, RequestID: uuid.NewString(), Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		queueAcquisitionJob(t, pool, nil)
	}
	failures := make(chan error, 16)
	var wg sync.WaitGroup
	for n := range 16 {
		identity, session := id, registration.SessionID
		if n%2 == 1 {
			identity, session = other, otherRegistration.SessionID
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := AcquireWork(ctx, pool, identity, AcquisitionRequest{SessionID: session, RequestID: uuid.NewString()}, AcquisitionPolicy{})
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
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM attempts").Scan(&count); err != nil || count != 2 {
		t.Fatal("workers oversubscribed shared project quota", count, err)
	}
}

func TestAcquisitionBackfillAndSoftScratchOptIn(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	queueAcquisitionJob(t, pool, func(j *spec.Job) { j.Spec.Resources = spec.Resources{CPUMillis: 3000, MemoryMiB: 1, ScratchMiB: 1} })
	acquire := func(policy AcquisitionPolicy) AcquisitionResult {
		t.Helper()
		result, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, policy)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	if acquire(AcquisitionPolicy{}).Assignment == nil {
		t.Fatal("initial assignment missing")
	}
	queueAcquisitionJob(t, pool, nil)
	small := queueAcquisitionJob(t, pool, func(j *spec.Job) { j.Spec.Resources = spec.Resources{CPUMillis: 500, MemoryMiB: 1, ScratchMiB: 1} })
	if _, err := pool.Exec(ctx, `UPDATE workers SET capabilities=(capabilities-'scratch.quota') || '{"scratch.soft":true}'`); err != nil {
		t.Fatal(err)
	}
	if result := acquire(AcquisitionPolicy{}); result.NoWorkReason != "PLACEMENT_MISMATCH" {
		t.Fatal("strict policy accepted soft scratch", result)
	}
	if result := acquire(AcquisitionPolicy{AllowSoftScratch: true}); result.Assignment == nil || result.Assignment.Authority.JobID != small.ID {
		t.Fatal("eligible small job was not backfilled", result)
	}
}

func TestAcquisitionReplayChecksExpiryAfterJobLock(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queueAcquisitionJob(t, pool, nil)
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	first, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{})
	if err != nil || first.Assignment == nil {
		t.Fatal(first, err)
	}
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(lock)
	if _, err := lock.Exec(ctx, "SELECT id FROM jobs FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	app := "acquire_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	waiting, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer waiting.Close()
	type outcome struct {
		result AcquisitionResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := AcquireWork(ctx, waiting, id, request, AcquisitionPolicy{})
		done <- outcome{result, err}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		var blocked bool
		if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'SELECT j.id%')", app).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("acquisition never waited for the job lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Move expiry past the waiting transaction's start, then release the job.
	// A transaction-start clock would incorrectly return live authority here.
	if _, err := lock.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp() WHERE id=$1", first.Assignment.Authority.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil || got.result.Decision != "FENCED" || got.result.Assignment != nil {
		t.Fatal("expired replay used stale transaction time", got)
	}
}

func TestAcquisitionRejectsCancelledAndFencedReplay(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	job := queueAcquisitionJob(t, pool, nil)
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	if result, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{}); err != nil || result.Assignment == nil {
		t.Fatal(result, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET cancel_requested=true,state='CANCELLING' WHERE id=$1", job.ID); err != nil {
		t.Fatal(err)
	}
	if result, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{}); err != nil || result.Decision != "STOP_REQUESTED" || result.Assignment != nil {
		t.Fatal("cancelled assignment replayed", result, err)
	}
	next := registration
	next.SessionID = uuid.NewString()
	next.RequestID = uuid.NewString()
	if err := ApproveSessionTakeover(ctx, pool, id.WorkerID, registration.SessionID, next.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSession(ctx, pool, id, next); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireWork(ctx, pool, id, request, AcquisitionPolicy{}); !errors.Is(err, ErrFenced) {
		t.Fatal("old session recovered assignment", err)
	}
}
