//go:build integration

package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const lastSchedulerProject = "ffffffff-ffff-ffff-ffff-ffffffffffff"

func addSchedulerProject(t *testing.T, pool *pgxpool.Pool, worker WorkerIdentity) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO projects(id,name,cpu_quota,memory_quota_mib,concurrency_quota)
		VALUES($1,'second',4000,8192,4)`, lastSchedulerProject); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO worker_projects(worker_id,project_id) VALUES($1,$2)", worker.WorkerID, lastSchedulerProject); err != nil {
		t.Fatal(err)
	}
}

func acquireSchedulerJob(t *testing.T, pool *pgxpool.Pool, worker WorkerIdentity, session, want string) *WorkAssignment {
	t.Helper()
	result, err := AcquireWork(context.Background(), pool, worker, AcquisitionRequest{SessionID: session, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || result.Assignment == nil || result.Assignment.Authority.JobID != want {
		t.Fatalf("want job %s, got %+v: %v", want, result, err)
	}
	return result.Assignment
}

func TestProjectRoundRobinOverridesCrossProjectPriority(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	addSchedulerProject(t, pool, worker)
	low := queueAcquisitionJob(t, pool, nil)
	first := queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Spec.Priority = 3 })
	next := queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Spec.Priority = 3 })
	ctx := context.Background()
	second := queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Metadata.Project = "second" })
	secondNext := queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Metadata.Project = "second" })
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	assigned, err := AcquireWork(ctx, pool, worker, request, AcquisitionPolicy{})
	if err != nil || assigned.Assignment == nil || assigned.Assignment.Authority.JobID != first.ID {
		t.Fatal("priority within first project", assigned, err)
	}
	// A busy project's older high-priority queue cannot consume both grants.
	secondAssignment := acquireSchedulerJob(t, pool, worker, registration.SessionID, second.ID)
	cursor, changed := schedulerCursor(t, pool)
	if cursor == nil || *cursor != second.ProjectID {
		t.Fatal("last selected project not persisted", cursor)
	}
	if replay, err := AcquireWork(ctx, pool, worker, request, AcquisitionPolicy{}); err != nil || replay.Assignment == nil || replay.Assignment.Authority != assigned.Assignment.Authority {
		t.Fatal("assignment replay", replay, err)
	}
	if empty, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{}); err != nil || empty.NoWorkReason != "NO_RESOURCE_FIT" {
		t.Fatal("full worker was assigned work", empty, err)
	}
	requireSchedulerCursor(t, pool, *cursor, changed)
	stopSchedulerJob(t, pool, worker, first, assigned.Assignment)
	stopSchedulerJob(t, pool, worker, second, secondAssignment)
	// Reconnect through a distinct pool: rotation cannot live in process memory.
	reconnected, err := pgxpool.NewWithConfig(ctx, pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	defer reconnected.Close()
	nextAssignment := acquireSchedulerJob(t, reconnected, worker, registration.SessionID, next.ID)
	secondNextAssignment := acquireSchedulerJob(t, reconnected, worker, registration.SessionID, secondNext.ID)
	stopSchedulerJob(t, pool, worker, next, nextAssignment)
	stopSchedulerJob(t, pool, worker, secondNext, secondNextAssignment)
	acquireSchedulerJob(t, reconnected, worker, registration.SessionID, low.ID)
}

func schedulerCursor(t *testing.T, pool *pgxpool.Pool) (*string, time.Time) {
	t.Helper()
	var project *string
	var updated time.Time
	if err := pool.QueryRow(context.Background(), "SELECT project_cursor::text,updated_at FROM scheduler_state WHERE singleton").Scan(&project, &updated); err != nil {
		t.Fatal(err)
	}
	return project, updated
}

func requireSchedulerCursor(t *testing.T, pool *pgxpool.Pool, want string, updated time.Time) {
	t.Helper()
	project, got := schedulerCursor(t, pool)
	if project == nil || *project != want || !got.Equal(updated) {
		t.Fatal("non-assignment changed rotation", project, got, want, updated)
	}
}

func stopSchedulerJob(t *testing.T, pool *pgxpool.Pool, worker WorkerIdentity, job JobRecord, assignment *WorkAssignment) {
	t.Helper()
	ctx := context.Background()
	if _, err := RequestCancellation(ctx, pool, job.ProjectID, job.ID); err != nil {
		t.Fatal(err)
	}
	// These grants were never launched. A stopped acknowledgement can release
	// their reservations without fabricating any runtime or output execution.
	r := CompletionRequest{Authority: assignment.Authority, CompletionID: uuid.NewString(), Reason: "USER_CANCELLED", Stopped: true}
	signCompletion(t, &r)
	if result, err := CompleteAttempt(ctx, pool, worker, r); err != nil || result.State != "CANCELLED" {
		t.Fatal("cancel acknowledgement", result, err)
	}
}

func TestProjectRoundRobinConcurrentWorkers(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	addSchedulerProject(t, pool, worker)
	ctx := context.Background()
	provision := workerProvision()
	provision.Name = "linux-second"
	provision.CertificateSHA256[0] ^= 1
	provision.Projects = append(provision.Projects, "second")
	other, err := ProvisionWorker(ctx, pool, provision)
	if err != nil {
		t.Fatal(err)
	}
	otherRegistration := registration
	otherRegistration.RequestID, otherRegistration.SessionID = uuid.NewString(), uuid.NewString()
	if _, err := RegisterSession(ctx, pool, other, otherRegistration); err != nil {
		t.Fatal(err)
	}
	if _, err := RecordHeartbeat(ctx, pool, other, HeartbeatReport{RequestID: uuid.NewString(), SessionID: otherRegistration.SessionID, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		queueAcquisitionJob(t, pool, nil)
		queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Metadata.Project = "second" })
	}
	var wg sync.WaitGroup
	failures := make(chan error, 16)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			identity, session := worker, registration.SessionID
			if i%2 == 1 {
				identity, session = other, otherRegistration.SessionID
			}
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
	var projects []string
	rows, err := pool.Query(ctx, "SELECT j.project_id::text FROM attempts a JOIN jobs j ON j.id=a.job_id ORDER BY a.created_at,a.id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			t.Fatal(err)
		}
		projects = append(projects, project)
	}
	if err := rows.Err(); err != nil || len(projects) != 4 || projects[0] == lastSchedulerProject || projects[1] != lastSchedulerProject || projects[2] != projects[0] || projects[3] != lastSchedulerProject {
		t.Fatal("global rotation across workers", projects, err)
	}
}

func TestProjectRoundRobinSkipsIneligibleProjects(t *testing.T) {
	for _, blocker := range []string{"disabled", "unauthorized", "placement", "resources", "quota", "backoff"} {
		t.Run(blocker, func(t *testing.T) {
			pool, worker, registration := readyAcquisitionWorker(t)
			addSchedulerProject(t, pool, worker)
			ctx := context.Background()
			if _, err := pool.Exec(ctx, "UPDATE projects SET cpu_quota=8000 WHERE name='research'"); err != nil {
				t.Fatal(err)
			}
			blocked := queueAcquisitionJob(t, pool, func(job *spec.Job) {
				if blocker == "placement" {
					job.Spec.Placement.Labels["architecture"] = "amd64"
				}
				if blocker == "resources" {
					job.Spec.Resources.CPUMillis = 6000
				}
			})
			query := map[string]string{
				"disabled":     "UPDATE projects SET enabled=false WHERE id=$1",
				"unauthorized": "DELETE FROM worker_projects WHERE project_id=$1",
				"quota":        "UPDATE projects SET cpu_quota=1000 WHERE id=$1",
				"backoff":      "UPDATE jobs SET next_eligible_at=clock_timestamp()+interval '1 hour' WHERE project_id=$1",
			}[blocker]
			if query != "" {
				if _, err := pool.Exec(ctx, query, blocked.ProjectID); err != nil {
					t.Fatal(err)
				}
			}
			fitting := queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Metadata.Project = "second" })
			acquireSchedulerJob(t, pool, worker, registration.SessionID, fitting.ID)
		})
	}
}

func TestProjectRoundRobinRollsBackWithAcquisitionResponse(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	addSchedulerProject(t, pool, worker)
	first := queueAcquisitionJob(t, pool, nil)
	second := queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Metadata.Project = "second" })
	acquireSchedulerJob(t, pool, worker, registration.SessionID, first.ID)
	cursor, changed := schedulerCursor(t, pool)
	ctx := context.Background()
	// Fail after the cursor write: neither rotation nor assignment may survive
	// if the response reference cannot commit, and the request must remain usable.
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_scheduler_response() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected acquisition response failure'; END $$;
		CREATE TRIGGER reject_scheduler_response BEFORE INSERT ON worker_requests
		FOR EACH ROW EXECUTE FUNCTION reject_scheduler_response()`); err != nil {
		t.Fatal(err)
	}
	request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
	if result, err := AcquireWork(ctx, pool, worker, request, AcquisitionPolicy{}); err == nil || result.Assignment != nil {
		t.Fatal("failed response exposed assignment", result, err)
	}
	requireSchedulerCursor(t, pool, *cursor, changed)
	for _, table := range []string{"attempts", "reservations", "worker_requests"} {
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 1 {
			t.Fatal("rolled back acquire leaked writes", table, count, err)
		}
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER reject_scheduler_response ON worker_requests"); err != nil {
		t.Fatal(err)
	}
	if result, err := AcquireWork(ctx, pool, worker, request, AcquisitionPolicy{}); err != nil || result.Assignment == nil || result.Assignment.Authority.JobID != second.ID {
		t.Fatal("rolled back request was not reusable", result, err)
	}
}

func TestProjectRoundRobinFailsClosedWithoutCursor(t *testing.T) {
	pool, worker, registration := readyAcquisitionWorker(t)
	queueAcquisitionJob(t, pool, nil)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "DELETE FROM scheduler_state"); err != nil {
		t.Fatal(err)
	}
	if result, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{}); !errors.Is(err, ErrInvalid) || result.Assignment != nil {
		t.Fatal("missing scheduler singleton silently accepted", result, err)
	}
	var writes int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM attempts)+(SELECT count(*) FROM worker_requests)").Scan(&writes); err != nil || writes != 0 {
		t.Fatal("missing cursor leaked assignment or replay key", writes, err)
	}
}
