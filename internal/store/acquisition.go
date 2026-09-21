package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const InitialLease = 30 * time.Second
const WorkerSuspectAfter = 15 * time.Second

type AcquisitionRequest struct{ SessionID, RequestID string }
type AcquisitionPolicy struct{ AllowSoftScratch bool }
type WorkAssignment struct {
	Authority                                 AttemptAuthority
	Job                                       spec.Job
	CanonicalSpec                             []byte
	SpecHash                                  string
	LeaseExpiresAt, PhaseDeadline, ServerTime time.Time
}
type AcquisitionResult struct {
	Assignment             *WorkAssignment
	NoWorkReason, Decision string
}

type acquisitionWorker struct {
	current, session                         *string
	fenced, heartbeat                        *time.Time
	state                                    string
	healthy, reconciled, diskPressure, drain bool
	free                                     spec.Resources
	slots                                    int64
	labels, capabilities                     []byte
}

func readAcquisitionWorker(ctx context.Context, tx pgx.Tx, id WorkerIdentity, session string, lock bool) (w acquisitionWorker, err error) {
	query := `SELECT w.current_session_id::text,s.id::text,s.fenced_at,w.last_heartbeat_at,w.state,w.runtime_healthy,w.reconciliation_complete,w.disk_pressure,w.drain_requested,
    w.cpu_millis-COALESCE(r.cpu,0),w.memory_mib-COALESCE(r.memory,0),w.scratch_mib-COALESCE(r.scratch,0),w.slots-COALESCE(r.slots,0),w.labels,w.capabilities
    FROM workers w LEFT JOIN worker_sessions s ON s.worker_id=w.id AND s.id=$2
    LEFT JOIN LATERAL (SELECT sum(cpu_millis) cpu,sum(memory_mib) memory,sum(scratch_mib) scratch,count(*) slots FROM reservations WHERE worker_id=w.id AND state IN ('active','quarantined')) r ON true
    WHERE w.id=$1`
	if lock {
		query += " FOR UPDATE OF w"
	}
	err = tx.QueryRow(ctx, query, id.WorkerID, session).Scan(&w.current, &w.session, &w.fenced, &w.heartbeat, &w.state, &w.healthy, &w.reconciled, &w.diskPressure, &w.drain, &w.free.CPUMillis, &w.free.MemoryMiB, &w.free.ScratchMiB, &w.slots, &w.labels, &w.capabilities)
	if err == nil && (w.current == nil || w.session == nil || *w.current != session || w.fenced != nil) {
		err = ErrFenced
	}
	return
}

func (w acquisitionWorker) readiness(now time.Time, policy AcquisitionPolicy) string {
	if w.state != "READY" || !w.healthy || !w.reconciled || w.diskPressure || w.drain || w.heartbeat == nil || now.Sub(*w.heartbeat) >= WorkerSuspectAfter {
		return "WORKER_NOT_READY"
	}
	var caps map[string]bool
	if json.Unmarshal(w.capabilities, &caps) != nil || !caps["docker.v1"] || !caps["cpu.hard"] || !caps["memory.hard"] || !caps["pids.hard"] || (!caps["scratch.quota"] && !(policy.AllowSoftScratch && caps["scratch.soft"])) {
		return "PLACEMENT_MISMATCH"
	}
	return ""
}

// AcquireWork commits job ownership, physical capacity, replay identity and the
// assignment event together. The result is not execution authority until commit.
func AcquireWork(ctx context.Context, pool *pgxpool.Pool, identity WorkerIdentity, request AcquisitionRequest, policy AcquisitionPolicy) (AcquisitionResult, error) {
	if !canonicalUUID(identity.WorkerID) || !canonicalUUID(identity.CredentialID) {
		return AcquisitionResult{}, ErrUnauthorized
	}
	if !canonicalUUID(request.SessionID) || !canonicalUUID(request.RequestID) {
		return AcquisitionResult{}, ErrInvalid
	}
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		return AcquisitionResult{}, err
	}
	defer rollback(tx)
	if err = authorizeWorkerTx(ctx, tx, identity); err != nil {
		return AcquisitionResult{}, err
	}
	// Acquire currently has no mutable payload. The operation tag prevents a
	// future method sharing the replay table from interpreting this request.
	digest := sha256.Sum256([]byte("dispatch.worker.v1.AcquireWork"))
	hash := hex.EncodeToString(digest[:])
	var savedAttempt, savedReason *string
	var savedHash string
	err = tx.QueryRow(ctx, "SELECT attempt_id::text,no_work_reason,request_hash FROM worker_requests WHERE worker_id=$1 AND session_id=$2 AND request_id=$3", identity.WorkerID, request.SessionID, request.RequestID).Scan(&savedAttempt, &savedReason, &savedHash)
	replay := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return AcquisitionResult{}, err
	}
	if replay && savedHash != hash {
		return AcquisitionResult{}, ErrConflict
	}
	if replay && savedAttempt != nil {
		result, err := replayAssignment(ctx, tx, identity, request.SessionID, *savedAttempt)
		if err != nil {
			return AcquisitionResult{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return AcquisitionResult{}, err
		}
		return result, nil
	}
	w, err := readAcquisitionWorker(ctx, tx, identity, request.SessionID, false)
	if err != nil {
		return AcquisitionResult{}, err
	}
	if replay {
		return AcquisitionResult{NoWorkReason: *savedReason}, nil
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return AcquisitionResult{}, err
	}
	reason := w.readiness(now, policy)
	jobID := ""
	if reason == "" {
		// Resource-changing transactions share the cluster lock. Rank fitting
		// candidates ahead of blocked jobs to permit backfill without overbooking.
		err = tx.QueryRow(ctx, `WITH usage AS (
            SELECT j.project_id,sum(r.cpu_millis) cpu,sum(r.memory_mib) memory,count(*) slots
            FROM attempts a JOIN jobs j ON j.id=a.job_id JOIN reservations r ON r.attempt_id=a.id
            WHERE a.state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING') GROUP BY j.project_id
        ), candidates AS (
            SELECT j.id,j.priority,j.created_at,CASE
            WHEN NOT ($6::jsonb @> COALESCE(j.spec->'spec'->'placement'->'labels','{}'::jsonb)) THEN 'PLACEMENT_MISMATCH'
            WHEN j.cpu_millis>$2 OR j.memory_mib>$3 OR j.scratch_mib>$4 OR $5<1 THEN 'NO_RESOURCE_FIT'
            WHEN j.cpu_millis>p.cpu_quota-COALESCE(u.cpu,0) OR j.memory_mib>p.memory_quota_mib-COALESCE(u.memory,0) OR COALESCE(u.slots,0)>=p.concurrency_quota THEN 'PROJECT_QUOTA'
            ELSE '' END reason
            FROM jobs j JOIN projects p ON p.id=j.project_id JOIN worker_projects wp ON wp.project_id=p.id AND wp.worker_id=$1
            LEFT JOIN usage u ON u.project_id=p.id
            WHERE p.enabled AND j.state IN ('QUEUED','RETRY_WAIT') AND NOT j.cancel_requested AND j.next_eligible_at<=clock_timestamp()
        ) SELECT id::text,reason FROM candidates ORDER BY (reason='') DESC,priority DESC,created_at,id LIMIT 1`, identity.WorkerID, w.free.CPUMillis, w.free.MemoryMiB, w.free.ScratchMiB, w.slots, w.labels).Scan(&jobID, &reason)
		if errors.Is(err, pgx.ErrNoRows) {
			reason = "QUEUE_EMPTY"
		} else if err != nil {
			return AcquisitionResult{}, err
		}
	}
	var job spec.Job
	var projectID, specHash, state string
	var body []byte
	var counter int64
	var cancelled bool
	var eligible time.Time
	if jobID != "" {
		// Only one job is involved. Lock it before worker/project accounting,
		// preserving the same ordering used by recovery and lease transitions.
		err = tx.QueryRow(ctx, "SELECT project_id::text,spec,spec_hash,state,attempt_counter,cancel_requested,next_eligible_at FROM jobs WHERE id=$1 FOR UPDATE", jobID).Scan(&projectID, &body, &specHash, &state, &counter, &cancelled, &eligible)
		if err != nil {
			return AcquisitionResult{}, err
		}
		if json.Unmarshal(body, &job) != nil || job.Validate() != nil {
			return AcquisitionResult{}, ErrInvalid
		}
	}
	w, err = readAcquisitionWorker(ctx, tx, identity, request.SessionID, true)
	if err != nil {
		return AcquisitionResult{}, err
	}
	if jobID != "" {
		var cpuQuota, memoryQuota, slotQuota int64
		var enabled bool
		if err = tx.QueryRow(ctx, "SELECT cpu_quota,memory_quota_mib,concurrency_quota,enabled FROM projects WHERE id=$1 FOR SHARE", projectID).Scan(&cpuQuota, &memoryQuota, &slotQuota, &enabled); err != nil {
			return AcquisitionResult{}, err
		}
		var usedCPU, usedMemory, usedSlots int64
		if err = tx.QueryRow(ctx, `SELECT COALESCE(sum(r.cpu_millis),0),COALESCE(sum(r.memory_mib),0),count(*) FROM attempts a JOIN jobs j ON j.id=a.job_id JOIN reservations r ON r.attempt_id=a.id WHERE j.project_id=$1 AND a.state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING')`, projectID).Scan(&usedCPU, &usedMemory, &usedSlots); err != nil {
			return AcquisitionResult{}, err
		}
		if !enabled || job.Spec.Resources.CPUMillis > cpuQuota-usedCPU || job.Spec.Resources.MemoryMiB > memoryQuota-usedMemory || usedSlots >= slotQuota {
			reason = "PROJECT_QUOTA"
		}
		if job.Spec.Resources.CPUMillis > w.free.CPUMillis || job.Spec.Resources.MemoryMiB > w.free.MemoryMiB || job.Spec.Resources.ScratchMiB > w.free.ScratchMiB || w.slots < 1 {
			reason = "NO_RESOURCE_FIT"
		}
		var labels map[string]string
		if json.Unmarshal(w.labels, &labels) != nil {
			return AcquisitionResult{}, ErrInvalid
		}
		for key, value := range job.Spec.Placement.Labels {
			if actual, ok := labels[key]; !ok || actual != value {
				reason = "PLACEMENT_MISMATCH"
			}
		}
	}
	// Fresh wall time is taken after locks. An old transaction snapshot must not
	// assign to a host that became suspect while waiting for accounting locks.
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return AcquisitionResult{}, err
	}
	if readiness := w.readiness(now, policy); readiness != "" {
		reason = readiness
	}
	if jobID != "" && (!slices.Contains([]string{"QUEUED", "RETRY_WAIT"}, state) || cancelled || eligible.After(now)) {
		reason = "QUEUE_EMPTY"
	}
	result := AcquisitionResult{NoWorkReason: reason}
	var attemptID *string
	if reason == "" {
		if counter >= int64(job.Spec.Retry.MaxAttempts) || len(job.Spec.Inputs) != 0 || !pinnedImage.MatchString(job.Spec.Image) {
			return AcquisitionResult{}, ErrInvalid
		}
		canonical, computedHash, err := job.Canonical()
		if err != nil || computedHash != specHash {
			return AcquisitionResult{}, ErrInvalid
		}
		id := uuid.NewString()
		attemptID = &id
		assignment := &WorkAssignment{Authority: AttemptAuthority{JobID: jobID, AttemptID: id, Generation: counter + 1, WorkerID: identity.WorkerID, SessionID: request.SessionID}, Job: job, CanonicalSpec: canonical, SpecHash: specHash, LeaseExpiresAt: now.Add(InitialLease), PhaseDeadline: now.Add(time.Duration(job.Spec.Timeouts.StartupSeconds) * time.Second), ServerTime: now}
		if _, err = tx.Exec(ctx, `INSERT INTO attempts(id,job_id,attempt_number,generation,worker_id,session_id,lease_expires_at,phase_deadline) VALUES($1,$2,$3,$3,$4,$5,$6,$7)`, id, jobID, counter+1, identity.WorkerID, request.SessionID, assignment.LeaseExpiresAt, assignment.PhaseDeadline); err != nil {
			return AcquisitionResult{}, err
		}
		resources := job.Spec.Resources
		if _, err = tx.Exec(ctx, "INSERT INTO reservations(attempt_id,worker_id,cpu_millis,memory_mib,scratch_mib) VALUES($1,$2,$3,$4,$5)", id, identity.WorkerID, resources.CPUMillis, resources.MemoryMiB, resources.ScratchMiB); err != nil {
			return AcquisitionResult{}, err
		}
		var sequence int64
		if err = tx.QueryRow(ctx, "UPDATE jobs SET state='ACTIVE',current_attempt_id=$2,attempt_counter=$3,event_sequence=event_sequence+1 WHERE id=$1 RETURNING event_sequence", jobID, id, counter+1).Scan(&sequence); err != nil {
			return AcquisitionResult{}, err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO job_events(job_id,sequence,attempt_id,type,payload) VALUES($1,$2,$3,'ASSIGNED',jsonb_build_object('workerId',$4::text,'sessionId',$5::text,'generation',$6::bigint))", jobID, sequence, id, identity.WorkerID, request.SessionID, counter+1); err != nil {
			return AcquisitionResult{}, err
		}
		result.Assignment = assignment
	}
	var noWork *string
	if reason != "" {
		noWork = &reason
	}
	if _, err = tx.Exec(ctx, "INSERT INTO worker_requests(worker_id,session_id,request_id,request_hash,attempt_id,no_work_reason) VALUES($1,$2,$3,$4,$5,$6)", identity.WorkerID, request.SessionID, request.RequestID, hash, attemptID, noWork); err != nil {
		return AcquisitionResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return AcquisitionResult{}, err
	}
	return result, nil
}

func replayAssignment(ctx context.Context, tx pgx.Tx, identity WorkerIdentity, sessionID, attemptID string) (AcquisitionResult, error) {
	var assignment WorkAssignment
	var body []byte
	var current *string
	var cancelled bool
	var state string
	if err := tx.QueryRow(ctx, "SELECT j.id::text,j.current_attempt_id::text,j.cancel_requested,j.spec,j.spec_hash FROM jobs j JOIN attempts a ON a.job_id=j.id WHERE a.id=$1 FOR UPDATE OF j", attemptID).Scan(&assignment.Authority.JobID, &current, &cancelled, &body, &assignment.SpecHash); err != nil {
		return AcquisitionResult{}, err
	}
	if err := tx.QueryRow(ctx, "SELECT id::text,worker_id::text,session_id::text,generation,state,lease_expires_at,phase_deadline FROM attempts WHERE id=$1 FOR UPDATE", attemptID).Scan(&assignment.Authority.AttemptID, &assignment.Authority.WorkerID, &assignment.Authority.SessionID, &assignment.Authority.Generation, &state, &assignment.LeaseExpiresAt, &assignment.PhaseDeadline); err != nil {
		return AcquisitionResult{}, err
	}
	if _, err := readAcquisitionWorker(ctx, tx, identity, sessionID, true); err != nil {
		return AcquisitionResult{}, err
	}
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&assignment.ServerTime); err != nil {
		return AcquisitionResult{}, err
	}
	if assignment.Authority.WorkerID != identity.WorkerID || assignment.Authority.SessionID != sessionID {
		return AcquisitionResult{}, ErrFenced
	}
	if !slices.Contains([]string{"ASSIGNED", "STARTING", "RUNNING", "FINALIZING"}, state) {
		return AcquisitionResult{Decision: "ALREADY_TERMINAL"}, nil
	}
	if current == nil || *current != attemptID || !assignment.LeaseExpiresAt.After(assignment.ServerTime) {
		return AcquisitionResult{Decision: "FENCED"}, nil
	}
	if cancelled || !assignment.PhaseDeadline.After(assignment.ServerTime) {
		return AcquisitionResult{Decision: "STOP_REQUESTED"}, nil
	}
	if json.Unmarshal(body, &assignment.Job) != nil {
		return AcquisitionResult{}, ErrInvalid
	}
	canonical, hash, err := assignment.Job.Canonical()
	if err != nil || hash != assignment.SpecHash {
		return AcquisitionResult{}, ErrInvalid
	}
	assignment.CanonicalSpec = canonical
	return AcquisitionResult{Assignment: &assignment}, nil
}
