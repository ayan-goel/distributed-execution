package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const QueueBlockerHistoryLimit = 16

type QueueDiagnostics struct {
	AsOf           time.Time      `json:"asOf"`
	AttemptCounter int64          `json:"attemptCounter"`
	Observations   []QueueBlocker `json:"observations"`
}

type QueueBlocker struct {
	Sequence       int64     `json:"sequence"`
	WorkerID       string    `json:"workerId"`
	SessionID      string    `json:"sessionId"`
	RequestID      string    `json:"requestId"`
	AttemptCounter int64     `json:"attemptCounter"`
	Reason         string    `json:"reason"`
	ObservedAt     time.Time `json:"observedAt"`
}

func validQueueBlockerReason(reason string) bool {
	switch reason {
	case "PLACEMENT_MISMATCH", "NO_RESOURCE_FIT", "PROJECT_QUOTA", "SWEEP_CONCURRENCY", "PROJECT_DISABLED", "RETRY_BACKOFF":
		return true
	}
	return false
}

// recordQueueBlocker requires the cluster transition lock and caller authorization.
// Provenance identifies an observation; it cannot authenticate a worker or grant
// placement authority. Only retained observations can be deduplicated here.
// Use the selection's database time so lock waits do not shift the observed check.
func recordQueueBlocker(ctx context.Context, tx pgx.Tx, jobID, workerID string, request AcquisitionRequest, reason string, observedAt time.Time) error {
	if !canonicalUUID(jobID) || !canonicalUUID(workerID) || !canonicalUUID(request.SessionID) || !canonicalUUID(request.RequestID) || !validQueueBlockerReason(reason) || observedAt.IsZero() {
		return ErrInvalid
	}
	var savedReason string
	err := tx.QueryRow(ctx, `SELECT reason FROM job_queue_blockers
		WHERE job_id=$1 AND worker_id=$2 AND session_id=$3 AND request_id=$4`, jobID, workerID, request.SessionID, request.RequestID).Scan(&savedReason)
	if err == nil {
		if savedReason != reason {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	// Prefer a vacant slot, otherwise replace the oldest sequence. Global IDs
	// may contain gaps, so taking an ID modulo the bound could evict newer history.
	// Reject nonqueued jobs so stale placement checks cannot append new blockers.
	var sequence int64
	err = tx.QueryRow(ctx, `INSERT INTO job_queue_blockers(job_id,slot,worker_id,session_id,request_id,attempt_counter,reason,observed_at)
		SELECT j.id,(SELECT s.slot FROM generate_series(0,$6::integer-1) s(slot)
			LEFT JOIN job_queue_blockers b ON b.job_id=j.id AND b.slot=s.slot
			ORDER BY b.id NULLS FIRST,s.slot LIMIT 1),$2,$3,$4,j.attempt_counter,$5,$7 FROM jobs j
		WHERE j.id=$1 AND j.state IN ('QUEUED','RETRY_WAIT') AND NOT j.cancel_requested
		ON CONFLICT(job_id,slot) DO UPDATE SET id=EXCLUDED.id,worker_id=EXCLUDED.worker_id,
			session_id=EXCLUDED.session_id,request_id=EXCLUDED.request_id,attempt_counter=EXCLUDED.attempt_counter,
			reason=EXCLUDED.reason,observed_at=EXCLUDED.observed_at RETURNING id`, jobID, workerID, request.SessionID, request.RequestID, reason, QueueBlockerHistoryLimit, observedAt).Scan(&sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalid
	}
	return err
}

const queueBlockerHistorySQL = `SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'sequence',b.id,'workerId',b.worker_id,'sessionId',b.session_id,'requestId',b.request_id,
    'attemptCounter',b.attempt_counter,'reason',b.reason,'observedAt',b.observed_at) ORDER BY b.id DESC),'[]'::jsonb)
    FROM job_queue_blockers b WHERE b.job_id=j.id`

func GetQueueBlockers(ctx context.Context, pool *pgxpool.Pool, projectID, jobID string) ([]QueueBlocker, error) {
	var body []byte
	// Scope by the owning job before returning worker-specific history. A foreign
	// job must be indistinguishable from an absent job, including empty histories.
	err := pool.QueryRow(ctx, "SELECT ("+queueBlockerHistorySQL+") FROM jobs j WHERE j.id=$1 AND j.project_id=$2", jobID, projectID).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var history []QueueBlocker
	err = json.Unmarshal(body, &history)
	return history, err
}
