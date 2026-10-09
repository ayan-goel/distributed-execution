package store

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxReapBatch = 64

// ReapExpiredAttempts fences a bounded number of expired attempts. Each attempt
// gets its own transaction so one batch never holds all workers' capacity locks.
func ReapExpiredAttempts(ctx context.Context, pool *pgxpool.Pool, limit int) (int, error) {
	if limit < 1 || limit > MaxReapBatch {
		return 0, ErrInvalid
	}
	processed := 0
	for inspected := 0; inspected < limit && processed < limit; inspected++ {
		found, reaped, err := reapOneExpiredAttempt(ctx, pool)
		if err != nil {
			return processed, err
		}
		if !found {
			break
		}
		if reaped {
			processed++
		}
	}
	return processed, nil
}

func reapOneExpiredAttempt(ctx context.Context, pool *pgxpool.Pool) (bool, bool, error) {
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		return false, false, err
	}
	defer rollback(tx)
	var candidate recoveryAttempt
	err = tx.QueryRow(ctx, `SELECT j.id::text,a.id::text,a.lease_expires_at
	FROM attempts a JOIN jobs j ON j.current_attempt_id=a.id
	WHERE a.state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING')
	AND a.lease_expires_at<=clock_timestamp()
	ORDER BY a.lease_expires_at,a.id LIMIT 1`).Scan(&candidate.JobID, &candidate.ID, &candidate.LeaseExpiry)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if err := lockFailureJobs(ctx, tx, []string{candidate.JobID}); err != nil {
		return true, false, err
	}
	var current *string
	if err := tx.QueryRow(ctx, "SELECT current_attempt_id::text FROM jobs WHERE id=$1 FOR UPDATE", candidate.JobID).Scan(&current); err != nil {
		return true, false, err
	}
	var state, workerID string
	var expiry time.Time
	if err := tx.QueryRow(ctx, "SELECT state,worker_id::text,lease_expires_at FROM attempts WHERE id=$1 FOR UPDATE", candidate.ID).Scan(&state, &workerID, &expiry); err != nil {
		return true, false, err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return true, false, err
	}
	// A renewal may extend the lease while this transaction waits on the job.
	// Fresh DB time under both locks is the authority; the candidate scan is not.
	if current == nil || *current != candidate.ID || !slices.Contains([]string{"ASSIGNED", "STARTING", "RUNNING", "FINALIZING"}, state) || expiry.After(now) {
		return true, false, nil
	}
	if err := fenceLostAttempt(ctx, tx, candidate, now); err != nil {
		return true, false, err
	}
	// INVARIANT: physical execution is uncertain after lease loss. Keep this
	// worker ineligible and its reservation quarantined until a new incarnation
	// proves cleanup; another job must not silently reuse this host.
	if _, err := tx.Exec(ctx, "UPDATE workers SET state='REGISTERING',reconciliation_complete=false WHERE id=$1", workerID); err != nil {
		return true, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return true, false, err
	}
	return true, true, nil
}
