package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AttemptRecord struct {
	ID             string     `json:"id"`
	Number         int64      `json:"number"`
	State          string     `json:"state"`
	Reason         *string    `json:"reason"`
	ExitCode       *int32     `json:"exitCode"`
	WorkerID       string     `json:"workerId"`
	CleanupPending bool       `json:"cleanupPending"`
	CreatedAt      time.Time  `json:"createdAt"`
	FinishedAt     *time.Time `json:"finishedAt"`
}

type AttemptHistory struct {
	JobID    string          `json:"jobId"`
	Attempts []AttemptRecord `json:"attempts"`
}

func ListAttempts(ctx context.Context, pool *pgxpool.Pool, projectID, jobID string) (AttemptHistory, error) {
	if !canonicalUUID(projectID) || !canonicalUUID(jobID) {
		return AttemptHistory{}, ErrInvalid
	}
	var exists bool
	// Check the project-scoped parent even when it has no attempts. A foreign
	// or unknown job must never reveal whether another project's workers ran.
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1 AND project_id=$2)`, jobID, projectID).Scan(&exists); err != nil {
		return AttemptHistory{}, err
	}
	if !exists {
		return AttemptHistory{}, ErrNotFound
	}
	rows, err := pool.Query(ctx, `SELECT a.id::text,a.attempt_number,a.state,a.reason,a.exit_code,a.worker_id::text,a.cleanup_pending,a.created_at,a.finished_at
        FROM attempts a JOIN jobs j ON j.id=a.job_id
        WHERE a.job_id=$1 AND j.project_id=$2 ORDER BY a.attempt_number`, jobID, projectID)
	if err != nil {
		return AttemptHistory{}, err
	}
	defer rows.Close()
	history := AttemptHistory{JobID: jobID, Attempts: []AttemptRecord{}}
	for rows.Next() {
		var attempt AttemptRecord
		if err := rows.Scan(&attempt.ID, &attempt.Number, &attempt.State, &attempt.Reason, &attempt.ExitCode,
			&attempt.WorkerID, &attempt.CleanupPending, &attempt.CreatedAt, &attempt.FinishedAt); err != nil {
			return AttemptHistory{}, err
		}
		attempt.CreatedAt = attempt.CreatedAt.UTC()
		if attempt.FinishedAt != nil {
			value := attempt.FinishedAt.UTC()
			attempt.FinishedAt = &value
		}
		history.Attempts = append(history.Attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return AttemptHistory{}, err
	}
	return history, nil
}
