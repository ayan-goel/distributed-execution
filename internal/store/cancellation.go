package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RequestCancellation records stop intent under the same transition lock as
// completion, so their commit order determines whether success can be accepted.
func RequestCancellation(ctx context.Context, pool *pgxpool.Pool, projectID, jobID string) (JobRecord, error) {
	if !canonicalUUID(projectID) || !canonicalUUID(jobID) {
		return JobRecord{}, ErrNotFound
	}
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		return JobRecord{}, err
	}
	defer rollback(tx)
	var state string
	err = tx.QueryRow(ctx, "SELECT state FROM jobs WHERE id=$1 AND project_id=$2 FOR UPDATE", jobID, projectID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return JobRecord{}, ErrNotFound
	}
	if err != nil {
		return JobRecord{}, err
	}
	// Terminal jobs and repeated requests retain their first durable outcome.
	// Active capacity stays reserved until the worker confirms a physical stop
	// or lease/session fencing quarantines it.
	switch state {
	case "QUEUED", "RETRY_WAIT", "ACTIVE":
		next := "CANCELLED"
		if state == "ACTIVE" {
			next = "CANCELLING"
		}
		var sequence int64
		err = tx.QueryRow(ctx, "UPDATE jobs SET state=$2,cancel_requested=true,event_sequence=event_sequence+1 WHERE id=$1 RETURNING event_sequence", jobID, next).Scan(&sequence)
		if err != nil {
			return JobRecord{}, err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO job_events(job_id,sequence,type,payload) VALUES($1,$2,'CANCEL_REQUESTED',jsonb_build_object('previousState',$3::text,'state',$4::text))", jobID, sequence, state, next); err != nil {
			return JobRecord{}, err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO audit_events(project_id,action) VALUES($1,'JOB_CANCEL_REQUESTED')", projectID); err != nil {
			return JobRecord{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return JobRecord{}, err
	}
	return GetJob(ctx, pool, projectID, jobID)
}
