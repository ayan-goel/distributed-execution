package store

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Lock the full failure scope before any attempts. Renewal batches lock sorted
// jobs without the cluster lock; adding sibling locks afterward could deadlock.
func lockFailureJobs(ctx context.Context, tx pgx.Tx, jobIDs []string) error {
	rows, err := tx.Query(ctx, `SELECT j.id::text FROM jobs j
		WHERE j.id=ANY($1::uuid[]) OR j.sweep_id IN (
			SELECT seed.sweep_id FROM jobs seed JOIN sweeps s ON s.id=seed.sweep_id
			WHERE seed.id=ANY($1::uuid[]) AND s.fail_fast)
		ORDER BY j.id FOR UPDATE OF j`, jobIDs)
	if err != nil {
		return err
	}
	var id string
	_, err = pgx.ForEachRow(rows, []any{&id}, func() error { return nil })
	return err
}

// INVARIANT: permanent failure and sibling stop intent commit together under
// the cluster lock, preventing acquisition between the two state transitions.
// Active reservations remain held until physical stop or fencing is confirmed.
func applySweepFailFast(ctx context.Context, tx pgx.Tx, failedJobID string) error {
	_, err := tx.Exec(ctx, `WITH policy AS (
		SELECT s.id AS sweep_id,s.cancel_running_on_failure
		FROM jobs failed JOIN sweeps s ON s.id=failed.sweep_id
		WHERE failed.id=$1 AND failed.state='FAILED' AND s.fail_fast
	), candidates AS (
		SELECT j.id,j.state AS previous_state,p.sweep_id
		FROM jobs j JOIN policy p ON p.sweep_id=j.sweep_id
		WHERE j.id<>$1 AND (j.state IN ('QUEUED','RETRY_WAIT')
			OR (j.state='ACTIVE' AND p.cancel_running_on_failure))
	), changed AS (
		UPDATE jobs j SET state=CASE WHEN c.previous_state='ACTIVE' THEN 'CANCELLING' ELSE 'CANCELLED' END,
			cancel_requested=true,event_sequence=j.event_sequence+1
		FROM candidates c WHERE j.id=c.id
		RETURNING j.id,j.event_sequence,c.previous_state,j.state,c.sweep_id
	)
	INSERT INTO job_events(job_id,sequence,type,payload)
	SELECT id,event_sequence,'SWEEP_FAIL_FAST',jsonb_build_object(
		'sweepId',sweep_id,'failedJobId',$1::text,'previousState',previous_state,'state',state)
	FROM changed`, failedJobID)
	return err
}
