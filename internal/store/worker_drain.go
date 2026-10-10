package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WorkerDrain struct {
	WorkerID       string `json:"workerId"`
	State          string `json:"state"`
	DrainRequested bool   `json:"drainRequested"`
}

func RequestWorkerDrain(ctx context.Context, pool *pgxpool.Pool, p Principal, workerID string) (WorkerDrain, error) {
	if !p.Allows(RoleOperator) || !canonicalUUID(p.ProjectID) || !canonicalUUID(p.TokenID) {
		return WorkerDrain{}, ErrUnauthorized
	}
	if !canonicalUUID(workerID) {
		return WorkerDrain{}, ErrNotFound
	}
	// Serialize drain with new assignment decisions. Existing attempts and their
	// reservations/lease authority remain untouched and can finish normally.
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		return WorkerDrain{}, err
	}
	defer rollback(tx)
	var authorized bool
	// Hold authorization through commit so a revoked token or disabled project
	// cannot race a new operator mutation after the HTTP authentication check.
	err = tx.QueryRow(ctx, `SELECT true FROM api_tokens t JOIN projects p ON p.id=t.project_id
		WHERE t.id=$1 AND p.id=$2 AND t.role='operator' AND t.revoked_at IS NULL AND p.enabled
		FOR SHARE OF t,p`, p.TokenID, p.ProjectID).Scan(&authorized)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkerDrain{}, ErrUnauthorized
	}
	if err != nil {
		return WorkerDrain{}, err
	}
	result := WorkerDrain{WorkerID: workerID}
	err = tx.QueryRow(ctx, `SELECT w.state,w.drain_requested FROM workers w
		JOIN worker_projects wp ON wp.worker_id=w.id WHERE w.id=$1 AND wp.project_id=$2
		FOR UPDATE OF w`, workerID, p.ProjectID).Scan(&result.State, &result.DrainRequested)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkerDrain{}, ErrNotFound
	}
	if err != nil {
		return WorkerDrain{}, err
	}
	if !result.DrainRequested {
		// READY -> DRAINING records planned maintenance without overwriting
		// quarantine, registration, or stale/offline health evidence.
		if result.State == "READY" {
			result.State = "DRAINING"
		}
		if _, err = tx.Exec(ctx, "UPDATE workers SET drain_requested=true,state=$2 WHERE id=$1", workerID, result.State); err != nil {
			return WorkerDrain{}, err
		}
		// The paired audit records link host intent to its project/operator token.
		// Both must commit with the flag; repeats create no additional records.
		if _, err = tx.Exec(ctx, "INSERT INTO worker_audit_events(worker_id,action) VALUES($1,'WORKER_DRAIN_REQUESTED')", workerID); err != nil {
			return WorkerDrain{}, err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO audit_events(project_id,token_id,action) VALUES($1,$2,$3)", p.ProjectID, p.TokenID, "WORKER_DRAIN_REQUESTED:"+workerID); err != nil {
			return WorkerDrain{}, err
		}
	}
	result.DrainRequested = true
	if err = tx.Commit(ctx); err != nil {
		return WorkerDrain{}, err
	}
	return result, nil
}
