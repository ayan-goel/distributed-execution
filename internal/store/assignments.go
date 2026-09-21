package store

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxAssignmentPageSize = 64

type AssignmentPage struct {
	Assignments    []WorkAssignment
	NextAfterJobID string
}

type assignmentReference struct{ JobID, AttemptID string }

// ListAssignments recovers only actionable authority for the current incarnation.
// Reading inventory never renews a lease or releases an uncertain reservation.
func ListAssignments(ctx context.Context, pool *pgxpool.Pool, identity WorkerIdentity, sessionID, afterJobID string, pageSize int) (AssignmentPage, error) {
	if !canonicalUUID(identity.WorkerID) || !canonicalUUID(identity.CredentialID) {
		return AssignmentPage{}, ErrUnauthorized
	}
	if !canonicalUUID(sessionID) || (afterJobID != "" && !canonicalUUID(afterJobID)) || pageSize < 0 || pageSize > MaxAssignmentPageSize {
		return AssignmentPage{}, ErrInvalid
	}
	if pageSize == 0 {
		pageSize = 32
	}
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		return AssignmentPage{}, err
	}
	defer rollback(tx)
	if err = authorizeWorkerTx(ctx, tx, identity); err != nil {
		return AssignmentPage{}, err
	}
	var cursor *string
	if afterJobID != "" {
		cursor = &afterJobID
	}
	// Lock every candidate job in UUID order before any attempts or worker row.
	// The extra candidate establishes continuation without loading its job spec.
	rows, err := tx.Query(ctx, `SELECT j.id::text,a.id::text FROM jobs j JOIN attempts a ON a.job_id=j.id
    WHERE a.worker_id=$1 AND a.session_id=$2 AND a.state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING')
    AND ($3::uuid IS NULL OR j.id>$3::uuid) ORDER BY j.id LIMIT $4 FOR UPDATE OF j`, identity.WorkerID, sessionID, cursor, pageSize+1)
	if err != nil {
		return AssignmentPage{}, err
	}
	references, err := pgx.CollectRows(rows, pgx.RowToStructByPos[assignmentReference])
	if err != nil {
		return AssignmentPage{}, err
	}
	ids := make([]string, 0, len(references))
	for _, ref := range references {
		ids = append(ids, ref.AttemptID)
	}
	rows, err = tx.Query(ctx, "SELECT id::text FROM attempts WHERE id=ANY($1::uuid[]) ORDER BY job_id,id FOR UPDATE", ids)
	if err != nil {
		return AssignmentPage{}, err
	}
	if _, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
		return AssignmentPage{}, err
	}
	if _, err = readAcquisitionWorker(ctx, tx, identity, sessionID, true); err != nil {
		return AssignmentPage{}, err
	}
	page := AssignmentPage{}
	more := len(references) > pageSize
	if more {
		references = references[:pageSize]
	}
	last := afterJobID
	bytes := 0
	// Canonical JSON includes the argv/image bytes repeated on the wire. Budget
	// half the RPC limit plus envelope headroom. One large item may stand alone;
	// the RPC adapter additionally checks its exact protobuf size before sending.
	const specBudget = (4*1024*1024 - 64*1024) / 2
	for _, ref := range references {
		// These job/attempt/worker locks are already held, so the replay reader
		// cannot invert lock order while recomputing fresh remaining authority.
		recovered, err := replayAssignment(ctx, tx, identity, sessionID, ref.AttemptID)
		if err != nil {
			return AssignmentPage{}, err
		}
		if recovered.Assignment != nil {
			size := len(recovered.Assignment.CanonicalSpec)
			if len(page.Assignments) > 0 && bytes+size > specBudget {
				more = true
				break
			}
			bytes += size
			page.Assignments = append(page.Assignments, *recovered.Assignment)
		}
		// Expired/cancelled candidates still advance the scan. Empty pages must
		// not loop forever or cause the caller to assume that recovery finished.
		last = ref.JobID
	}
	if more {
		page.NextAfterJobID = last
	}
	if err = tx.Commit(ctx); err != nil {
		return AssignmentPage{}, err
	}
	return page, nil
}
