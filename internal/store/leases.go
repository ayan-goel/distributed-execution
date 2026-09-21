package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxLeaseRenewalBatch = 64

type LeaseRenewal struct {
	SessionID, RequestID string
	Attempts             []AttemptAuthority
}

type LeaseGrant struct {
	Authority                                 AttemptAuthority
	Decision                                  string
	LeaseExpiresAt, PhaseDeadline, ServerTime time.Time
}

func (r LeaseRenewal) hash(workerID string) (string, error) {
	if !canonicalUUID(r.SessionID) || !canonicalUUID(r.RequestID) || len(r.Attempts) < 1 || len(r.Attempts) > MaxLeaseRenewalBatch {
		return "", ErrInvalid
	}
	seen := map[string]bool{}
	for _, a := range r.Attempts {
		if !canonicalUUID(a.JobID) || !canonicalUUID(a.AttemptID) || a.WorkerID != workerID || a.SessionID != r.SessionID || a.Generation < 1 || seen[a.AttemptID] {
			return "", ErrInvalid
		}
		seen[a.AttemptID] = true
	}
	// A batch is a set of authorities; transport order must not change its replay
	// identity. Clone before sorting so the caller still receives its own order.
	ordered := slices.Clone(r.Attempts)
	slices.SortFunc(ordered, func(a, b AttemptAuthority) int { return strings.Compare(a.AttemptID, b.AttemptID) })
	body, err := json.Marshal(ordered)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("dispatch.worker.v1.RenewLeases\n"), body...))
	return hex.EncodeToString(digest[:]), nil
}

func requireCurrentWorkerSession(ctx context.Context, tx pgx.Tx, workerID, sessionID string) error {
	var current bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM workers w JOIN worker_sessions s ON s.worker_id=w.id AND s.id=w.current_session_id WHERE w.id=$1 AND s.id=$2 AND s.fenced_at IS NULL)`, workerID, sessionID).Scan(&current)
	if err != nil {
		return err
	}
	if !current {
		return ErrFenced
	}
	return nil
}

type leaseJob struct {
	current   *string
	cancelled bool
}
type leaseAttempt struct {
	authority      AttemptAuthority
	state          string
	expires, phase time.Time
}

func lockLeaseBatch(ctx context.Context, tx pgx.Tx, authorities []AttemptAuthority) (map[string]leaseJob, map[string]leaseAttempt, error) {
	jobIDs := make([]string, 0, len(authorities))
	attemptIDs := make([]string, 0, len(authorities))
	for _, a := range authorities {
		jobIDs = append(jobIDs, a.JobID)
		attemptIDs = append(attemptIDs, a.AttemptID)
	}
	jobs := map[string]leaseJob{}
	rows, err := tx.Query(ctx, `SELECT id::text,current_attempt_id::text,cancel_requested FROM jobs WHERE id=ANY($1::uuid[]) ORDER BY id FOR UPDATE`, jobIDs)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var id string
		var job leaseJob
		if err = rows.Scan(&id, &job.current, &job.cancelled); err != nil {
			rows.Close()
			return nil, nil, err
		}
		jobs[id] = job
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	// Restrict both dimensions: an invalid job/attempt pair must not acquire an
	// attempt lock outside the jobs already held, inverting recovery lock order.
	rows, err = tx.Query(ctx, `SELECT job_id::text,id::text,worker_id::text,session_id::text,generation,state,lease_expires_at,phase_deadline FROM attempts WHERE job_id=ANY($1::uuid[]) AND id=ANY($2::uuid[]) ORDER BY job_id,id FOR UPDATE`, jobIDs, attemptIDs)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	attempts := map[string]leaseAttempt{}
	for rows.Next() {
		var a leaseAttempt
		if err = rows.Scan(&a.authority.JobID, &a.authority.AttemptID, &a.authority.WorkerID, &a.authority.SessionID, &a.authority.Generation, &a.state, &a.expires, &a.phase); err != nil {
			return nil, nil, err
		}
		attempts[a.authority.AttemptID] = a
	}
	return jobs, attempts, rows.Err()
}

func leaseDecision(authority AttemptAuthority, job leaseJob, attempt leaseAttempt, now time.Time) string {
	if authority != attempt.authority {
		return "FENCED"
	}
	if !slices.Contains([]string{"ASSIGNED", "STARTING", "RUNNING", "FINALIZING"}, attempt.state) {
		return "ALREADY_TERMINAL"
	}
	if job.current == nil || *job.current != authority.AttemptID || !attempt.expires.After(now) {
		return "FENCED"
	}
	if job.cancelled || !attempt.phase.After(now) {
		return "STOP_REQUESTED"
	}
	return "ACCEPTED"
}

func decodeLeaseReplay(body []byte, authorities []AttemptAuthority) (map[string]LeaseGrant, error) {
	var grants []LeaseGrant
	if json.Unmarshal(body, &grants) != nil || len(grants) != len(authorities) {
		return nil, ErrInvalid
	}
	saved := map[string]LeaseGrant{}
	for _, g := range grants {
		if _, duplicate := saved[g.Authority.AttemptID]; duplicate {
			return nil, ErrInvalid
		}
		if !slices.Contains([]string{"ACCEPTED", "FENCED", "STOP_REQUESTED", "ALREADY_TERMINAL"}, g.Decision) || g.ServerTime.IsZero() {
			return nil, ErrInvalid
		}
		if g.Decision == "ACCEPTED" {
			if !g.LeaseExpiresAt.After(g.ServerTime) || g.LeaseExpiresAt.Sub(g.ServerTime) > InitialLease || !g.PhaseDeadline.After(g.ServerTime) {
				return nil, ErrInvalid
			}
		} else if !g.LeaseExpiresAt.IsZero() || !g.PhaseDeadline.IsZero() {
			return nil, ErrInvalid
		}
		saved[g.Authority.AttemptID] = g
	}
	for _, a := range authorities {
		if saved[a.AttemptID].Authority != a {
			return nil, ErrInvalid
		}
	}
	return saved, nil
}

// RenewLeases commits authority and its original replay grant together. Rejected
// members do not prevent other valid members from renewing in the same batch.
func RenewLeases(ctx context.Context, pool *pgxpool.Pool, id WorkerIdentity, r LeaseRenewal) ([]LeaseGrant, error) {
	if !canonicalUUID(id.WorkerID) || !canonicalUUID(id.CredentialID) {
		return nil, ErrUnauthorized
	}
	hash, err := r.hash(id.WorkerID)
	if err != nil {
		return nil, err
	}
	// Renewal changes no resource accounting, so it must stay independent of the
	// scheduler cluster lock. All paths lock sorted jobs before sorted attempts.
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='4s'"); err != nil {
		return nil, err
	}
	if err = authorizeWorkerTx(ctx, tx, id); err != nil {
		return nil, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.SessionID); err != nil {
		return nil, err
	}
	// Insert before ownership locks to serialize duplicate batches, including
	// batches whose attempts do not exist. Empty results never commit.
	insert, err := tx.Exec(ctx, `INSERT INTO worker_lease_requests(worker_id,session_id,request_id,request_hash,results) VALUES($1,$2,$3,$4,'[]') ON CONFLICT DO NOTHING`, id.WorkerID, r.SessionID, r.RequestID, hash)
	if err != nil {
		return nil, err
	}
	replay := insert.RowsAffected() == 0
	var saved map[string]LeaseGrant
	if replay {
		var oldHash string
		var body []byte
		if err = tx.QueryRow(ctx, `SELECT request_hash,results FROM worker_lease_requests WHERE worker_id=$1 AND session_id=$2 AND request_id=$3`, id.WorkerID, r.SessionID, r.RequestID).Scan(&oldHash, &body); err != nil {
			return nil, err
		}
		if oldHash != hash {
			return nil, ErrConflict
		}
		if saved, err = decodeLeaseReplay(body, r.Attempts); err != nil {
			return nil, err
		}
	}
	jobs, attempts, err := lockLeaseBatch(ctx, tx, r.Attempts)
	if err != nil {
		return nil, err
	}
	// Recovery locks every live attempt's job before fencing its session. Holding
	// those jobs makes this recheck stable for any authority we can grant, without
	// taking a worker lock that would serialize independent renewal batches.
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.SessionID); err != nil {
		return nil, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return nil, err
	}
	grants := make([]LeaseGrant, 0, len(r.Attempts))
	for _, a := range r.Attempts {
		attempt := attempts[a.AttemptID]
		g := LeaseGrant{Authority: a, ServerTime: now}
		g.Decision = leaseDecision(a, jobs[a.JobID], attempt, now)
		if replay {
			old := saved[a.AttemptID]
			if old.Decision != "ACCEPTED" {
				g.Decision = old.Decision
			}
			if g.Decision == "ACCEPTED" {
				// Later renewals or phase transitions cannot lend authority to this
				// older operation. Retain the stricter original/current bounds.
				g.LeaseExpiresAt = old.LeaseExpiresAt
				if attempt.expires.Before(g.LeaseExpiresAt) {
					g.LeaseExpiresAt = attempt.expires
				}
				g.PhaseDeadline = old.PhaseDeadline
				if attempt.phase.Before(g.PhaseDeadline) {
					g.PhaseDeadline = attempt.phase
				}
				if !g.LeaseExpiresAt.After(now) {
					g.Decision = "FENCED"
				} else if !g.PhaseDeadline.After(now) {
					g.Decision = "STOP_REQUESTED"
				}
			}
		} else if g.Decision == "ACCEPTED" {
			g.LeaseExpiresAt = now.Add(InitialLease)
			g.PhaseDeadline = attempt.phase
			if _, err = tx.Exec(ctx, "UPDATE attempts SET lease_expires_at=$2 WHERE id=$1", a.AttemptID, g.LeaseExpiresAt); err != nil {
				return nil, err
			}
		}
		if g.Decision != "ACCEPTED" {
			g.LeaseExpiresAt = time.Time{}
			g.PhaseDeadline = time.Time{}
		}
		grants = append(grants, g)
	}
	if !replay {
		body, err := json.Marshal(grants)
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `UPDATE worker_lease_requests SET results=$4 WHERE worker_id=$1 AND session_id=$2 AND request_id=$3`, id.WorkerID, r.SessionID, r.RequestID, body); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return grants, nil
}
