package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const SessionInactivity = 30 * time.Second

// ApproveSessionTakeover is an operator-only database action. The replacement
// UUID is chosen by the new incarnation; possession of host credentials alone
// never authorizes replacing another live incarnation.
func ApproveSessionTakeover(ctx context.Context, pool *pgxpool.Pool, workerID, from, to string) error {
	if !canonicalUUID(workerID) || !canonicalUUID(from) || !canonicalUUID(to) || from == to {
		return ErrInvalid
	}
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var approved string
	err = tx.QueryRow(ctx, "SELECT to_session_id::text FROM worker_takeovers WHERE worker_id=$1 AND from_session_id=$2", workerID, from).Scan(&approved)
	if err == nil {
		if approved != to {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	var current *string
	err = tx.QueryRow(ctx, "SELECT current_session_id::text FROM workers WHERE id=$1", workerID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if current == nil || *current != from {
		return ErrFenced
	}
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM worker_sessions WHERE id=$1)", to).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, "INSERT INTO worker_takeovers(worker_id,from_session_id,to_session_id) VALUES($1,$2,$3)", workerID, from, to); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO worker_audit_events(worker_id,action) VALUES($1,'SESSION_TAKEOVER_APPROVED')", workerID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type recoveryAttempt struct {
	JobID, ID   string
	LeaseExpiry time.Time
}

func lockWorkerAttempts(ctx context.Context, tx pgx.Tx, workerID string) ([]recoveryAttempt, error) {
	// The cluster lock is already held. Lock every affected job in stable order
	// before attempts so renewal/completion races cannot invert ownership locks.
	rows, err := tx.Query(ctx, `SELECT j.id FROM jobs j WHERE EXISTS(SELECT 1 FROM attempts a WHERE a.job_id=j.id AND a.worker_id=$1 AND a.state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING')) ORDER BY j.id FOR UPDATE OF j`, workerID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = tx.Query(ctx, `SELECT job_id::text,id::text,lease_expires_at FROM attempts WHERE worker_id=$1 AND state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING') ORDER BY job_id,id FOR UPDATE`, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var attempts []recoveryAttempt
	for rows.Next() {
		var a recoveryAttempt
		if err := rows.Scan(&a.JobID, &a.ID, &a.LeaseExpiry); err != nil {
			return nil, err
		}
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

func recoverSession(ctx context.Context, tx pgx.Tx, identity WorkerIdentity, from, to string) error {
	attempts, err := lockWorkerAttempts(ctx, tx, identity.WorkerID)
	if err != nil {
		return err
	}
	var heartbeat, fenced *time.Time
	var created time.Time
	if err = tx.QueryRow(ctx, `SELECT w.last_heartbeat_at,s.created_at,s.fenced_at FROM workers w JOIN worker_sessions s ON s.worker_id=w.id AND s.id=w.current_session_id WHERE w.id=$1 AND s.id=$2 FOR UPDATE OF w`, identity.WorkerID, from).Scan(&heartbeat, &created, &fenced); err != nil {
		return err
	}
	var now time.Time
	// Read wall time only after all job, attempt, and worker locks are acquired.
	// Transaction-start time can be stale after waiting on a renewal transaction.
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return err
	}
	var approved bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM worker_takeovers WHERE worker_id=$1 AND from_session_id=$2 AND to_session_id=$3 AND used_at IS NULL)", identity.WorkerID, from, to).Scan(&approved); err != nil {
		return err
	}
	if !approved && fenced == nil {
		lastSeen := created
		if heartbeat != nil {
			lastSeen = *heartbeat
		}
		if now.Sub(lastSeen) < SessionInactivity {
			return ErrSessionActive
		}
		for _, a := range attempts {
			if a.LeaseExpiry.After(now) {
				return ErrSessionActive
			}
		}
	}
	for _, a := range attempts {
		if err := fenceLostAttempt(ctx, tx, a, now); err != nil {
			return err
		}
	}
	// Fencing is permanent, but physical execution is still uncertain. The new
	// session stays REGISTERING while old reservations remain quarantined.
	if _, err = tx.Exec(ctx, "UPDATE worker_sessions SET fenced_at=$2 WHERE worker_id=$1 AND fenced_at IS NULL", identity.WorkerID, now); err != nil {
		return err
	}
	if approved {
		if _, err = tx.Exec(ctx, "UPDATE worker_takeovers SET used_at=$4 WHERE worker_id=$1 AND from_session_id=$2 AND to_session_id=$3", identity.WorkerID, from, to, now); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, "INSERT INTO worker_audit_events(worker_id,credential_id,action) VALUES($1,$2,'SESSION_FENCED')", identity.WorkerID, identity.CredentialID)
	return err
}

func fenceLostAttempt(ctx context.Context, tx pgx.Tx, a recoveryAttempt, now time.Time) error {
	var body []byte
	var cancelled bool
	var number int
	err := tx.QueryRow(ctx, "SELECT spec,cancel_requested,attempt_counter FROM jobs WHERE id=$1 AND current_attempt_id=$2", a.JobID, a.ID).Scan(&body, &cancelled, &number)
	if err != nil {
		return err
	}
	var job spec.Job
	if err = json.Unmarshal(body, &job); err != nil {
		return err
	}
	if err = job.Validate(); err != nil {
		return err
	}
	jobState, attemptState, reason, event := "FAILED", "LOST", "WORKER_LOST", "ATTEMPT_LOST"
	nextEligible := now
	if cancelled {
		jobState, attemptState, reason, event = "CANCELLED", "CANCELLED", "USER_CANCELLED", "ATTEMPT_CANCELLED"
	} else if number < job.Spec.Retry.MaxAttempts && slices.Contains(job.Spec.Retry.On, "WORKER_LOST") {
		jobState = "RETRY_WAIT"
		nextEligible = now.Add(retryDelay(job.Spec.Retry, number, a.ID))
	}
	if _, err = tx.Exec(ctx, "UPDATE attempts SET state=$2,reason=$3,cleanup_pending=true,finished_at=$4 WHERE id=$1", a.ID, attemptState, reason, now); err != nil {
		return err
	}
	changed, err := tx.Exec(ctx, "UPDATE reservations SET state='quarantined' WHERE attempt_id=$1 AND state='active'", a.ID)
	if err != nil {
		return err
	}
	if changed.RowsAffected() != 1 {
		return errors.New("active attempt lacks active reservation")
	}
	var sequence int64
	if err = tx.QueryRow(ctx, "UPDATE jobs SET state=$2,current_attempt_id=NULL,next_eligible_at=$3,event_sequence=event_sequence+1 WHERE id=$1 RETURNING event_sequence", a.JobID, jobState, nextEligible).Scan(&sequence); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{"reason": reason, "nextState": jobState, "cleanupPending": true, "nextEligibleAt": nextEligible})
	_, err = tx.Exec(ctx, "INSERT INTO job_events(job_id,sequence,attempt_id,type,payload) VALUES($1,$2,$3,$4,$5)", a.JobID, sequence, a.ID, event, payload)
	return err
}

func retryDelay(policy spec.Retry, attemptNumber int, attemptID string) time.Duration {
	base := time.Duration(policy.InitialBackoffSeconds) * time.Second
	maximum := time.Duration(policy.MaxBackoffSeconds) * time.Second
	for n := 1; n < attemptNumber && base < maximum; n++ {
		base = min(base*2, maximum)
	}
	// Equal jitter spreads simultaneous host-loss retries across [base/2,base].
	// Deriving it from the immutable attempt ID makes transaction retries stable.
	hash := sha256.Sum256([]byte(attemptID))
	half := base / 2
	return half + time.Duration(binary.BigEndian.Uint64(hash[:8])%uint64(base-half+1))
}
