package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrStaleHeartbeat = errors.New("heartbeat report is older than the latest accepted sequence")

type AttemptAuthority struct {
	JobID      string `json:"jobId"`
	AttemptID  string `json:"attemptId"`
	Generation int64  `json:"generation"`
	WorkerID   string `json:"workerId"`
	SessionID  string `json:"sessionId"`
}
type ExecutionRecord struct {
	Authority   AttemptAuthority `json:"authority"`
	ContainerID string           `json:"containerId"`
	Running     bool             `json:"running"`
}
type HeartbeatReport struct {
	RequestID              string            `json:"-"`
	SessionID              string            `json:"sessionId"`
	Sequence               int64             `json:"sequence"`
	RuntimeHealthy         bool              `json:"runtimeHealthy"`
	DiskPressure           bool              `json:"diskPressure"`
	ReconciliationComplete bool              `json:"reconciliationComplete"`
	Inventory              []ExecutionRecord `json:"inventory"`
}
type HeartbeatResult struct {
	Drain, Reconcile bool
	Stop             []AttemptAuthority
}

func (h HeartbeatReport) hash(workerID string) (string, error) {
	if !canonicalUUID(h.RequestID) || !canonicalUUID(h.SessionID) || h.Sequence < 1 || len(h.Inventory) > 1024 {
		return "", ErrInvalid
	}
	seen := map[string]bool{}
	for _, record := range h.Inventory {
		a := record.Authority
		if a.WorkerID != workerID || !canonicalUUID(a.JobID) || !canonicalUUID(a.AttemptID) || !canonicalUUID(a.SessionID) || a.Generation < 1 || !hashPattern.MatchString(record.ContainerID) || seen[record.ContainerID] {
			return "", ErrInvalid
		}
		seen[record.ContainerID] = true
	}
	h.Inventory = slices.Clone(h.Inventory)
	slices.SortFunc(h.Inventory, func(a, b ExecutionRecord) int { return strings.Compare(a.ContainerID, b.ContainerID) })
	// Enumeration order does not change report identity; normalize an empty
	// inventory too so nil and an empty slice have the same wire meaning.
	if h.Inventory == nil {
		h.Inventory = []ExecutionRecord{}
	}
	body, err := json.Marshal(h)
	if err != nil {
		return "", ErrInvalid
	}
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:]), nil
}

type heartbeatAttempt struct {
	Authority          AttemptAuthority
	State, Reservation string
	LeaseExpiry        time.Time
	Current            *string
	Fenced             *time.Time
	Cancelled          bool
}

func (a heartbeatAttempt) active() bool {
	return slices.Contains([]string{"ASSIGNED", "STARTING", "RUNNING", "FINALIZING"}, a.State)
}
func (a heartbeatAttempt) authorized(session string, now time.Time) bool {
	return a.active() && a.Current != nil && *a.Current == a.Authority.AttemptID && a.Authority.SessionID == session && a.Fenced == nil && a.LeaseExpiry.After(now) && !a.Cancelled
}

func heartbeatAttempts(ctx context.Context, tx pgx.Tx, workerID string, inventory []ExecutionRecord) (map[string]heartbeatAttempt, error) {
	ids := make([]string, 0, len(inventory))
	for _, r := range inventory {
		ids = append(ids, r.Authority.AttemptID)
	}
	rows, err := tx.Query(ctx, `SELECT a.job_id::text,a.id::text,a.generation,a.session_id::text,a.state,a.lease_expires_at,j.current_attempt_id::text,j.cancel_requested,r.state,s.fenced_at
	FROM attempts a JOIN jobs j ON j.id=a.job_id JOIN reservations r ON r.attempt_id=a.id JOIN worker_sessions s ON s.id=a.session_id
	WHERE a.worker_id=$1 AND (a.state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING') OR r.state='quarantined' OR a.id=ANY($2::uuid[]))
	ORDER BY a.job_id,a.id FOR UPDATE OF a,r`, workerID, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	attempts := map[string]heartbeatAttempt{}
	for rows.Next() {
		var a heartbeatAttempt
		a.Authority.WorkerID = workerID
		if err := rows.Scan(&a.Authority.JobID, &a.Authority.AttemptID, &a.Authority.Generation, &a.Authority.SessionID, &a.State, &a.LeaseExpiry, &a.Current, &a.Cancelled, &a.Reservation, &a.Fenced); err != nil {
			return nil, err
		}
		attempts[a.Authority.AttemptID] = a
	}
	return attempts, rows.Err()
}

func RecordHeartbeat(ctx context.Context, pool *pgxpool.Pool, identity WorkerIdentity, h HeartbeatReport) (HeartbeatResult, error) {
	var result HeartbeatResult
	if !canonicalUUID(identity.WorkerID) || !canonicalUUID(identity.CredentialID) {
		return result, ErrUnauthorized
	}
	hash, err := h.hash(identity.WorkerID)
	if err != nil {
		return result, err
	}
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		return result, err
	}
	defer rollback(tx)
	if err := authorizeWorkerTx(ctx, tx, identity); err != nil {
		return result, err
	}
	if _, err := lockWorkerAttempts(ctx, tx, identity.WorkerID); err != nil {
		return result, err
	}
	attempts, err := heartbeatAttempts(ctx, tx, identity.WorkerID, h.Inventory)
	if err != nil {
		return result, err
	}
	var sequence int64
	var lastRequest, lastHash *string
	var oldState string
	var reconciled, drain bool
	err = tx.QueryRow(ctx, `SELECT s.heartbeat_sequence,s.heartbeat_request_id::text,s.heartbeat_hash,w.state,w.reconciliation_complete,w.drain_requested
	FROM workers w JOIN worker_sessions s ON s.worker_id=w.id AND s.id=w.current_session_id WHERE w.id=$1 AND s.id=$2 AND s.fenced_at IS NULL FOR UPDATE OF w,s`, identity.WorkerID, h.SessionID).Scan(&sequence, &lastRequest, &lastHash, &oldState, &reconciled, &drain)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrFenced
	}
	if err != nil {
		return result, err
	}
	if h.Sequence < sequence {
		return result, ErrStaleHeartbeat
	}
	replay := h.Sequence == sequence
	if replay && (lastRequest == nil || lastHash == nil || *lastRequest != h.RequestID || *lastHash != hash) {
		return result, ErrConflict
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return result, err
	}
	stop := map[AttemptAuthority]bool{}
	present := map[string]bool{}
	for _, record := range h.Inventory {
		present[record.Authority.AttemptID] = true
		a, exists := attempts[record.Authority.AttemptID]
		if !exists || a.Authority != record.Authority || !a.authorized(h.SessionID, now) {
			stop[record.Authority] = true
		}
	}
	needsReconcile := !h.ReconciliationComplete
	for _, a := range attempts {
		if a.active() && !a.authorized(h.SessionID, now) {
			stop[a.Authority] = true
		}
		if a.State == "RUNNING" && !present[a.Authority.AttemptID] {
			needsReconcile = true
		}
	}
	if len(stop) > 0 {
		needsReconcile = true
	}
	canRelease := !replay && h.RuntimeHealthy && !needsReconcile
	for _, a := range attempts {
		if a.Reservation != "quarantined" {
			continue
		}
		// Only a new incarnation can attest cleanup of fenced predecessors. Never
		// free this session's own uncertain capacity from an old inventory snapshot.
		if canRelease && a.Authority.SessionID != h.SessionID && a.Fenced != nil && !a.active() {
			if _, err := tx.Exec(ctx, "UPDATE reservations SET state='released' WHERE attempt_id=$1", a.Authority.AttemptID); err != nil {
				return result, err
			}
			if _, err := tx.Exec(ctx, "UPDATE attempts SET cleanup_pending=false WHERE id=$1", a.Authority.AttemptID); err != nil {
				return result, err
			}
		} else {
			needsReconcile = true
		}
	}
	state := oldState
	if !replay {
		reconciled = !needsReconcile && h.RuntimeHealthy
		switch {
		case !h.RuntimeHealthy || h.DiskPressure:
			state = "QUARANTINED"
		case !reconciled:
			state = "REGISTERING"
		case drain:
			state = "DRAINING"
		default:
			state = "READY"
		}
		// Liveness and reconciliation advance together with the report sequence.
		// Duplicate reports cannot refresh a dead agent or undo newer health state.
		if _, err := tx.Exec(ctx, "UPDATE worker_sessions SET heartbeat_sequence=$2,heartbeat_request_id=$3,heartbeat_hash=$4 WHERE id=$1", h.SessionID, h.Sequence, h.RequestID, hash); err != nil {
			return result, err
		}
		if _, err := tx.Exec(ctx, "UPDATE workers SET state=$2,reconciliation_complete=$3,runtime_healthy=$4,disk_pressure=$5,last_heartbeat_at=$6 WHERE id=$1", identity.WorkerID, state, reconciled, h.RuntimeHealthy, h.DiskPressure, now); err != nil {
			return result, err
		}
		if state != oldState {
			if _, err := tx.Exec(ctx, "INSERT INTO worker_audit_events(worker_id,credential_id,action) VALUES($1,$2,$3)", identity.WorkerID, identity.CredentialID, "WORKER_"+state); err != nil {
				return result, err
			}
		}
	}
	result.Reconcile = !reconciled || needsReconcile
	result.Drain = drain || state != "READY"
	for a := range stop {
		result.Stop = append(result.Stop, a)
	}
	slices.SortFunc(result.Stop, func(a, b AttemptAuthority) int {
		if a.AttemptID != b.AttemptID {
			return strings.Compare(a.AttemptID, b.AttemptID)
		}
		if a.SessionID != b.SessionID {
			return strings.Compare(a.SessionID, b.SessionID)
		}
		if a.Generation < b.Generation {
			return -1
		}
		if a.Generation > b.Generation {
			return 1
		}
		return strings.Compare(a.JobID, b.JobID)
	})
	if err := tx.Commit(ctx); err != nil {
		return HeartbeatResult{}, err
	}
	return result, nil
}
