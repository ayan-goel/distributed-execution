package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrSessionActive = errors.New("worker already has an active session")
	ErrFenced        = errors.New("worker session or attempt is fenced")
)

type Registration struct {
	RequestID       string            `json:"-"`
	SessionID       string            `json:"sessionId"`
	ProtocolVersion uint32            `json:"protocolVersion"`
	Resources       spec.Resources    `json:"resources"`
	Slots           int               `json:"slots"`
	Labels          map[string]string `json:"labels"`
	Capabilities    []string          `json:"capabilities"`
}

type Session struct {
	WorkerID        string
	SessionID       string
	Generation      int64
	CleanupRequired bool
}

func canonicalUUID(id string) bool {
	v, err := uuid.Parse(id)
	return err == nil && v != uuid.Nil && v.String() == id
}

func (r Registration) hash() (string, error) {
	if !canonicalUUID(r.RequestID) || !canonicalUUID(r.SessionID) || r.ProtocolVersion != 1 {
		return "", ErrInvalid
	}
	if err := validateWorkerConfiguration(r.Resources, r.Slots, r.Labels); err != nil {
		return "", err
	}
	if len(r.Capabilities) < 5 || len(r.Capabilities) > 6 {
		return "", ErrInvalid
	}
	caps := map[string]bool{}
	for _, capability := range r.Capabilities {
		if caps[capability] || !slices.Contains([]string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.soft", "scratch.quota"}, capability) {
			return "", ErrInvalid
		}
		caps[capability] = true
	}
	if !caps["docker.v1"] || !caps["cpu.hard"] || !caps["memory.hard"] || !caps["pids.hard"] || caps["scratch.soft"] == caps["scratch.quota"] {
		return "", ErrInvalid
	}
	r.Capabilities = slices.Clone(r.Capabilities)
	slices.Sort(r.Capabilities)
	body, err := json.Marshal(r)
	if err != nil {
		return "", ErrInvalid
	}
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:]), nil
}

func beginTransition(ctx context.Context, pool *pgxpool.Pool) (pgx.Tx, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='4s'"); err != nil {
		rollback(tx)
		return nil, err
	}
	// All reservation-changing paths take this cluster lock first, then sorted
	// jobs, attempts, and worker/accounting rows. Lease renewal must never take it
	// after locking a job. No external I/O belongs inside this transaction.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(1146310734)"); err != nil {
		rollback(tx)
		return nil, err
	}
	return tx, nil
}

func authorizeWorkerTx(ctx context.Context, tx pgx.Tx, identity WorkerIdentity) error {
	var found bool
	// Recheck under a credential lock so revocation cannot commit between this
	// authorization check and a new session becoming authoritative.
	err := tx.QueryRow(ctx, "SELECT true FROM worker_credentials WHERE worker_id=$1 AND id=$2 AND revoked_at IS NULL FOR SHARE", identity.WorkerID, identity.CredentialID).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrUnauthorized
	}
	return err
}

func RegisterSession(ctx context.Context, pool *pgxpool.Pool, identity WorkerIdentity, r Registration) (result Session, err error) {
	hash, err := r.hash()
	if err != nil {
		return result, err
	}
	if !canonicalUUID(identity.WorkerID) || !canonicalUUID(identity.CredentialID) {
		return result, ErrUnauthorized
	}
	defer func() {
		var dbErr *pgconn.PgError
		if errors.As(err, &dbErr) && dbErr.Code == "23505" {
			result = Session{}
			err = ErrConflict
		}
	}()
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		return result, err
	}
	defer rollback(tx)
	if err = authorizeWorkerTx(ctx, tx, identity); err != nil {
		return Session{}, err
	}
	var current *string
	var reconciled bool
	var limit spec.Resources
	var slots int
	var labelsJSON []byte
	err = tx.QueryRow(ctx, `SELECT w.current_session_id::text,w.reconciliation_complete,a.cpu_limit,a.memory_limit_mib,a.scratch_limit_mib,a.slot_limit,a.labels
	FROM workers w JOIN worker_authorizations a ON a.worker_id=w.id WHERE w.id=$1`, identity.WorkerID).Scan(&current, &reconciled, &limit.CPUMillis, &limit.MemoryMiB, &limit.ScratchMiB, &slots, &labelsJSON)
	if err != nil {
		return Session{}, err
	}
	var labels map[string]string
	if json.Unmarshal(labelsJSON, &labels) != nil {
		return Session{}, ErrInvalid
	}
	if r.Resources.CPUMillis > limit.CPUMillis || r.Resources.MemoryMiB > limit.MemoryMiB || r.Resources.ScratchMiB > limit.ScratchMiB || r.Slots > slots || !maps.Equal(r.Labels, labels) {
		return Session{}, ErrInvalid
	}
	var savedSession, savedHash string
	err = tx.QueryRow(ctx, "SELECT session_id::text,request_hash FROM worker_registrations WHERE worker_id=$1 AND request_id=$2", identity.WorkerID, r.RequestID).Scan(&savedSession, &savedHash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Session{}, err
	}
	if err == nil && (savedSession != r.SessionID || savedHash != hash) {
		return Session{}, ErrConflict
	}
	var generation int64
	var fenced *time.Time
	var sessionHash *string
	err = tx.QueryRow(ctx, "SELECT generation,fenced_at,registration_hash FROM worker_sessions WHERE worker_id=$1 AND id=$2", identity.WorkerID, r.SessionID).Scan(&generation, &fenced, &sessionHash)
	if err == nil {
		if fenced != nil || current == nil || *current != r.SessionID {
			return Session{}, ErrFenced
		}
		if sessionHash == nil || *sessionHash != hash {
			return Session{}, ErrConflict
		}
		if err = recordRegistration(ctx, tx, identity.WorkerID, r, hash); err != nil {
			return Session{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return Session{}, err
		}
		return Session{identity.WorkerID, r.SessionID, generation, !reconciled}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Session{}, err
	}
	if current != nil {
		if err = recoverSession(ctx, tx, identity, *current, r.SessionID); err != nil {
			return Session{}, err
		}
	}
	if err = tx.QueryRow(ctx, "SELECT COALESCE(max(generation),0)+1 FROM worker_sessions WHERE worker_id=$1", identity.WorkerID).Scan(&generation); err != nil {
		return Session{}, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO worker_sessions(id,worker_id,generation,registration_hash) VALUES($1,$2,$3,$4)", r.SessionID, identity.WorkerID, generation, hash); err != nil {
		return Session{}, err
	}
	caps := map[string]bool{}
	for _, capability := range r.Capabilities {
		caps[capability] = true
	}
	capsJSON, _ := json.Marshal(caps)
	// Registration creates session identity, not execution permission. Capacity
	// remains ineligible until the new agent proves local cleanup by heartbeat.
	if _, err = tx.Exec(ctx, `UPDATE workers SET current_session_id=$2,state='REGISTERING',reconciliation_complete=false,last_heartbeat_at=clock_timestamp(),
	cpu_millis=$3,memory_mib=$4,scratch_mib=$5,slots=$6,capabilities=$7 WHERE id=$1`, identity.WorkerID, r.SessionID, r.Resources.CPUMillis, r.Resources.MemoryMiB, r.Resources.ScratchMiB, r.Slots, capsJSON); err != nil {
		return Session{}, err
	}
	if err = recordRegistration(ctx, tx, identity.WorkerID, r, hash); err != nil {
		return Session{}, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO worker_audit_events(worker_id,credential_id,action) VALUES($1,$2,'SESSION_REGISTERED')", identity.WorkerID, identity.CredentialID); err != nil {
		return Session{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Session{}, err
	}
	return Session{identity.WorkerID, r.SessionID, generation, true}, nil
}

func recordRegistration(ctx context.Context, tx pgx.Tx, workerID string, r Registration, hash string) error {
	_, err := tx.Exec(ctx, `INSERT INTO worker_registrations(worker_id,request_id,session_id,request_hash) VALUES($1,$2,$3,$4) ON CONFLICT(worker_id,request_id) DO NOTHING`, workerID, r.RequestID, r.SessionID, hash)
	return err
}
