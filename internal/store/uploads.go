package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxUploadBytes int64 = 64 << 20
const MaxAttemptUploadCount = 1024
const MaxAttemptUploadBytes int64 = 8 << 30

var ErrUploadLimit = errors.New("attempt upload limit exceeded")
var uploadNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

type UploadRequest struct {
	Authority   AttemptAuthority `json:"authority"`
	RequestID   string           `json:"-"`
	Kind        string           `json:"kind"`
	LogicalName string           `json:"logicalName"`
	SizeBytes   int64            `json:"sizeBytes"`
	SHA256      string           `json:"sha256"`
	PartCount   int              `json:"partCount"`
}
type UploadRecord struct {
	UploadID, ProjectID, ObjectKey string
	Authority                      AttemptAuthority
	Kind, LogicalName, SHA256      string
	SizeBytes                      int64
	PartCount                      int
	CreatedAt                      time.Time
}
type UploadResult struct {
	Decision, State                           string
	Upload                                    *UploadRecord
	ServerTime, LeaseExpiresAt, PhaseDeadline time.Time
}

func (r UploadRequest) hash(workerID string) (string, error) {
	a := r.Authority
	if !canonicalUUID(r.RequestID) || !canonicalUUID(a.JobID) || !canonicalUUID(a.AttemptID) || !canonicalUUID(a.SessionID) || a.WorkerID != workerID || a.Generation < 1 || !uploadNamePattern.MatchString(r.LogicalName) || !slices.Contains([]string{"OUTPUT", "LOG", "MANIFEST"}, r.Kind) || r.SizeBytes < 0 || r.SizeBytes > MaxUploadBytes || !hashPattern.MatchString(r.SHA256) || r.PartCount != 1 {
		return "", ErrInvalid
	}
	body, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("dispatch.worker.v1.CreateUpload\n"), body...))
	return hex.EncodeToString(sum[:]), nil
}

func validateUploadDeclaration(r UploadRequest, job spec.Job, state string) error {
	if r.Kind == "LOG" {
		if !slices.Contains([]string{"STARTING", "RUNNING", "FINALIZING"}, state) {
			return ErrConflict
		}
		if r.LogicalName != "stdout" && r.LogicalName != "stderr" {
			return ErrInvalid
		}
		return nil
	}
	if state != "FINALIZING" {
		return ErrConflict
	}
	if r.Kind == "MANIFEST" {
		if r.LogicalName != "result" || r.SizeBytes > 1<<20 {
			return ErrInvalid
		}
		return nil
	}
	for _, output := range job.Spec.Outputs {
		if r.LogicalName == output.Name && r.SizeBytes <= output.MaxBytes {
			return nil
		}
	}
	return ErrInvalid
}

// CreateUpload persists a bounded declaration before any storage capability is
// signed. It does not perform network I/O, renew authority, or accept a result.
func CreateUpload(ctx context.Context, pool *pgxpool.Pool, id WorkerIdentity, r UploadRequest) (UploadResult, error) {
	if !canonicalUUID(id.WorkerID) || !canonicalUUID(id.CredentialID) {
		return UploadResult{}, ErrUnauthorized
	}
	hash, err := r.hash(id.WorkerID)
	if err != nil {
		return UploadResult{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return UploadResult{}, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='4s'"); err != nil {
		return UploadResult{}, err
	}
	if err = authorizeWorkerTx(ctx, tx, id); err != nil {
		return UploadResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.Authority.SessionID); err != nil {
		return UploadResult{}, err
	}
	// Upload accounting is per attempt and changes no scheduler reservation.
	// Keep the established credential/job/attempt lock order without cluster locks.
	jobs, attempts, err := lockLeaseBatch(ctx, tx, []AttemptAuthority{r.Authority})
	if err != nil {
		return UploadResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.Authority.SessionID); err != nil {
		return UploadResult{}, err
	}
	attempt := attempts[r.Authority.AttemptID]
	if attempt.authority != r.Authority {
		return UploadResult{Decision: "FENCED"}, nil
	}
	var projectID string
	var body []byte
	if err = tx.QueryRow(ctx, "SELECT project_id::text,spec FROM jobs WHERE id=$1", r.Authority.JobID).Scan(&projectID, &body); err != nil {
		return UploadResult{}, err
	}
	var job spec.Job
	if json.Unmarshal(body, &job) != nil || job.Validate() != nil {
		return UploadResult{}, ErrInvalid
	}
	uploadID, err := uuid.NewRandom()
	if err != nil {
		return UploadResult{}, err
	}
	// Claim only after ownership locks: upload foreign keys must not lock an
	// attempt ahead of its job. Equal cross-attempt request IDs serialize here.
	inserted, err := tx.Exec(ctx, `INSERT INTO artifact_uploads(upload_id,project_id,job_id,attempt_id,worker_id,session_id,generation,request_id,request_hash,kind,logical_name,size_bytes,sha256,part_count)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT(worker_id,session_id,request_id) DO NOTHING`, uploadID.String(), projectID, r.Authority.JobID, r.Authority.AttemptID, id.WorkerID, r.Authority.SessionID, r.Authority.Generation, r.RequestID, hash, r.Kind, r.LogicalName, r.SizeBytes, r.SHA256, r.PartCount)
	if err != nil {
		return UploadResult{}, err
	}
	record, oldHash, err := readUpload(ctx, tx, id.WorkerID, r.Authority.SessionID, r.RequestID)
	if err != nil {
		return UploadResult{}, err
	}
	if hash != oldHash {
		return UploadResult{}, ErrConflict
	}
	saved := UploadRequest{Authority: record.Authority, RequestID: r.RequestID, Kind: record.Kind, LogicalName: record.LogicalName, SizeBytes: record.SizeBytes, SHA256: record.SHA256, PartCount: record.PartCount}
	savedHash, err := saved.hash(id.WorkerID)
	if err != nil || savedHash != oldHash || record.ProjectID != projectID {
		return UploadResult{}, ErrInvalid
	}
	// Sample after every ownership/replay lock wait. Transaction-start time or
	// the original request's grant cannot authorize an expired upload replay.
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return UploadResult{}, err
	}
	result := UploadResult{Decision: leaseDecision(r.Authority, jobs[r.Authority.JobID], attempt, now), State: attempt.state, ServerTime: now}
	if result.Decision != "ACCEPTED" {
		return result, nil
	}
	if err = validateUploadDeclaration(r, job, attempt.state); err != nil {
		return UploadResult{}, err
	}
	if inserted.RowsAffected() != 0 {
		var count, bytes int64
		if err = tx.QueryRow(ctx, "SELECT count(*),COALESCE(sum(size_bytes),0)::bigint FROM artifact_uploads WHERE attempt_id=$1", r.Authority.AttemptID).Scan(&count, &bytes); err != nil {
			return UploadResult{}, err
		}
		// Count bounds zero-byte floods; byte bounds include all pending and
		// verified declarations. The attempt lock prevents concurrent oversubscription.
		if count > MaxAttemptUploadCount || bytes > MaxAttemptUploadBytes {
			return UploadResult{}, ErrUploadLimit
		}
		if err = appendUploadEvent(ctx, tx, record); err != nil {
			return UploadResult{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return UploadResult{}, err
	}
	result.Upload = &record
	result.LeaseExpiresAt = attempt.expires
	result.PhaseDeadline = attempt.phase
	return result, nil
}

func readUpload(ctx context.Context, tx pgx.Tx, worker, session, request string) (UploadRecord, string, error) {
	var r UploadRecord
	var hash string
	err := tx.QueryRow(ctx, `SELECT upload_id::text,project_id::text,object_key,job_id::text,attempt_id::text,worker_id::text,session_id::text,generation,kind,logical_name,size_bytes,sha256,part_count,created_at,request_hash FROM artifact_uploads WHERE worker_id=$1 AND session_id=$2 AND request_id=$3`, worker, session, request).Scan(&r.UploadID, &r.ProjectID, &r.ObjectKey, &r.Authority.JobID, &r.Authority.AttemptID, &r.Authority.WorkerID, &r.Authority.SessionID, &r.Authority.Generation, &r.Kind, &r.LogicalName, &r.SizeBytes, &r.SHA256, &r.PartCount, &r.CreatedAt, &hash)
	return r, hash, err
}

func appendUploadEvent(ctx context.Context, tx pgx.Tx, r UploadRecord) error {
	var sequence int64
	if err := tx.QueryRow(ctx, "UPDATE jobs SET event_sequence=event_sequence+1 WHERE id=$1 RETURNING event_sequence", r.Authority.JobID).Scan(&sequence); err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{"uploadId": r.UploadID, "kind": r.Kind, "logicalName": r.LogicalName, "sizeBytes": r.SizeBytes, "sha256": r.SHA256})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO job_events(job_id,sequence,attempt_id,type,payload) VALUES($1,$2,$3,'UPLOAD_CREATED',$4)", r.Authority.JobID, sequence, r.Authority.AttemptID, body)
	return err
}
