package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MultipartPartRequest struct {
	Authority AttemptAuthority
	UploadID  string
	Number    int
	SHA256    string
}

// PrepareMultipartPart binds content before a signed part grant is issued. The
// natural replay identity is upload/part number; retries may not change its hash.
func PrepareMultipartPart(ctx context.Context, pool *pgxpool.Pool, id WorkerIdentity, r MultipartPartRequest) (UploadResult, error) {
	if !canonicalUUID(id.WorkerID) || !canonicalUUID(id.CredentialID) {
		return UploadResult{}, ErrUnauthorized
	}
	a := r.Authority
	if !canonicalUUID(r.UploadID) || !canonicalUUID(a.JobID) || !canonicalUUID(a.AttemptID) || !canonicalUUID(a.SessionID) || a.WorkerID != id.WorkerID || a.Generation < 1 || r.Number < 1 || r.Number > MaxUploadParts || !hashPattern.MatchString(r.SHA256) {
		return UploadResult{}, ErrInvalid
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
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, a.SessionID); err != nil {
		return UploadResult{}, err
	}
	jobs, attempts, err := lockLeaseBatch(ctx, tx, []AttemptAuthority{a})
	if err != nil {
		return UploadResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, a.SessionID); err != nil {
		return UploadResult{}, err
	}
	attempt := attempts[a.AttemptID]
	if attempt.authority != a {
		return UploadResult{Decision: "FENCED"}, nil
	}
	upload, err := loadMultipartUploadTx(ctx, tx, id, a, r.UploadID)
	if err != nil {
		return UploadResult{}, err
	}
	if r.Number > upload.PartCount {
		return UploadResult{}, ErrInvalid
	}
	if _, err = tx.Exec(ctx, "INSERT INTO artifact_upload_parts(upload_id,part_number,sha256) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", r.UploadID, r.Number, r.SHA256); err != nil {
		return UploadResult{}, err
	}
	var savedHash string
	if err = tx.QueryRow(ctx, "SELECT sha256 FROM artifact_upload_parts WHERE upload_id=$1 AND part_number=$2 FOR UPDATE", r.UploadID, r.Number).Scan(&savedHash); err != nil {
		return UploadResult{}, err
	}
	if savedHash != r.SHA256 {
		return UploadResult{}, ErrConflict
	}
	// Fresh wall time follows all ownership and part replay locks. Historical
	// content evidence cannot renew a lease or authorize a cancelled attempt.
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return UploadResult{}, err
	}
	result := UploadResult{Decision: leaseDecision(a, jobs[a.JobID], attempt, now), State: attempt.state, ServerTime: now}
	if result.Decision != "ACCEPTED" {
		return result, nil
	}
	if attempt.state != "FINALIZING" {
		return UploadResult{}, ErrConflict
	}
	var stored bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM artifact_multipart_completions WHERE upload_id=$1 AND object_version IS NOT NULL)", r.UploadID).Scan(&stored); err != nil {
		return UploadResult{}, err
	}
	if stored {
		return UploadResult{}, ErrConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return UploadResult{}, err
	}
	result.Upload = &upload
	result.LeaseExpiresAt = attempt.expires
	result.PhaseDeadline = attempt.phase
	return result, nil
}

// Caller holds credential/job/attempt locks before taking the multipart row lock.
// Shared loading keeps part grants and completion bound to the same declaration.
func loadMultipartUploadTx(ctx context.Context, tx pgx.Tx, id WorkerIdentity, a AttemptAuthority, uploadID string) (UploadRecord, error) {
	var createRequest string
	err := tx.QueryRow(ctx, "SELECT request_id::text FROM artifact_uploads WHERE upload_id=$1 AND worker_id=$2 AND session_id=$3", uploadID, id.WorkerID, a.SessionID).Scan(&createRequest)
	if errors.Is(err, pgx.ErrNoRows) {
		return UploadRecord{}, ErrNotFound
	}
	if err != nil {
		return UploadRecord{}, err
	}
	upload, _, err := readUpload(ctx, tx, id.WorkerID, a.SessionID, createRequest)
	if err != nil {
		return UploadRecord{}, err
	}
	if upload.Authority != a || upload.PartCount < 2 {
		return UploadRecord{}, ErrInvalid
	}
	if err = tx.QueryRow(ctx, "SELECT initialization_id::text,COALESCE(backend_upload_id,'') FROM artifact_multipart_uploads WHERE upload_id=$1 FOR UPDATE", uploadID).Scan(&upload.InitializationID, &upload.BackendUploadID); err != nil {
		return UploadRecord{}, err
	}
	if upload.BackendUploadID == "" {
		return UploadRecord{}, ErrConflict
	}
	return upload, nil
}
