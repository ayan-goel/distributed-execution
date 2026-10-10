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

type MultipartCompletionPart struct {
	Number int    `json:"number"`
	ETag   string `json:"etag"`
	SHA256 string `json:"sha256"`
}

type MultipartCompletionRequest struct {
	Authority AttemptAuthority          `json:"authority"`
	RequestID string                    `json:"-"`
	UploadID  string                    `json:"uploadId"`
	Parts     []MultipartCompletionPart `json:"parts"`
}

type MultipartCompletionResult struct {
	Decision, State, Version string
	Upload                   *UploadRecord
}

func validMultipartValue(value string) bool {
	return len(value) > 0 && len(value) <= 1024 && strings.IndexFunc(value, func(c rune) bool { return c < 33 || c > 126 }) == -1
}

func (r MultipartCompletionRequest) hash(worker string) (string, error) {
	a := r.Authority
	if !canonicalUUID(r.RequestID) || !canonicalUUID(r.UploadID) || !canonicalUUID(a.JobID) || !canonicalUUID(a.AttemptID) || !canonicalUUID(a.SessionID) || a.WorkerID != worker || a.Generation < 1 || len(r.Parts) < 2 || len(r.Parts) > MaxUploadParts {
		return "", ErrInvalid
	}
	for i, p := range r.Parts {
		if p.Number != i+1 || !validMultipartValue(p.ETag) || !hashPattern.MatchString(p.SHA256) {
			return "", ErrInvalid
		}
	}
	body, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	if len(body) > 2<<20 {
		return "", ErrInvalid
	}
	hash := sha256.Sum256(append([]byte("dispatch.worker.v1.CompleteMultipartUpload\n"), body...))
	return hex.EncodeToString(hash[:]), nil
}

// CompleteMultipartUpload commits immutable intent before external storage work,
// then rechecks authority before recording the exact version. Recording storage
// completion alone does not verify bytes, create an artifact, or accept a job.
func CompleteMultipartUpload(ctx context.Context, pool *pgxpool.Pool, id WorkerIdentity, r MultipartCompletionRequest, complete func(context.Context, UploadRecord, []MultipartCompletionPart) (string, error)) (MultipartCompletionResult, error) {
	if !canonicalUUID(id.WorkerID) || !canonicalUUID(id.CredentialID) {
		return MultipartCompletionResult{}, ErrUnauthorized
	}
	// Snapshot caller-owned slices before hashing or passing them across storage
	// work; a callback must not change the intent used by the second transaction.
	r.Parts = slices.Clone(r.Parts)
	hash, err := r.hash(id.WorkerID)
	if err != nil || complete == nil {
		return MultipartCompletionResult{}, ErrInvalid
	}
	result, err := multipartCompletionTx(ctx, pool, id, r, hash, "")
	if err != nil || result.Decision != "ACCEPTED" || result.Version != "" {
		return result, err
	}
	version, err := complete(ctx, *result.Upload, slices.Clone(r.Parts))
	if err != nil {
		return MultipartCompletionResult{}, err
	}
	if !validMultipartValue(version) || version == "null" {
		return MultipartCompletionResult{}, ErrInvalid
	}
	return multipartCompletionTx(ctx, pool, id, r, hash, version)
}

func multipartCompletionTx(ctx context.Context, pool *pgxpool.Pool, id WorkerIdentity, r MultipartCompletionRequest, hash, version string) (MultipartCompletionResult, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return MultipartCompletionResult{}, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='4s'"); err != nil {
		return MultipartCompletionResult{}, err
	}
	if err = authorizeWorkerTx(ctx, tx, id); err != nil {
		return MultipartCompletionResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.Authority.SessionID); err != nil {
		return MultipartCompletionResult{}, err
	}
	jobs, attempts, err := lockLeaseBatch(ctx, tx, []AttemptAuthority{r.Authority})
	if err != nil {
		return MultipartCompletionResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.Authority.SessionID); err != nil {
		return MultipartCompletionResult{}, err
	}
	attempt := attempts[r.Authority.AttemptID]
	if attempt.authority != r.Authority {
		return MultipartCompletionResult{Decision: "FENCED"}, nil
	}
	upload, err := loadMultipartUploadTx(ctx, tx, id, r.Authority, r.UploadID)
	if err != nil {
		return MultipartCompletionResult{}, err
	}
	if upload.PartCount != len(r.Parts) {
		return MultipartCompletionResult{}, ErrInvalid
	}
	body, err := json.Marshal(r.Parts)
	if err != nil {
		return MultipartCompletionResult{}, err
	}
	var boundParts int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM jsonb_array_elements($2::jsonb) AS p(part)
	    JOIN artifact_upload_parts b ON b.upload_id=$1 AND b.part_number=(p.part->>'number')::integer AND b.sha256=p.part->>'sha256'`, r.UploadID, body).Scan(&boundParts); err != nil {
		return MultipartCompletionResult{}, err
	}
	if boundParts != len(r.Parts) {
		return MultipartCompletionResult{}, ErrConflict
	}
	// Ownership locks precede FK/replay locks. One row per upload bounds durable
	// completion history and retains the same part set after a lost response.
	if _, err = tx.Exec(ctx, `INSERT INTO artifact_multipart_completions(upload_id,worker_id,session_id,request_id,request_hash,parts) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, r.UploadID, id.WorkerID, r.Authority.SessionID, r.RequestID, hash, body); err != nil {
		return MultipartCompletionResult{}, err
	}
	var savedRequest, savedHash, savedVersion string
	if err = tx.QueryRow(ctx, "SELECT request_id::text,request_hash,COALESCE(object_version,'') FROM artifact_multipart_completions WHERE upload_id=$1 FOR UPDATE", r.UploadID).Scan(&savedRequest, &savedHash, &savedVersion); errors.Is(err, pgx.ErrNoRows) {
		return MultipartCompletionResult{}, ErrConflict
	} else if err != nil {
		return MultipartCompletionResult{}, err
	}
	if savedRequest != r.RequestID || savedHash != hash {
		return MultipartCompletionResult{}, ErrConflict
	}
	// SQL wall time is sampled after all lock waits, including the transaction
	// following storage I/O. A completion response cannot renew attempt authority.
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return MultipartCompletionResult{}, err
	}
	result := MultipartCompletionResult{Decision: leaseDecision(r.Authority, jobs[r.Authority.JobID], attempt, now), State: attempt.state}
	if result.Decision != "ACCEPTED" {
		return result, nil
	}
	if attempt.state != "FINALIZING" {
		return MultipartCompletionResult{}, ErrConflict
	}
	if version != "" {
		if savedVersion != "" && savedVersion != version {
			return MultipartCompletionResult{}, ErrConflict
		}
		if savedVersion == "" {
			// PREPARED -> STORED binds one exact backend version. Event failure
			// rolls back this transition so retry still uses the durable intent.
			if _, err = tx.Exec(ctx, "UPDATE artifact_multipart_completions SET object_version=$2,stored_at=clock_timestamp() WHERE upload_id=$1", r.UploadID, version); err != nil {
				return MultipartCompletionResult{}, err
			}
			if err = appendMultipartStored(ctx, tx, upload); err != nil {
				return MultipartCompletionResult{}, err
			}
			savedVersion = version
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return MultipartCompletionResult{}, err
	}
	result.Upload = &upload
	result.Version = savedVersion
	return result, nil
}

func appendMultipartStored(ctx context.Context, tx pgx.Tx, u UploadRecord) error {
	var sequence int64
	if err := tx.QueryRow(ctx, "UPDATE jobs SET event_sequence=event_sequence+1 WHERE id=$1 RETURNING event_sequence", u.Authority.JobID).Scan(&sequence); err != nil {
		return err
	}
	body, err := json.Marshal(map[string]string{"uploadId": u.UploadID, "initializationId": u.InitializationID})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO job_events(job_id,sequence,attempt_id,type,payload) VALUES($1,$2,$3,'MULTIPART_STORED',$4)", u.Authority.JobID, sequence, u.Authority.AttemptID, body)
	return err
}
