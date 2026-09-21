package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxAttemptFinalizations = 1024

var artifactKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$`)

type ArtifactObject struct {
	Key       string `json:"key"`
	Version   string `json:"version"`
	SizeBytes int64  `json:"sizeBytes"`
	SHA256    string `json:"sha256"`
}

type FinalizeUploadRequest struct {
	Authority AttemptAuthority `json:"authority"`
	RequestID string           `json:"-"`
	UploadID  string           `json:"uploadId"`
	Object    ArtifactObject   `json:"object"`
}

type ArtifactRecord struct {
	ArtifactID, UploadID string
	Object               ArtifactObject
	VerifiedAt           time.Time
}

type FinalizeUploadResult struct {
	Decision, State string
	Artifact        *ArtifactRecord
}

func (r FinalizeUploadRequest) hash(worker string) (string, error) {
	a, o := r.Authority, r.Object
	if !canonicalUUID(r.RequestID) || !canonicalUUID(r.UploadID) || !canonicalUUID(a.JobID) || !canonicalUUID(a.AttemptID) || !canonicalUUID(a.SessionID) || a.WorkerID != worker || a.Generation < 1 || len(o.Key) > 1024 || !artifactKeyPattern.MatchString(o.Key) || o.Version == "" || o.Version == "null" || len(o.Version) > 1024 || strings.IndexFunc(o.Version, func(c rune) bool { return c < 33 || c > 126 }) != -1 || o.SizeBytes < 0 || o.SizeBytes > MaxUploadBytes || !hashPattern.MatchString(o.SHA256) {
		return "", ErrInvalid
	}
	body, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(append([]byte("dispatch.worker.v1.FinalizeUpload\n"), body...))
	return hex.EncodeToString(hash[:]), nil
}

// FinalizeUpload invokes the trusted verifier with the exact declared object only
// after preflight commits. No database locks survive into verification. A second
// transaction must still authorize ownership before recording the verified bytes.
func FinalizeUpload(ctx context.Context, pool *pgxpool.Pool, id WorkerIdentity, r FinalizeUploadRequest, verify func(context.Context, ArtifactObject) error) (FinalizeUploadResult, error) {
	if !canonicalUUID(id.WorkerID) || !canonicalUUID(id.CredentialID) {
		return FinalizeUploadResult{}, ErrUnauthorized
	}
	hash, err := r.hash(id.WorkerID)
	if err != nil || verify == nil {
		return FinalizeUploadResult{}, ErrInvalid
	}
	result, err := finalizeUploadTx(ctx, pool, id, r, hash, false)
	if err != nil || result.Decision != "ACCEPTED" || result.Artifact != nil {
		return result, err
	}
	if err := verify(ctx, r.Object); err != nil {
		return FinalizeUploadResult{}, err
	}
	return finalizeUploadTx(ctx, pool, id, r, hash, true)
}

func finalizeUploadTx(ctx context.Context, pool *pgxpool.Pool, id WorkerIdentity, r FinalizeUploadRequest, hash string, verified bool) (FinalizeUploadResult, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return FinalizeUploadResult{}, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='4s'"); err != nil {
		return FinalizeUploadResult{}, err
	}
	if err = authorizeWorkerTx(ctx, tx, id); err != nil {
		return FinalizeUploadResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.Authority.SessionID); err != nil {
		return FinalizeUploadResult{}, err
	}
	jobs, attempts, err := lockLeaseBatch(ctx, tx, []AttemptAuthority{r.Authority})
	if err != nil {
		return FinalizeUploadResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.Authority.SessionID); err != nil {
		return FinalizeUploadResult{}, err
	}
	attempt := attempts[r.Authority.AttemptID]
	if attempt.authority != r.Authority {
		return FinalizeUploadResult{Decision: "FENCED"}, nil
	}
	var object ArtifactObject
	var kind string
	err = tx.QueryRow(ctx, `SELECT object_key,size_bytes,sha256,kind FROM artifact_uploads WHERE upload_id=$1 AND job_id=$2 AND attempt_id=$3 AND worker_id=$4 AND session_id=$5 AND generation=$6`, r.UploadID, r.Authority.JobID, r.Authority.AttemptID, id.WorkerID, r.Authority.SessionID, r.Authority.Generation).Scan(&object.Key, &object.SizeBytes, &object.SHA256, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return FinalizeUploadResult{}, ErrNotFound
	}
	if err != nil {
		return FinalizeUploadResult{}, err
	}
	object.Version = r.Object.Version
	if object != r.Object {
		return FinalizeUploadResult{}, ErrInvalid
	}
	finalizationID, err := uuid.NewRandom()
	if err != nil {
		return FinalizeUploadResult{}, err
	}
	// Claim replay after ownership locks to keep foreign-key checks in the same
	// job/attempt order. Failed storage verification retains this stable intent.
	inserted, err := tx.Exec(ctx, `INSERT INTO artifact_finalizations(id,upload_id,worker_id,session_id,request_id,request_hash,object_version) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(worker_id,session_id,request_id) DO NOTHING`, finalizationID.String(), r.UploadID, id.WorkerID, r.Authority.SessionID, r.RequestID, hash, r.Object.Version)
	if err != nil {
		return FinalizeUploadResult{}, err
	}
	var savedID, savedHash string
	if err = tx.QueryRow(ctx, "SELECT id::text,request_hash FROM artifact_finalizations WHERE worker_id=$1 AND session_id=$2 AND request_id=$3", id.WorkerID, r.Authority.SessionID, r.RequestID).Scan(&savedID, &savedHash); err != nil {
		return FinalizeUploadResult{}, err
	}
	if savedHash != hash {
		return FinalizeUploadResult{}, ErrConflict
	}
	// SQL wall time must be sampled after replay/ownership lock waits, including
	// the second transaction after a potentially slow object-store read.
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return FinalizeUploadResult{}, err
	}
	result := FinalizeUploadResult{Decision: leaseDecision(r.Authority, jobs[r.Authority.JobID], attempt, now), State: attempt.state}
	if result.Decision != "ACCEPTED" {
		return result, nil
	}
	if kind == "LOG" {
		if !slices.Contains([]string{"STARTING", "RUNNING", "FINALIZING"}, attempt.state) {
			return FinalizeUploadResult{}, ErrConflict
		}
	} else if attempt.state != "FINALIZING" {
		return FinalizeUploadResult{}, ErrConflict
	}
	if inserted.RowsAffected() != 0 {
		var count int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM artifact_finalizations f JOIN artifact_uploads u ON u.upload_id=f.upload_id WHERE u.attempt_id=$1`, r.Authority.AttemptID).Scan(&count); err != nil {
			return FinalizeUploadResult{}, err
		}
		// Different request IDs must not create an unbounded durable history even
		// when all of them target one zero-byte upload. Exact retries cost no slot.
		if count > MaxAttemptFinalizations {
			return FinalizeUploadResult{}, ErrUploadLimit
		}
	}
	artifact := ArtifactRecord{UploadID: r.UploadID, Object: object}
	err = tx.QueryRow(ctx, "SELECT id::text,object_version,verified_at FROM artifacts WHERE upload_id=$1", r.UploadID).Scan(&artifact.ArtifactID, &artifact.Object.Version, &artifact.VerifiedAt)
	switch {
	case err == nil:
		if artifact.Object != r.Object {
			return FinalizeUploadResult{}, ErrConflict
		}
		result.Artifact = &artifact
	case !errors.Is(err, pgx.ErrNoRows):
		return FinalizeUploadResult{}, err
	case verified:
		artifactID, err := uuid.NewRandom()
		if err != nil {
			return FinalizeUploadResult{}, err
		}
		artifact.ArtifactID = artifactID.String()
		if err = tx.QueryRow(ctx, `INSERT INTO artifacts(id,upload_id,finalization_id,object_version) VALUES($1,$2,$3,$4) RETURNING verified_at`, artifact.ArtifactID, r.UploadID, savedID, r.Object.Version).Scan(&artifact.VerifiedAt); err != nil {
			return FinalizeUploadResult{}, err
		}
		if err = appendArtifactEvent(ctx, tx, r.Authority, artifact); err != nil {
			return FinalizeUploadResult{}, err
		}
		result.Artifact = &artifact
	}
	if err = tx.Commit(ctx); err != nil {
		return FinalizeUploadResult{}, err
	}
	return result, nil
}

func appendArtifactEvent(ctx context.Context, tx pgx.Tx, a AttemptAuthority, artifact ArtifactRecord) error {
	var sequence int64
	if err := tx.QueryRow(ctx, "UPDATE jobs SET event_sequence=event_sequence+1 WHERE id=$1 RETURNING event_sequence", a.JobID).Scan(&sequence); err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{"artifactId": artifact.ArtifactID, "uploadId": artifact.UploadID, "sizeBytes": artifact.Object.SizeBytes, "sha256": artifact.Object.SHA256})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO job_events(job_id,sequence,attempt_id,type,payload) VALUES($1,$2,$3,'ARTIFACT_VERIFIED',$4)", a.JobID, sequence, a.AttemptID, body)
	return err
}
