package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxLogSegmentBytes int64 = 1 << 20
const MaxLogSegmentGaps = 1024

type LogSequenceGap struct {
	FirstSequence uint64 `json:"firstSequence"`
	LastSequence  uint64 `json:"lastSequence"`
}

type LogSegmentRequest struct {
	Authority     AttemptAuthority `json:"authority"`
	RequestID     string           `json:"-"`
	ArtifactID    string           `json:"artifactId"`
	Stream        string           `json:"stream"`
	FirstSequence uint64           `json:"firstSequence"`
	LastSequence  uint64           `json:"lastSequence"`
	Gaps          []LogSequenceGap `json:"gaps"`
}

func (r LogSegmentRequest) normalized(worker string) (LogSegmentRequest, string, error) {
	a := r.Authority
	if !canonicalUUID(r.RequestID) || !canonicalUUID(r.ArtifactID) || !canonicalUUID(a.JobID) || !canonicalUUID(a.AttemptID) || !canonicalUUID(a.SessionID) || a.WorkerID != worker || a.Generation < 1 || !slices.Contains([]string{"STDOUT", "STDERR"}, r.Stream) || r.FirstSequence < 1 || r.LastSequence < r.FirstSequence || r.LastSequence > math.MaxInt64 || len(r.Gaps) > MaxLogSegmentGaps {
		return LogSegmentRequest{}, "", ErrInvalid
	}
	r.Gaps = append([]LogSequenceGap{}, r.Gaps...)
	var previous uint64
	var dropped uint64
	for _, gap := range r.Gaps {
		if gap.FirstSequence < r.FirstSequence || gap.LastSequence > r.LastSequence || gap.LastSequence < gap.FirstSequence || gap.FirstSequence <= previous {
			return LogSegmentRequest{}, "", ErrInvalid
		}
		previous = gap.LastSequence
		dropped += gap.LastSequence - gap.FirstSequence + 1
	}
	// A cataloged segment must contain at least one captured record. A range
	// consisting entirely of dropped records belongs in completion gap evidence.
	if dropped >= r.LastSequence-r.FirstSequence+1 {
		return LogSegmentRequest{}, "", ErrInvalid
	}
	body, err := json.Marshal(r)
	if err != nil {
		return LogSegmentRequest{}, "", ErrInvalid
	}
	hash := sha256.Sum256(append([]byte("dispatch.worker.v1.RegisterLogSegment\n"), body...))
	return r, hex.EncodeToString(hash[:]), nil
}

// RegisterLogSegment catalogs only an already verified LOG object. The attempt
// lock serializes sequence advancement without extending execution authority.
func RegisterLogSegment(ctx context.Context, pool *pgxpool.Pool, id WorkerIdentity, request LogSegmentRequest) (MutationResult, error) {
	if !canonicalUUID(id.WorkerID) || !canonicalUUID(id.CredentialID) {
		return MutationResult{}, ErrUnauthorized
	}
	r, hash, err := request.normalized(id.WorkerID)
	if err != nil {
		return MutationResult{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='4s'"); err != nil {
		return MutationResult{}, err
	}
	if err = authorizeWorkerTx(ctx, tx, id); err != nil {
		return MutationResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.Authority.SessionID); err != nil {
		return MutationResult{}, err
	}
	jobs, attempts, err := lockLeaseBatch(ctx, tx, []AttemptAuthority{r.Authority})
	if err != nil {
		return MutationResult{}, err
	}
	if err = requireCurrentWorkerSession(ctx, tx, id.WorkerID, r.Authority.SessionID); err != nil {
		return MutationResult{}, err
	}
	attempt := attempts[r.Authority.AttemptID]
	if attempt.authority != r.Authority {
		return MutationResult{Decision: "FENCED"}, nil
	}
	var savedHash string
	err = tx.QueryRow(ctx, "SELECT request_hash FROM log_segments WHERE worker_id=$1 AND session_id=$2 AND request_id=$3", id.WorkerID, r.Authority.SessionID, r.RequestID).Scan(&savedHash)
	if err == nil {
		if savedHash != hash {
			return MutationResult{}, ErrConflict
		}
		return MutationResult{Decision: "ACCEPTED", State: attempt.state}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return MutationResult{}, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return MutationResult{}, err
	}
	decision := leaseDecision(r.Authority, jobs[r.Authority.JobID], attempt, now)
	if decision != "ACCEPTED" {
		return MutationResult{Decision: decision, State: attempt.state}, nil
	}
	if !slices.Contains([]string{"STARTING", "RUNNING", "FINALIZING"}, attempt.state) {
		return MutationResult{}, ErrConflict
	}
	var uploadID string
	var size int64
	err = tx.QueryRow(ctx, `SELECT u.upload_id::text,u.size_bytes FROM artifacts a JOIN artifact_uploads u ON u.upload_id=a.upload_id WHERE a.id=$1 AND u.attempt_id=$2 AND u.job_id=$3 AND u.worker_id=$4 AND u.session_id=$5 AND u.generation=$6 AND u.kind='LOG' AND u.logical_name=$7`, r.ArtifactID, r.Authority.AttemptID, r.Authority.JobID, id.WorkerID, r.Authority.SessionID, r.Authority.Generation, logName(r.Stream)).Scan(&uploadID, &size)
	if errors.Is(err, pgx.ErrNoRows) {
		return MutationResult{}, ErrNotFound
	}
	if err != nil {
		return MutationResult{}, err
	}
	if size < 1 || size > MaxLogSegmentBytes {
		return MutationResult{}, ErrInvalid
	}
	var claimed bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM log_segments WHERE artifact_id=$1)", r.ArtifactID).Scan(&claimed); err != nil {
		return MutationResult{}, err
	}
	if claimed {
		return MutationResult{}, ErrConflict
	}
	var last *int64
	if err = tx.QueryRow(ctx, "SELECT max(last_sequence) FROM log_segments WHERE attempt_id=$1 AND stream=$2", r.Authority.AttemptID, r.Stream).Scan(&last); err != nil {
		return MutationResult{}, err
	}
	if last == nil && r.FirstSequence != 1 || last != nil && (*last == math.MaxInt64 || r.FirstSequence != uint64(*last+1)) {
		return MutationResult{}, ErrConflict
	}
	gaps, err := json.Marshal(r.Gaps)
	if err != nil {
		return MutationResult{}, ErrInvalid
	}
	segmentID, err := uuid.NewRandom()
	if err != nil {
		return MutationResult{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO log_segments(id,attempt_id,worker_id,session_id,request_id,request_hash,artifact_id,upload_id,stream,logical_name,first_sequence,last_sequence,gaps) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, segmentID.String(), r.Authority.AttemptID, id.WorkerID, r.Authority.SessionID, r.RequestID, hash, r.ArtifactID, uploadID, r.Stream, logName(r.Stream), int64(r.FirstSequence), int64(r.LastSequence), gaps)
	if err != nil {
		return MutationResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return MutationResult{}, err
	}
	return MutationResult{Decision: "ACCEPTED", State: attempt.state}, nil
}

func logName(stream string) string {
	if stream == "STDOUT" {
		return "stdout"
	}
	return "stderr"
}
