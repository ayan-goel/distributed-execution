package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrConflict = errors.New("conflicting request or identity")
	ErrNotFound = errors.New("not found")
	ErrQuota    = errors.New("job exceeds project resource quota")
	ErrDisabled = errors.New("project disabled")
	ErrInvalid  = errors.New("invalid admission")
	hashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	pinnedImage = regexp.MustCompile(`^[^\s@]+@sha256:[0-9a-f]{64}$`)
)

type JobRecord struct {
	ID                string          `json:"id"`
	ProjectID         string          `json:"projectId"`
	State             string          `json:"state"`
	Spec              json.RawMessage `json:"spec"`
	SpecHash          string          `json:"specHash"`
	CreatedAt         time.Time       `json:"createdAt"`
	AcceptedAttemptID *string         `json:"acceptedAttemptId"`
	AcceptedManifest  json.RawMessage `json:"acceptedManifest"`
	Replayed          bool            `json:"-"`
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func GetJob(ctx context.Context, pool *pgxpool.Pool, projectID, id string) (JobRecord, error) {
	var job JobRecord
	// Scope the lookup in SQL and follow only the accepted completion pointer.
	// Partial uploads and failed-attempt diagnostics must never become canonical.
	err := pool.QueryRow(ctx, `SELECT j.id::text,j.project_id::text,j.state,j.spec,j.spec_hash,j.created_at,j.accepted_attempt_id::text,c.manifest_json
		FROM jobs j LEFT JOIN attempt_completions c ON c.job_id=j.id AND c.attempt_id=j.accepted_attempt_id AND c.state='SUCCEEDED'
		WHERE j.id=$1 AND j.project_id=$2`, id, projectID).Scan(&job.ID, &job.ProjectID, &job.State, &job.Spec, &job.SpecHash, &job.CreatedAt, &job.AcceptedAttemptID, &job.AcceptedManifest)
	if errors.Is(err, pgx.ErrNoRows) {
		return job, ErrNotFound
	}
	job.CreatedAt = job.CreatedAt.UTC()
	return job, err
}

func LookupSubmission(ctx context.Context, pool *pgxpool.Pool, project, key, requestHash string) (JobRecord, error) {
	return lookupSubmission(ctx, pool, project, key, requestHash)
}

func lookupSubmission(ctx context.Context, q rowQuerier, project, key, requestHash string) (JobRecord, error) {
	var job JobRecord
	var existingHash string
	err := q.QueryRow(ctx, `SELECT j.id::text,j.project_id::text,j.state,j.spec,j.spec_hash,j.created_at,k.request_hash,j.accepted_attempt_id::text,c.manifest_json
		FROM idempotency_keys k JOIN projects p ON p.id=k.project_id JOIN jobs j ON j.id=k.response_reference
		LEFT JOIN attempt_completions c ON c.job_id=j.id AND c.attempt_id=j.accepted_attempt_id AND c.state='SUCCEEDED'
		WHERE p.name=$1 AND k.endpoint='/v1/jobs' AND k.key=$2`, project, key).Scan(
		&job.ID, &job.ProjectID, &job.State, &job.Spec, &job.SpecHash, &job.CreatedAt, &existingHash, &job.AcceptedAttemptID, &job.AcceptedManifest)
	if errors.Is(err, pgx.ErrNoRows) {
		return job, ErrNotFound
	}
	if err != nil {
		return job, err
	}
	if existingHash != requestHash {
		return JobRecord{}, ErrConflict
	}
	job.CreatedAt = job.CreatedAt.UTC()
	job.Replayed = true
	return job, nil
}

func SubmitJob(ctx context.Context, pool *pgxpool.Pool, key, requestHash string, job spec.Job) (JobRecord, error) {
	var result JobRecord
	if len(key) < 1 || len(key) > 128 || !hashPattern.MatchString(requestHash) {
		return result, ErrInvalid
	}
	canonical, executionHash, err := job.Canonical()
	if err != nil {
		return result, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	// INVARIANT: queue insertion accepts resolved execution identities only.
	// Dataset manifests will extend this contract in the data-pipeline slice;
	// unresolved dataset names must never silently enter the runnable queue.
	if !pinnedImage.MatchString(job.Spec.Image) || len(job.Spec.Inputs) != 0 {
		return result, fmt.Errorf("%w: image must be pinned and inputs resolved", ErrInvalid)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='10s'"); err != nil {
		return result, err
	}
	var projectID string
	var cpuQuota, memoryQuota int64
	var enabled bool
	// Hold a shared project lock so disabling a project or lowering its limits
	// cannot race admission. Concurrent submissions can still share this lock.
	err = tx.QueryRow(ctx, "SELECT id::text,cpu_quota,memory_quota_mib,enabled FROM projects WHERE name=$1 FOR SHARE", job.Metadata.Project).Scan(&projectID, &cpuQuota, &memoryQuota, &enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrNotFound
	}
	if err != nil {
		return result, err
	}
	var id string
	err = tx.QueryRow(ctx, `INSERT INTO idempotency_keys(project_id,endpoint,key,request_hash,response_reference)
		VALUES ($1,'/v1/jobs',$2,$3,gen_random_uuid()) ON CONFLICT DO NOTHING RETURNING response_reference::text`, projectID, key, requestHash).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return lookupSubmission(ctx, tx, job.Metadata.Project, key, requestHash)
	}
	if err != nil {
		return result, err
	}
	if !enabled {
		return result, ErrDisabled
	}
	if job.Spec.Resources.CPUMillis > cpuQuota || job.Spec.Resources.MemoryMiB > memoryQuota {
		return result, ErrQuota
	}
	// Admission transitions a new immutable job into QUEUED. The submission key,
	// job, and first event commit together so a lost reply can be recovered safely.
	err = tx.QueryRow(ctx, `INSERT INTO jobs(id,project_id,spec,spec_hash,cpu_millis,memory_mib,scratch_mib,event_sequence)
		VALUES ($1,$2,$3,$4,$5,$6,$7,1) RETURNING id::text,project_id::text,state,spec,spec_hash,created_at`, id, projectID, canonical, executionHash,
		job.Spec.Resources.CPUMillis, job.Spec.Resources.MemoryMiB, job.Spec.Resources.ScratchMiB).Scan(
		&result.ID, &result.ProjectID, &result.State, &result.Spec, &result.SpecHash, &result.CreatedAt)
	if err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO job_events(job_id,sequence,type,payload) VALUES ($1,1,'SUBMITTED',jsonb_build_object('specHash',$2::text))`, id, executionHash); err != nil {
		return result, err
	}
	if err = tx.Commit(ctx); err != nil {
		return JobRecord{}, err
	}
	result.CreatedAt = result.CreatedAt.UTC()
	return result, nil
}
