package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SweepRecord struct {
	ID            string          `json:"id"`
	ProjectID     string          `json:"projectId"`
	Spec          json.RawMessage `json:"spec"`
	SpecHash      string          `json:"specHash"`
	ChildIDs      []string        `json:"childIds"`
	MaxConcurrent int             `json:"maxConcurrent"`
	CreatedAt     time.Time       `json:"createdAt"`
	Replayed      bool            `json:"-"`
}

func LookupSweepSubmission(ctx context.Context, pool *pgxpool.Pool, project, key, requestHash string) (SweepRecord, error) {
	return lookupSweepSubmission(ctx, pool, project, key, requestHash)
}

func lookupSweepSubmission(ctx context.Context, q rowQuerier, project, key, requestHash string) (SweepRecord, error) {
	var sweep SweepRecord
	var existingHash string
	err := q.QueryRow(ctx, `SELECT s.id::text,s.project_id::text,s.spec,s.spec_hash,s.max_concurrent,s.created_at,k.request_hash
		FROM idempotency_keys k JOIN projects p ON p.id=k.project_id JOIN sweeps s ON s.id=k.response_reference
		WHERE p.name=$1 AND k.endpoint='/v1/sweeps' AND k.key=$2`, project, key).Scan(
		&sweep.ID, &sweep.ProjectID, &sweep.Spec, &sweep.SpecHash, &sweep.MaxConcurrent, &sweep.CreatedAt, &existingHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return SweepRecord{}, ErrNotFound
	}
	if err != nil {
		return SweepRecord{}, err
	}
	if existingHash != requestHash {
		return SweepRecord{}, ErrConflict
	}
	sweep.ChildIDs, err = sweepChildIDs(ctx, q, sweep.ID)
	if err != nil {
		return SweepRecord{}, err
	}
	sweep.CreatedAt = sweep.CreatedAt.UTC()
	sweep.Replayed = true
	return sweep, nil
}

func sweepChildIDs(ctx context.Context, q rowQuerier, sweepID string) ([]string, error) {
	// A scalar aggregate keeps replay available through the same transaction
	// query interface while preserving stable child-index order.
	var ids []string
	err := q.QueryRow(ctx, `SELECT coalesce(array_agg(id::text ORDER BY sweep_index),ARRAY[]::text[])
		FROM jobs WHERE sweep_id=$1`, sweepID).Scan(&ids)
	return ids, err
}

func SubmitSweepResolved(ctx context.Context, pool *pgxpool.Pool, key, requestHash string, sweep spec.Sweep, bindings []DatasetBinding) (SweepRecord, error) {
	if len(key) < 1 || len(key) > 128 || !hashPattern.MatchString(requestHash) {
		return SweepRecord{}, ErrInvalid
	}
	children, err := sweep.Expand()
	if err != nil {
		return SweepRecord{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	template := sweep.Spec.JobTemplate
	if !pinnedImage.MatchString(template.Spec.Image) || len(template.Spec.Inputs) != len(bindings) {
		return SweepRecord{}, ErrInvalid
	}
	for i, binding := range bindings {
		if !canonicalUUID(binding.ID) || binding.Name != template.Spec.Inputs[i].Dataset {
			return SweepRecord{}, ErrInvalid
		}
	}
	canonical, hash, err := sweep.Canonical()
	if err != nil {
		return SweepRecord{}, ErrInvalid
	}
	result := SweepRecord{Spec: canonical, SpecHash: hash, ChildIDs: make([]string, 0, len(children)), MaxConcurrent: sweep.Spec.MaxConcurrent}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return SweepRecord{}, err
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='10s'"); err != nil {
		return SweepRecord{}, err
	}
	var projectID string
	var cpuQuota, memoryQuota int64
	var enabled bool
	err = tx.QueryRow(ctx, `SELECT id::text,cpu_quota,memory_quota_mib,enabled FROM projects
		WHERE name=$1 FOR SHARE`, sweep.Metadata.Project).Scan(&projectID, &cpuQuota, &memoryQuota, &enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return SweepRecord{}, ErrNotFound
	}
	if err != nil {
		return SweepRecord{}, err
	}
	var id string
	err = tx.QueryRow(ctx, `INSERT INTO idempotency_keys(project_id,endpoint,key,request_hash,response_reference)
		VALUES($1,'/v1/sweeps',$2,$3,gen_random_uuid()) ON CONFLICT DO NOTHING RETURNING response_reference::text`, projectID, key, requestHash).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return lookupSweepSubmission(ctx, tx, sweep.Metadata.Project, key, requestHash)
	}
	if err != nil {
		return SweepRecord{}, err
	}
	if !enabled {
		return SweepRecord{}, ErrDisabled
	}
	for _, child := range children {
		if child.Spec.Resources.CPUMillis > cpuQuota || child.Spec.Resources.MemoryMiB > memoryQuota {
			return SweepRecord{}, ErrQuota
		}
	}
	err = tx.QueryRow(ctx, `INSERT INTO sweeps(id,project_id,name,spec,spec_hash,child_count,max_concurrent,fail_fast,cancel_running_on_failure)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id::text,project_id::text,created_at`, id, projectID,
		sweep.Metadata.Name, canonical, result.SpecHash, len(children), sweep.Spec.MaxConcurrent,
		sweep.Spec.FailFast, sweep.Spec.CancelRunningOnFailure).Scan(&result.ID, &result.ProjectID, &result.CreatedAt)
	if err != nil {
		return SweepRecord{}, err
	}
	for index, child := range children {
		body, childHash, err := child.Canonical()
		if err != nil {
			return SweepRecord{}, ErrInvalid
		}
		var childID string
		err = tx.QueryRow(ctx, `INSERT INTO jobs(project_id,spec,spec_hash,cpu_millis,memory_mib,scratch_mib,priority,sweep_id,sweep_index,event_sequence)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,1) RETURNING id::text`, projectID, body, childHash,
			child.Spec.Resources.CPUMillis, child.Spec.Resources.MemoryMiB, child.Spec.Resources.ScratchMiB, child.Spec.Priority,
			result.ID, index).Scan(&childID)
		if err != nil {
			return SweepRecord{}, err
		}
		for position, binding := range bindings {
			tag, err := tx.Exec(ctx, `INSERT INTO job_inputs(job_id,project_id,position,dataset_id,mount_path)
				SELECT $1,$2,$3,d.id,$4 FROM datasets d WHERE d.id=$5 AND d.project_id=$2 AND d.name=$6`,
				childID, projectID, position, child.Spec.Inputs[position].MountPath, binding.ID, binding.Name)
			if err != nil {
				return SweepRecord{}, err
			}
			if tag.RowsAffected() != 1 {
				return SweepRecord{}, ErrNotFound
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO job_events(job_id,sequence,type,payload)
			VALUES($1,1,'SUBMITTED',jsonb_build_object('specHash',$2::text))`, childID, childHash); err != nil {
			return SweepRecord{}, err
		}
		result.ChildIDs = append(result.ChildIDs, childID)
	}
	if err := tx.Commit(ctx); err != nil {
		return SweepRecord{}, err
	}
	result.CreatedAt = result.CreatedAt.UTC()
	return result, nil
}
