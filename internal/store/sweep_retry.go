package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SweepRetryChild struct {
	ID          string `json:"id"`
	Index       int    `json:"index"`
	ParentJobID string `json:"parentJobId"`
}

type SweepRetryRecord struct {
	ID            string            `json:"id"`
	ProjectID     string            `json:"projectId"`
	ParentSweepID string            `json:"parentSweepId"`
	SpecHash      string            `json:"specHash"`
	MaxConcurrent int               `json:"maxConcurrent"`
	Children      []SweepRetryChild `json:"children"`
	CreatedAt     time.Time         `json:"createdAt"`
	Replayed      bool              `json:"-"`
}

// RetrySweep copies only failed/cancelled children of a terminal sweep into fresh
// jobs. The caller must authorize submit access to projectID before invoking it.
func RetrySweep(ctx context.Context, pool *pgxpool.Pool, projectID, sweepID, key string) (SweepRetryRecord, error) {
	if !canonicalUUID(projectID) || !canonicalUUID(sweepID) || len(key) < 1 || len(key) > 128 {
		return SweepRetryRecord{}, ErrInvalid
	}
	endpoint := "/v1/sweeps/" + sweepID + "/retry"
	requestHash := fmt.Sprintf("%x", sha256.Sum256([]byte(sweepID)))
	tx, err := beginTransition(ctx, pool)
	if err != nil {
		return SweepRetryRecord{}, err
	}
	defer rollback(tx)
	// Replay precedes mutable enablement/quota checks, so a lost reply cannot
	// turn an already admitted retry into different jobs or depend on a registry.
	if saved, err := lookupSweepRetry(ctx, tx, projectID, sweepID, endpoint, key, requestHash); err == nil {
		return saved, nil
	} else if !errors.Is(err, ErrNotFound) {
		return SweepRetryRecord{}, err
	}
	var sourceSpec []byte
	var expectedCount int
	err = tx.QueryRow(ctx, "SELECT spec,child_count FROM sweeps WHERE id=$1 AND project_id=$2", sweepID, projectID).Scan(&sourceSpec, &expectedCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return SweepRetryRecord{}, ErrNotFound
	}
	if err != nil {
		return SweepRetryRecord{}, err
	}
	var total, unfinished int
	var parents []string
	var cpu, memory int64
	err = tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELLED')),
		coalesce(array_agg(id::text ORDER BY sweep_index) FILTER(WHERE state IN ('FAILED','CANCELLED')),ARRAY[]::text[]),
		coalesce(max(cpu_millis) FILTER(WHERE state IN ('FAILED','CANCELLED')),0),
		coalesce(max(memory_mib) FILTER(WHERE state IN ('FAILED','CANCELLED')),0)
		FROM jobs WHERE sweep_id=$1 AND project_id=$2`, sweepID, projectID).Scan(&total, &unfinished, &parents, &cpu, &memory)
	if err != nil {
		return SweepRetryRecord{}, err
	}
	if total != expectedCount || total < 1 || total > spec.MaxSweepJobs {
		return SweepRetryRecord{}, ErrInvalid
	}
	// The cluster transition lock stabilizes this observation. Once all source
	// jobs are terminal, their immutable outcomes fix the selected retry subset.
	if unfinished != 0 || len(parents) == 0 {
		return SweepRetryRecord{}, ErrConflict
	}
	var cpuQuota, memoryQuota int64
	var enabled bool
	err = tx.QueryRow(ctx, "SELECT cpu_quota,memory_quota_mib,enabled FROM projects WHERE id=$1 FOR SHARE", projectID).Scan(&cpuQuota, &memoryQuota, &enabled)
	if err != nil {
		return SweepRetryRecord{}, err
	}
	if !enabled {
		return SweepRetryRecord{}, ErrDisabled
	}
	if cpu > cpuQuota || memory > memoryQuota {
		return SweepRetryRecord{}, ErrQuota
	}
	body, hash, err := retrySweepSpec(sourceSpec, sweepID, parents)
	if err != nil {
		return SweepRetryRecord{}, err
	}
	var retryID string
	err = tx.QueryRow(ctx, `INSERT INTO idempotency_keys(project_id,endpoint,key,request_hash,response_reference)
		VALUES($1,$2,$3,$4,gen_random_uuid()) ON CONFLICT DO NOTHING RETURNING response_reference::text`, projectID, endpoint, key, requestHash).Scan(&retryID)
	if errors.Is(err, pgx.ErrNoRows) {
		return lookupSweepRetry(ctx, tx, projectID, sweepID, endpoint, key, requestHash)
	}
	if err != nil {
		return SweepRetryRecord{}, err
	}
	result := SweepRetryRecord{ID: retryID, ProjectID: projectID, ParentSweepID: sweepID, SpecHash: hash}
	err = tx.QueryRow(ctx, `INSERT INTO sweeps(id,project_id,parent_sweep_id,name,spec,spec_hash,child_count,max_concurrent,fail_fast,cancel_running_on_failure)
		SELECT $1,project_id,id,name,$3,$4,$5,max_concurrent,fail_fast,cancel_running_on_failure FROM sweeps WHERE id=$2
		RETURNING max_concurrent,created_at`, retryID, sweepID, body, hash, len(parents)).Scan(&result.MaxConcurrent, &result.CreatedAt)
	if err != nil {
		return SweepRetryRecord{}, err
	}
	// QUEUED retries get new identities, zero attempts, and no accepted result.
	// Copy frozen execution bytes and input IDs, never resolve mutable names again.
	tag, err := tx.Exec(ctx, `INSERT INTO jobs(project_id,parent_job_id,spec,spec_hash,cpu_millis,memory_mib,scratch_mib,priority,sweep_id,sweep_index,event_sequence)
		SELECT project_id,id,spec,spec_hash,cpu_millis,memory_mib,scratch_mib,priority,$1,
			(row_number() OVER(ORDER BY sweep_index)-1)::integer,1
		FROM jobs WHERE id=ANY($2::uuid[]) ORDER BY sweep_index`, retryID, parents)
	if err != nil {
		return SweepRetryRecord{}, err
	}
	if tag.RowsAffected() != int64(len(parents)) {
		return SweepRetryRecord{}, ErrInvalid
	}
	if _, err := tx.Exec(ctx, `INSERT INTO job_inputs(job_id,project_id,position,dataset_id,mount_path)
		SELECT n.id,n.project_id,i.position,i.dataset_id,i.mount_path
		FROM jobs n JOIN job_inputs i ON i.job_id=n.parent_job_id AND i.project_id=n.project_id WHERE n.sweep_id=$1`, retryID); err != nil {
		return SweepRetryRecord{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO job_events(job_id,sequence,type,payload)
		SELECT id,1,'SUBMITTED',jsonb_build_object('specHash',spec_hash,'parentJobId',parent_job_id,'parentSweepId',$2::text)
		FROM jobs WHERE sweep_id=$1`, retryID, sweepID); err != nil {
		return SweepRetryRecord{}, err
	}
	result.Children, err = sweepRetryChildren(ctx, tx, retryID)
	if err != nil {
		return SweepRetryRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SweepRetryRecord{}, err
	}
	result.CreatedAt = result.CreatedAt.UTC()
	return result, nil
}

func retrySweepSpec(source []byte, sweepID string, parents []string) ([]byte, string, error) {
	var document map[string]json.RawMessage
	if json.Unmarshal(source, &document) != nil || document == nil {
		return nil, "", ErrInvalid
	}
	// Server-owned selection extends the frozen matrix description: arbitrary
	// failed combinations need not form a rectangular matrix. Hash the selection
	// too, so a partial retry is distinguishable from rerunning the whole grid.
	selection, err := json.Marshal(struct {
		ParentSweepID string   `json:"parentSweepId"`
		ParentJobIDs  []string `json:"parentJobIds"`
	}{sweepID, parents})
	if err != nil {
		return nil, "", err
	}
	document["retry"] = selection
	body, err := json.Marshal(document)
	if err != nil || len(body) > 2<<20 {
		return nil, "", ErrInvalid
	}
	return body, fmt.Sprintf("%x", sha256.Sum256(body)), nil
}

func lookupSweepRetry(ctx context.Context, q rowQuerier, projectID, sweepID, endpoint, key, requestHash string) (SweepRetryRecord, error) {
	var result SweepRetryRecord
	var savedHash string
	err := q.QueryRow(ctx, `SELECT s.id::text,s.project_id::text,s.parent_sweep_id::text,s.spec_hash,s.max_concurrent,s.created_at,k.request_hash
		FROM idempotency_keys k JOIN sweeps s ON s.id=k.response_reference AND s.project_id=k.project_id
		WHERE k.project_id=$1 AND k.endpoint=$2 AND k.key=$3 AND s.parent_sweep_id=$4`, projectID, endpoint, key, sweepID).Scan(&result.ID, &result.ProjectID, &result.ParentSweepID, &result.SpecHash, &result.MaxConcurrent, &result.CreatedAt, &savedHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return SweepRetryRecord{}, ErrNotFound
	}
	if err != nil {
		return SweepRetryRecord{}, err
	}
	if savedHash != requestHash {
		return SweepRetryRecord{}, ErrConflict
	}
	result.Children, err = sweepRetryChildren(ctx, q, result.ID)
	result.Replayed, result.CreatedAt = true, result.CreatedAt.UTC()
	return result, err
}

func sweepRetryChildren(ctx context.Context, q rowQuerier, sweepID string) ([]SweepRetryChild, error) {
	var body []byte
	err := q.QueryRow(ctx, `SELECT coalesce(jsonb_agg(jsonb_build_object('id',id,'index',sweep_index,'parentJobId',parent_job_id) ORDER BY sweep_index),'[]'::jsonb)
		FROM jobs WHERE sweep_id=$1`, sweepID).Scan(&body)
	if err != nil {
		return nil, err
	}
	var children []SweepRetryChild
	if json.Unmarshal(body, &children) != nil {
		return nil, ErrInvalid
	}
	return children, nil
}
