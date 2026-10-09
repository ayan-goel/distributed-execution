package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxSweepPageSize = 100
const MaxSweepPageBytes = 2 << 20

type SweepProgress struct {
	Total      int `json:"total"`
	Queued     int `json:"queued"`
	RetryWait  int `json:"retryWait"`
	Active     int `json:"active"`
	Cancelling int `json:"cancelling"`
	Succeeded  int `json:"succeeded"`
	Failed     int `json:"failed"`
	Cancelled  int `json:"cancelled"`
}

type SweepSummary struct {
	ID                     string        `json:"id"`
	ProjectID              string        `json:"projectId"`
	Name                   string        `json:"name"`
	State                  string        `json:"state"`
	SpecHash               string        `json:"specHash"`
	MaxConcurrent          int           `json:"maxConcurrent"`
	FailFast               bool          `json:"failFast"`
	CancelRunningOnFailure bool          `json:"cancelRunningOnFailure"`
	CreatedAt              time.Time     `json:"createdAt"`
	Progress               SweepProgress `json:"progress"`
}

type SweepChild struct {
	ID                string                 `json:"id"`
	Index             int                    `json:"index"`
	State             string                 `json:"state"`
	Parameters        map[string]string      `json:"parameters"`
	CurrentAttemptID  *string                `json:"currentAttemptId"`
	AcceptedAttemptID *string                `json:"acceptedAttemptId"`
	Metrics           map[string]json.Number `json:"metrics"`
}

type SweepPage struct {
	Sweep     SweepSummary `json:"sweep"`
	Children  []SweepChild `json:"children"`
	HasMore   bool         `json:"hasMore"`
	NextIndex int          `json:"-"`
}

func GetSweep(ctx context.Context, pool *pgxpool.Pool, projectID, sweepID string, after, limit int) (SweepPage, error) {
	if !canonicalUUID(projectID) || !canonicalUUID(sweepID) || after < -1 || after >= spec.MaxSweepJobs || limit < 1 || limit > MaxSweepPageSize {
		return SweepPage{}, ErrInvalid
	}
	// Counts and children share a read-only snapshot. Completion or cancellation
	// between these queries must not produce contradictory progress within a page.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return SweepPage{}, err
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='4s'"); err != nil {
		return SweepPage{}, err
	}
	page := SweepPage{Children: []SweepChild{}, NextIndex: after}
	s := &page.Sweep
	p := &s.Progress
	err = tx.QueryRow(ctx, `SELECT s.id::text,s.project_id::text,s.name,s.spec_hash,s.max_concurrent,
		s.fail_fast,s.cancel_running_on_failure,s.created_at,s.child_count,
		count(*) FILTER(WHERE j.state='QUEUED'),count(*) FILTER(WHERE j.state='RETRY_WAIT'),
		count(*) FILTER(WHERE j.state='ACTIVE'),count(*) FILTER(WHERE j.state='CANCELLING'),
		count(*) FILTER(WHERE j.state='SUCCEEDED'),count(*) FILTER(WHERE j.state='FAILED'),
		count(*) FILTER(WHERE j.state='CANCELLED')
		FROM sweeps s LEFT JOIN jobs j ON j.sweep_id=s.id AND j.project_id=s.project_id
		WHERE s.id=$1 AND s.project_id=$2 GROUP BY s.id`, sweepID, projectID).Scan(
		&s.ID, &s.ProjectID, &s.Name, &s.SpecHash, &s.MaxConcurrent, &s.FailFast, &s.CancelRunningOnFailure,
		&s.CreatedAt, &p.Total, &p.Queued, &p.RetryWait, &p.Active, &p.Cancelling, &p.Succeeded, &p.Failed, &p.Cancelled)
	if errors.Is(err, pgx.ErrNoRows) {
		return SweepPage{}, ErrNotFound
	}
	if err != nil {
		return SweepPage{}, err
	}
	if p.Total != p.Queued+p.RetryWait+p.Active+p.Cancelling+p.Succeeded+p.Failed+p.Cancelled {
		return SweepPage{}, ErrInvalid
	}
	s.State, s.CreatedAt = sweepState(*p), s.CreatedAt.UTC()
	// Follow only accepted successful completion references. Failed or retried
	// attempts may retain diagnostic metrics but cannot supply canonical results.
	rows, err := tx.Query(ctx, `SELECT j.id::text,j.sweep_index,j.state,j.current_attempt_id::text,j.accepted_attempt_id::text,
		(SELECT jsonb_object_agg(k.key,j.spec->'spec'->'env'->k.key)
		 FROM jsonb_object_keys(s.spec->'spec'->'matrix') AS k(key)),
		coalesce(convert_from(c.manifest_json,'UTF8')::jsonb->'metrics','{}'::jsonb)
		FROM jobs j JOIN sweeps s ON s.id=j.sweep_id AND s.project_id=j.project_id
		LEFT JOIN attempt_completions c ON c.job_id=j.id AND c.attempt_id=j.accepted_attempt_id AND c.state='SUCCEEDED'
		WHERE j.sweep_id=$1 AND j.project_id=$2 AND j.sweep_index>$3
		ORDER BY j.sweep_index LIMIT $4`, sweepID, projectID, after, limit+1)
	if err != nil {
		return SweepPage{}, err
	}
	defer rows.Close()
	used := 2
	for rows.Next() {
		var child SweepChild
		var parameters, metrics []byte
		if err := rows.Scan(&child.ID, &child.Index, &child.State, &child.CurrentAttemptID, &child.AcceptedAttemptID, &parameters, &metrics); err != nil {
			return SweepPage{}, err
		}
		if json.Unmarshal(parameters, &child.Parameters) != nil || json.Unmarshal(metrics, &child.Metrics) != nil {
			return SweepPage{}, ErrInvalid
		}
		encoded, err := json.Marshal(child)
		if err != nil || len(encoded)+2 > MaxSweepPageBytes {
			return SweepPage{}, ErrInvalid
		}
		additional := len(encoded)
		if len(page.Children) > 0 {
			additional++
		}
		// Wide parameter matrices can exceed a row-count limit's byte budget.
		// Stop before the extra child so its stable index resumes on the next page.
		if len(page.Children) == limit || used+additional > MaxSweepPageBytes {
			page.HasMore = true
			break
		}
		page.Children = append(page.Children, child)
		page.NextIndex = child.Index
		used += additional
	}
	if err := rows.Err(); err != nil {
		return SweepPage{}, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return SweepPage{}, err
	}
	return page, nil
}

func sweepState(p SweepProgress) string {
	// A failed child does not finish the sweep while siblings still run. Final
	// aggregation preserves failure over cancellation once all children stop.
	if p.Succeeded+p.Failed+p.Cancelled == p.Total {
		if p.Failed > 0 {
			return "FAILED"
		}
		if p.Cancelled > 0 {
			return "CANCELLED"
		}
		return "SUCCEEDED"
	}
	if p.Queued == p.Total {
		return "QUEUED"
	}
	return "ACTIVE"
}
