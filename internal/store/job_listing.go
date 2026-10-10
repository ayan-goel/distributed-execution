package store

import (
	"context"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Standard JSON escaping can expand each label value byte to six bytes.
// The 128-entry, 8192-byte value and 128-byte key admission bounds keep one
// summary below 6.1 MiB; an 8 MiB page therefore always permits progress.
const MaxJobPageBytes = 8 << 20

type JobListFilter struct {
	State  string
	Labels map[string]string
}

type JobListPosition struct {
	CreatedAt time.Time
	ID        string
}

type JobSummary struct {
	ID        string            `json:"id"`
	ProjectID string            `json:"projectId"`
	Name      string            `json:"name"`
	State     string            `json:"state"`
	Labels    map[string]string `json:"labels"`
	Priority  int               `json:"priority"`
	CreatedAt time.Time         `json:"createdAt"`
}

type JobListPage struct {
	Jobs    []JobSummary     `json:"jobs"`
	HasMore bool             `json:"hasMore"`
	Next    *JobListPosition `json:"-"`
}

func validJobListState(state string) bool {
	switch state {
	case "", "QUEUED", "RETRY_WAIT", "ACTIVE", "CANCELLING", "SUCCEEDED", "FAILED", "CANCELLED":
		return true
	}
	return false
}

func validJobListFilter(filter JobListFilter) bool {
	if !validJobListState(filter.State) || len(filter.Labels) > 128 {
		return false
	}
	for key, value := range filter.Labels {
		if !workerName.MatchString(key) || len(value) > 8192 || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return false
		}
	}
	return true
}

func ListJobs(ctx context.Context, pool *pgxpool.Pool, projectID string, filter JobListFilter, after *JobListPosition, limit int) (JobListPage, error) {
	page := JobListPage{Jobs: []JobSummary{}}
	if !canonicalUUID(projectID) || limit < 1 || limit > 100 || !validJobListFilter(filter) {
		return page, ErrInvalid
	}
	var afterTime, afterID any
	if after != nil {
		if !canonicalUUID(after.ID) || after.CreatedAt.IsZero() || after.CreatedAt.UTC().Year() < 1 || after.CreatedAt.UTC().Year() > 9999 {
			return page, ErrInvalid
		}
		afterTime, afterID = after.CreatedAt.UTC(), after.ID
	}
	labels := filter.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	labelJSON, err := json.Marshal(labels)
	if err != nil {
		return page, err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return page, err
	}
	defer rollback(tx)
	// Listing must not hold scheduler locks. Timeouts bound waits and expensive
	// filtered scans; the ordering index alone does not bound sparse-filter work.
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='4s'"); err != nil {
		return page, err
	}
	// Project scope is independent of the supplied position. Comparing values
	// keeps continuation valid if retention deletes the last returned job.
	rows, err := tx.Query(ctx, `SELECT j.id::text,j.project_id::text,j.spec->'metadata'->>'name',j.state,
		COALESCE(j.spec->'metadata'->'labels','{}'::jsonb),j.priority,j.created_at
		FROM jobs j WHERE j.project_id=$1 AND ($2='' OR j.state::text=$2)
		AND COALESCE(j.spec->'metadata'->'labels','{}'::jsonb) @> $3::jsonb
		AND ($4::timestamptz IS NULL OR (j.created_at,j.id)<($4::timestamptz,$5::uuid))
		ORDER BY j.created_at DESC,j.id DESC LIMIT $6`, projectID, filter.State, labelJSON, afterTime, afterID, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	// Reserve space for the public envelope and bounded continuation cursor.
	// Stop before the unreturned row so the next page never skips it.
	used := 1024
	for rows.Next() {
		var job JobSummary
		var body []byte
		if err = rows.Scan(&job.ID, &job.ProjectID, &job.Name, &job.State, &body, &job.Priority, &job.CreatedAt); err != nil {
			return JobListPage{}, err
		}
		if err = json.Unmarshal(body, &job.Labels); err != nil {
			return JobListPage{}, err
		}
		job.CreatedAt = job.CreatedAt.UTC()
		encoded, err := json.Marshal(job)
		if err != nil {
			return JobListPage{}, err
		}
		if len(page.Jobs) == limit || used+len(encoded)+1 > MaxJobPageBytes {
			// An out-of-contract database row must fail rather than emit an empty
			// page whose continuation can never advance past that row.
			if len(page.Jobs) == 0 {
				return JobListPage{}, ErrInvalid
			}
			page.HasMore = true
			last := page.Jobs[len(page.Jobs)-1]
			page.Next = &JobListPosition{CreatedAt: last.CreatedAt, ID: last.ID}
			break
		}
		page.Jobs = append(page.Jobs, job)
		used += len(encoded) + 1
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return JobListPage{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return JobListPage{}, err
	}
	return page, nil
}
