package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxEventPageBytes = 1 << 20

type JobEvent struct {
	Sequence  int64           `json:"sequence"`
	AttemptID *string         `json:"attemptId"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"createdAt"`
}

type JobEventPage struct {
	Events       []JobEvent `json:"events"`
	HasMore      bool       `json:"hasMore"`
	NextSequence int64      `json:"-"`
}

func ListJobEvents(ctx context.Context, pool *pgxpool.Pool, projectID, jobID string, after int64, limit int) (JobEventPage, error) {
	page := JobEventPage{Events: []JobEvent{}, NextSequence: after}
	if !canonicalUUID(projectID) || !canonicalUUID(jobID) || after < 0 || limit < 1 || limit > 100 {
		return page, ErrInvalid
	}
	// Keep ownership and event rows in one read-only snapshot. No lifecycle
	// locks are needed, and local timeouts bound retention/DDL conflicts.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return page, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='4s'"); err != nil {
		return page, err
	}
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1 AND project_id=$2)", jobID, projectID).Scan(&exists); err != nil {
		return page, err
	}
	if !exists {
		return page, ErrNotFound
	}
	// A cursor is only a sequence position. Repeat authenticated project scope
	// in SQL so a forged cursor cannot disclose another project's history.
	rows, err := tx.Query(ctx, `SELECT e.sequence,e.attempt_id::text,e.type,e.payload,e.created_at
		FROM job_events e JOIN jobs j ON j.id=e.job_id
		WHERE e.job_id=$1 AND j.project_id=$2 AND e.sequence>$3
		ORDER BY e.sequence LIMIT $4`, jobID, projectID, after, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	// Reserve the bounded public envelope/cursor. Counting encoded bytes also
	// covers JSON escaping, which can expand a valid 64 KiB stored payload.
	used := 1024
	for rows.Next() {
		var event JobEvent
		if err = rows.Scan(&event.Sequence, &event.AttemptID, &event.Type, &event.Payload, &event.CreatedAt); err != nil {
			return JobEventPage{}, err
		}
		event.CreatedAt = event.CreatedAt.UTC()
		encoded, err := json.Marshal(event)
		if err != nil {
			return JobEventPage{}, err
		}
		if len(page.Events) == limit || used+len(encoded)+1 > MaxEventPageBytes {
			if len(page.Events) == 0 {
				return JobEventPage{}, errors.New("stored event exceeds page budget")
			}
			page.HasMore = true
			break
		}
		page.Events = append(page.Events, event)
		page.NextSequence = event.Sequence
		used += len(encoded) + 1
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return JobEventPage{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return JobEventPage{}, err
	}
	return page, nil
}
