//go:build integration

package store

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestAttemptHistoryScopesProjectAndPreservesFailureEvidence(t *testing.T) {
	pool, _, request := completedOutputFixture(t)
	ctx := context.Background()
	var projectID string
	if err := pool.QueryRow(ctx, "SELECT project_id::text FROM jobs WHERE id=$1", request.Authority.JobID).Scan(&projectID); err != nil {
		t.Fatal(err)
	}
	history, err := ListAttempts(ctx, pool, projectID, request.Authority.JobID)
	if err != nil || history.JobID != request.Authority.JobID || len(history.Attempts) != 1 || history.Attempts[0].ID != request.Authority.AttemptID || history.Attempts[0].Number != 1 || history.Attempts[0].State != "FINALIZING" {
		t.Fatal(history, err)
	}
	if _, err := ListAttempts(ctx, pool, uuid.NewString(), request.Authority.JobID); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign attempt history", err)
	}
	if _, err := ListAttempts(ctx, pool, projectID, uuid.NewString()); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown job", err)
	}
}
