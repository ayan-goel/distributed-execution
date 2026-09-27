package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"time"

	"github.com/google/uuid"
)

type Attempt struct {
	ID             string     `json:"id"`
	Number         int64      `json:"number"`
	State          string     `json:"state"`
	Reason         *string    `json:"reason"`
	ExitCode       *int32     `json:"exitCode"`
	WorkerID       string     `json:"workerId"`
	CleanupPending bool       `json:"cleanupPending"`
	CreatedAt      time.Time  `json:"createdAt"`
	FinishedAt     *time.Time `json:"finishedAt"`
}

func (c *Client) ListAttempts(ctx context.Context, jobID string) ([]Attempt, error) {
	if !downloadUUID(jobID) {
		return nil, errors.New("job ID must be a canonical UUID")
	}
	body, err := c.requestBody(ctx, http.MethodGet, "/v1/jobs/"+jobID+"/attempts", nil, "")
	if err != nil {
		return nil, err
	}
	var response struct {
		JobID    string    `json:"jobId"`
		Attempts []Attempt `json:"attempts"`
	}
	if json.Unmarshal(body, &response) != nil || response.JobID != jobID || response.Attempts == nil || len(response.Attempts) > 10 {
		return nil, errors.New("invalid attempt history response")
	}
	states := []string{"ASSIGNED", "STARTING", "RUNNING", "FINALIZING", "SUCCEEDED", "FAILED", "LOST", "CANCELLED"}
	for index, attempt := range response.Attempts {
		id, err := uuid.Parse(attempt.ID)
		if err != nil || id.String() != attempt.ID || !downloadUUID(attempt.WorkerID) || !slices.Contains(states, attempt.State) || attempt.Number != int64(index+1) || attempt.CreatedAt.IsZero() || attempt.FinishedAt != nil && attempt.FinishedAt.Before(attempt.CreatedAt) {
			return nil, errors.New("invalid attempt history response")
		}
	}
	return response.Attempts, nil
}
