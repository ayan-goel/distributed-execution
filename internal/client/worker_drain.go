package client

import (
	"context"
	"errors"
	"net/http"
)

type WorkerDrain struct {
	WorkerID       string `json:"workerId"`
	State          string `json:"state"`
	DrainRequested bool   `json:"drainRequested"`
}

func validWorkerState(state string) bool {
	switch state {
	case "REGISTERING", "READY", "DRAINING", "SUSPECT", "OFFLINE", "QUARANTINED":
		return true
	}
	return false
}

func (c *Client) DrainWorker(ctx context.Context, workerID string) (WorkerDrain, error) {
	if !downloadUUID(workerID) {
		return WorkerDrain{}, errors.New("worker ID must be a canonical UUID")
	}
	body, err := c.requestBody(ctx, http.MethodPost, "/v1/workers/"+workerID+"/drain", nil, "")
	if err != nil {
		return WorkerDrain{}, err
	}
	var result WorkerDrain
	if requiredDiagnosticObject(body, map[string]any{"workerId": &result.WorkerID, "state": &result.State, "drainRequested": &result.DrainRequested}) != nil || result.WorkerID != workerID || !result.DrainRequested || !validWorkerState(result.State) {
		return WorkerDrain{}, errors.New("invalid worker drain response")
	}
	return result, nil
}
