package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"dispatch.local/dispatch/internal/spec"
)

type SweepRetryChild struct {
	ID          string `json:"id"`
	Index       int    `json:"index"`
	ParentJobID string `json:"parentJobId"`
}

type SweepRetry struct {
	ID            string            `json:"id"`
	ProjectID     string            `json:"projectId"`
	ParentSweepID string            `json:"parentSweepId"`
	SpecHash      string            `json:"specHash"`
	MaxConcurrent int               `json:"maxConcurrent"`
	Children      []SweepRetryChild `json:"children"`
	CreatedAt     time.Time         `json:"createdAt"`
}

func (c *Client) RetrySweep(ctx context.Context, sourceID, key string) (SweepRetry, error) {
	if !downloadUUID(sourceID) || !visibleASCII(key, 128) {
		return SweepRetry{}, errors.New("sweep retry requires a canonical UUID and a 1–128 character visible ASCII idempotency key")
	}
	body, err := c.requestBody(ctx, http.MethodPost, "/v1/sweeps/"+sourceID+"/retry", nil, key)
	if err != nil {
		return SweepRetry{}, err
	}
	// Missing indices must not silently become zero, which would accept an
	// incomplete mapping for the first child or a one-child retry.
	var wire struct {
		SweepRetry
		Children []struct {
			SweepRetryChild
			Index *int `json:"index"`
		} `json:"children"`
	}
	invalid := errors.New("invalid sweep retry response")
	if json.Unmarshal(body, &wire) != nil || !downloadUUID(wire.ID) || wire.ID == sourceID || !downloadUUID(wire.ProjectID) ||
		wire.ParentSweepID != sourceID || wire.CreatedAt.IsZero() || !objectSHA.MatchString(wire.SpecHash) ||
		wire.MaxConcurrent < 1 || wire.MaxConcurrent > spec.MaxSweepJobs || len(wire.Children) < 1 || len(wire.Children) > spec.MaxSweepJobs {
		return SweepRetry{}, invalid
	}
	result := wire.SweepRetry
	result.Children = make([]SweepRetryChild, 0, len(wire.Children))
	newIDs, parentIDs := map[string]bool{}, map[string]bool{}
	for i, item := range wire.Children {
		child := item.SweepRetryChild
		if item.Index == nil || *item.Index != i || !downloadUUID(child.ID) || !downloadUUID(child.ParentJobID) ||
			newIDs[child.ID] || parentIDs[child.ParentJobID] || child.ID == sourceID || child.ID == result.ID ||
			child.ParentJobID == sourceID || child.ParentJobID == result.ID {
			return SweepRetry{}, invalid
		}
		child.Index = *item.Index
		newIDs[child.ID], parentIDs[child.ParentJobID] = true, true
		result.Children = append(result.Children, child)
	}
	// A fresh job cannot alias any source job, including one listed later in
	// the response. Check both complete identity sets before exposing the mapping.
	for id := range newIDs {
		if parentIDs[id] {
			return SweepRetry{}, invalid
		}
	}
	return result, nil
}
