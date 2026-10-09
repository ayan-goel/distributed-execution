package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/spec"
)

type Sweep struct {
	ID            string          `json:"id"`
	ProjectID     string          `json:"projectId"`
	Spec          json.RawMessage `json:"spec"`
	SpecHash      string          `json:"specHash"`
	ChildIDs      []string        `json:"childIds"`
	MaxConcurrent int             `json:"maxConcurrent"`
	CreatedAt     time.Time       `json:"createdAt"`
}

func (c *Client) SubmitSweep(ctx context.Context, body []byte, key string) (Sweep, error) {
	if len(body) == 0 || len(body) > spec.MaxDocumentBytes || !visibleASCII(key, 128) {
		return Sweep{}, errors.New("sweep submission requires a bounded specification and a 1–128 character visible ASCII idempotency key")
	}
	requested, err := spec.DecodeSweep(bytes.NewReader(body))
	if err != nil {
		return Sweep{}, err
	}
	response, err := c.requestBody(ctx, http.MethodPost, "/v1/sweeps", body, key)
	if err != nil {
		return Sweep{}, err
	}
	var result Sweep
	invalid := errors.New("invalid sweep submission response")
	if json.Unmarshal(response, &result) != nil || !downloadUUID(result.ID) || !downloadUUID(result.ProjectID) || result.CreatedAt.IsZero() ||
		len(result.ChildIDs) < 1 || len(result.ChildIDs) > spec.MaxSweepJobs || !objectSHA.MatchString(result.SpecHash) {
		return Sweep{}, invalid
	}
	seen := make(map[string]bool, len(result.ChildIDs))
	for _, id := range result.ChildIDs {
		if !downloadUUID(id) || seen[id] {
			return Sweep{}, invalid
		}
		seen[id] = true
	}
	var resolved spec.Sweep
	if json.Unmarshal(result.Spec, &resolved) != nil {
		return Sweep{}, invalid
	}
	_, digest, ok := strings.Cut(resolved.Spec.JobTemplate.Spec.Image, "@sha256:")
	if !ok || !objectSHA.MatchString(digest) {
		return Sweep{}, invalid
	}
	// The server may resolve the image, but must preserve every other submitted
	// field. Compare canonical identities before trusting its child declaration.
	requested.Spec.JobTemplate.Spec.Image = resolved.Spec.JobTemplate.Spec.Image
	_, expectedHash, err := requested.Canonical()
	if err != nil || expectedHash != result.SpecHash || result.MaxConcurrent != requested.Spec.MaxConcurrent {
		return Sweep{}, invalid
	}
	_, resolvedHash, err := resolved.Canonical()
	if err != nil || resolvedHash != result.SpecHash {
		return Sweep{}, invalid
	}
	count := 1
	for _, values := range requested.Spec.Matrix {
		count *= len(values)
	}
	if count != len(result.ChildIDs) {
		return Sweep{}, invalid
	}
	return result, nil
}
