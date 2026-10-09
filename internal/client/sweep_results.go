package client

import (
	"context"
	"encoding/json"
	"errors"
)

const MaxSweepExportBytes = 32 << 20

type SweepResults struct {
	SweepID   string       `json:"sweepId"`
	ProjectID string       `json:"projectId"`
	SpecHash  string       `json:"specHash"`
	Children  []SweepChild `json:"children"`
}

func (c *Client) CollectSweep(ctx context.Context, id string) (SweepResults, error) {
	result := SweepResults{Children: []SweepChild{}}
	var first SweepSummary
	cursor := ""
	seenIDs, seenCursors := map[string]bool{}, map[string]bool{}
	used := 1024
	for {
		page, err := c.GetSweep(ctx, id, cursor, 100)
		if err != nil {
			return SweepResults{}, err
		}
		if cursor == "" {
			first = page.Sweep
			result.SweepID, result.ProjectID, result.SpecHash = first.ID, first.ProjectID, first.SpecHash
		}
		s := page.Sweep
		// State/counts can advance between page snapshots; membership and policy
		// cannot. Reject changed identity before assembling a misleading export.
		if s.ProjectID != first.ProjectID || s.Name != first.Name || s.SpecHash != first.SpecHash || !s.CreatedAt.Equal(first.CreatedAt) ||
			s.MaxConcurrent != first.MaxConcurrent || s.FailFast != first.FailFast || s.CancelRunningOnFailure != first.CancelRunningOnFailure || s.Progress.Total != first.Progress.Total {
			return SweepResults{}, errors.New("sweep identity changed between export pages")
		}
		for _, child := range page.Children {
			if child.Index != len(result.Children) || seenIDs[child.ID] {
				return SweepResults{}, errors.New("sweep export repeated or skipped a child")
			}
			body, err := json.Marshal(child)
			if err != nil {
				return SweepResults{}, err
			}
			used += len(body) + 1
			// Buffer before writing so a late page failure cannot look like a
			// successful export. Cap the buffer for wide matrices and metrics.
			if used > MaxSweepExportBytes {
				return SweepResults{}, errors.New("sweep export exceeds 32 MiB; inspect bounded pages instead")
			}
			seenIDs[child.ID] = true
			result.Children = append(result.Children, child)
		}
		if !page.HasMore {
			if len(result.Children) != first.Progress.Total {
				return SweepResults{}, errors.New("sweep export ended before every child was read")
			}
			return result, nil
		}
		if len(result.Children) >= first.Progress.Total || seenCursors[page.NextCursor] {
			return SweepResults{}, errors.New("sweep export continuation did not advance")
		}
		seenCursors[page.NextCursor] = true
		cursor = page.NextCursor
	}
}
