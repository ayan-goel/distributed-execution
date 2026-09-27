package store

import (
	"context"
	"encoding/json"
	"math"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxLogPageSize = 100

type LogSegment struct {
	ArtifactID    string           `json:"artifactId"`
	Stream        string           `json:"stream"`
	FirstSequence uint64           `json:"firstSequence"`
	LastSequence  uint64           `json:"lastSequence"`
	Gaps          []LogSequenceGap `json:"gaps"`
	Object        ArtifactObject   `json:"object"`
	RegisteredAt  time.Time        `json:"registeredAt"`
}

type LogSegmentPage struct {
	Segments []LogSegment
	More     bool
}

// ListLogSegments reads only verified objects bound to an attempt in the
// caller's project. Append-only ranges make the last sequence a stable cursor.
func ListLogSegments(ctx context.Context, pool *pgxpool.Pool, projectID, attemptID, stream string, after uint64, limit int) (LogSegmentPage, error) {
	if !canonicalUUID(projectID) || !canonicalUUID(attemptID) || !slices.Contains([]string{"STDOUT", "STDERR"}, stream) || after > math.MaxInt64 || limit < 1 || limit > MaxLogPageSize {
		return LogSegmentPage{}, ErrInvalid
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE a.id=$1 AND j.project_id=$2)`, attemptID, projectID).Scan(&exists); err != nil {
		return LogSegmentPage{}, err
	}
	if !exists {
		return LogSegmentPage{}, ErrNotFound
	}
	rows, err := pool.Query(ctx, `SELECT s.artifact_id::text,s.first_sequence,s.last_sequence,s.gaps,s.created_at,u.object_key,a.object_version,u.size_bytes,u.sha256
		FROM log_segments s JOIN artifacts a ON a.id=s.artifact_id JOIN artifact_uploads u ON u.upload_id=s.upload_id
		JOIN attempts at ON at.id=s.attempt_id JOIN jobs j ON j.id=at.job_id
		WHERE s.attempt_id=$1 AND j.project_id=$2 AND s.stream=$3 AND s.last_sequence>$4
		ORDER BY s.first_sequence LIMIT $5`, attemptID, projectID, stream, int64(after), limit+1)
	if err != nil {
		return LogSegmentPage{}, err
	}
	defer rows.Close()
	page := LogSegmentPage{Segments: []LogSegment{}}
	for rows.Next() {
		var segment LogSegment
		var first, last int64
		var gapJSON []byte
		if err := rows.Scan(&segment.ArtifactID, &first, &last, &gapJSON, &segment.RegisteredAt, &segment.Object.Key, &segment.Object.Version, &segment.Object.SizeBytes, &segment.Object.SHA256); err != nil {
			return LogSegmentPage{}, err
		}
		if err := json.Unmarshal(gapJSON, &segment.Gaps); err != nil {
			return LogSegmentPage{}, err
		}
		segment.Stream, segment.FirstSequence, segment.LastSequence = stream, uint64(first), uint64(last)
		page.Segments = append(page.Segments, segment)
	}
	if err := rows.Err(); err != nil {
		return LogSegmentPage{}, err
	}
	if len(page.Segments) > limit {
		page.More = true
		page.Segments = page.Segments[:limit]
	}
	return page, nil
}
