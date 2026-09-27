//go:build integration

package store

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestListLogSegmentsScopesAndPaginates(t *testing.T) {
	pool, id, first := verifiedLogFixture(t)
	ctx := context.Background()
	if result, err := RegisterLogSegment(ctx, pool, id, first); err != nil || result.Decision != "ACCEPTED" {
		t.Fatal(result, err)
	}
	second := LogSegmentRequest{Authority: first.Authority, RequestID: uuid.NewString(), ArtifactID: verifiedCatalogArtifact(t, pool, id, first.Authority, "LOG", "stdout"), Stream: "STDOUT", FirstSequence: 4, LastSequence: 4}
	if result, err := RegisterLogSegment(ctx, pool, id, second); err != nil || result.Decision != "ACCEPTED" {
		t.Fatal(result, err)
	}
	stderr := LogSegmentRequest{Authority: first.Authority, RequestID: uuid.NewString(), ArtifactID: verifiedCatalogArtifact(t, pool, id, first.Authority, "LOG", "stderr"), Stream: "STDERR", FirstSequence: 1, LastSequence: 1}
	if result, err := RegisterLogSegment(ctx, pool, id, stderr); err != nil || result.Decision != "ACCEPTED" {
		t.Fatal(result, err)
	}
	var projectID string
	if err := pool.QueryRow(ctx, "SELECT project_id::text FROM jobs WHERE id=$1", first.Authority.JobID).Scan(&projectID); err != nil {
		t.Fatal(err)
	}
	page, err := ListLogSegments(ctx, pool, projectID, first.Authority.AttemptID, "STDOUT", 0, 1)
	if err != nil || !page.More || page.Completion != nil || len(page.Segments) != 1 || page.Segments[0].ArtifactID != first.ArtifactID || page.Segments[0].FirstSequence != 1 || page.Segments[0].LastSequence != 3 {
		t.Fatal("first cursor page", page, err)
	}
	page, err = ListLogSegments(ctx, pool, projectID, first.Authority.AttemptID, "STDOUT", page.Segments[0].LastSequence, 1)
	if err != nil || page.More || len(page.Segments) != 1 || page.Segments[0].ArtifactID != second.ArtifactID || page.Segments[0].FirstSequence != 4 {
		t.Fatal("second cursor page", page, err)
	}
	page, err = ListLogSegments(ctx, pool, projectID, first.Authority.AttemptID, "STDERR", 0, 10)
	if err != nil || len(page.Segments) != 1 || page.Segments[0].ArtifactID != stderr.ArtifactID {
		t.Fatal("stream cursor crossed streams", page, err)
	}
	if _, err := ListLogSegments(ctx, pool, uuid.NewString(), first.Authority.AttemptID, "STDOUT", 0, 10); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign project read logs", err)
	}
	if _, err := ListLogSegments(ctx, pool, projectID, first.Authority.AttemptID, "LOG", 0, 10); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid stream", err)
	}
}

func TestListLogSegmentsReturnsTailGapsOnEmptyFollowPage(t *testing.T) {
	pool, id, r := completedOutputFixture(t)
	ctx := context.Background()
	artifactID := verifiedCatalogArtifact(t, pool, id, r.Authority, "LOG", "stdout")
	segment := LogSegmentRequest{Authority: r.Authority, RequestID: uuid.NewString(), ArtifactID: artifactID, Stream: "STDOUT", FirstSequence: 1, LastSequence: 3, Gaps: []LogSequenceGap{{FirstSequence: 2, LastSequence: 2}}}
	if result, err := RegisterLogSegment(ctx, pool, id, segment); err != nil || result.Decision != "ACCEPTED" {
		t.Fatal(result, err)
	}
	r.LogsComplete = false
	r.Gaps = []CompletionLogGap{{Stream: "stdout", First: 2, Last: 2}, {Stream: "stdout", First: 4, Last: 7}}
	signCompletion(t, &r)
	if result, err := CompleteAttempt(ctx, pool, id, r); err != nil || result.State != "SUCCEEDED" {
		t.Fatal(result, err)
	}
	var projectID string
	if err := pool.QueryRow(ctx, "SELECT project_id::text FROM jobs WHERE id=$1", r.Authority.JobID).Scan(&projectID); err != nil {
		t.Fatal(err)
	}
	page, err := ListLogSegments(ctx, pool, projectID, r.Authority.AttemptID, "STDOUT", 3, 10)
	if err != nil || len(page.Segments) != 0 || page.Completion == nil || page.Completion.LogsComplete || len(page.Completion.Gaps) != 2 || page.Completion.Gaps[1].First != 4 || page.Completion.Gaps[1].Last != 7 {
		t.Fatal("empty follow page hid tail loss", page, err)
	}
}
