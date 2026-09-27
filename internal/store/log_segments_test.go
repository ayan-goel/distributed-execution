package store

import (
	"errors"
	"math"
	"testing"

	"github.com/google/uuid"
)

func validLogSegment() LogSegmentRequest {
	return LogSegmentRequest{
		Authority: AttemptAuthority{JobID: uuid.NewString(), AttemptID: uuid.NewString(), WorkerID: uuid.NewString(), SessionID: uuid.NewString(), Generation: 1},
		RequestID: uuid.NewString(), ArtifactID: uuid.NewString(), Stream: "STDOUT", FirstSequence: 1, LastSequence: 5,
		Gaps: []LogSequenceGap{{FirstSequence: 3, LastSequence: 3}},
	}
}

func TestLogSegmentRequestBindsScopeAndOrderedGaps(t *testing.T) {
	base := validLogSegment()
	_, hash, err := base.normalized(base.Authority.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	replay := base
	replay.RequestID = uuid.NewString()
	if _, other, err := replay.normalized(base.Authority.WorkerID); err != nil || other != hash {
		t.Fatal("request UUID changed content identity", err)
	}
	for _, change := range []func(*LogSegmentRequest){
		func(r *LogSegmentRequest) { r.Stream = "LOG" },
		func(r *LogSegmentRequest) { r.FirstSequence = 0 },
		func(r *LogSegmentRequest) { r.LastSequence = math.MaxInt64 + 1 },
		func(r *LogSegmentRequest) { r.Gaps[0].FirstSequence = 0 },
		func(r *LogSegmentRequest) { r.Gaps[0].LastSequence = 6 },
		func(r *LogSegmentRequest) { r.Gaps[0].LastSequence = 2 },
		func(r *LogSegmentRequest) { r.Gaps = []LogSequenceGap{{1, 5}} },
		func(r *LogSegmentRequest) { r.Gaps = []LogSequenceGap{{2, 3}, {3, 4}} },
		func(r *LogSegmentRequest) { r.ArtifactID = "bad" },
		func(r *LogSegmentRequest) { r.Authority.WorkerID = uuid.NewString() },
	} {
		r := base
		r.Gaps = append([]LogSequenceGap{}, base.Gaps...)
		change(&r)
		if _, _, err := r.normalized(base.Authority.WorkerID); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid log range accepted", r, err)
		}
	}
}
