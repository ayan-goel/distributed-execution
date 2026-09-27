package workerapi

import (
	"context"
	"math"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) RegisterLogSegment(ctx context.Context, r *pb.RegisterLogSegmentRequest) (*pb.MutationResponse, error) {
	id, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(r, id.WorkerID); err != nil {
		return nil, err
	}
	a := r.GetAuthority()
	if a.GetGeneration() > math.MaxInt64 || len(r.GetGaps()) > store.MaxLogSegmentGaps {
		return nil, rpcError(store.ErrInvalid)
	}
	request := store.LogSegmentRequest{
		Authority: store.AttemptAuthority{JobID: a.GetJobId(), AttemptID: a.GetAttemptId(), WorkerID: a.GetWorkerId(), SessionID: a.GetSessionId(), Generation: int64(a.GetGeneration())},
		RequestID: r.GetRequestId(), ArtifactID: r.GetArtifactId(), Stream: r.GetStream().String(),
		FirstSequence: r.GetFirstSequence(), LastSequence: r.GetLastSequence(),
	}
	for _, gap := range r.GetGaps() {
		if gap == nil || gap.GetStream() != r.GetStream() {
			return nil, rpcError(store.ErrInvalid)
		}
		request.Gaps = append(request.Gaps, store.LogSequenceGap{FirstSequence: gap.GetFirstSequence(), LastSequence: gap.GetLastSequence()})
	}
	// The store joins the artifact to the exact verified LOG upload and attempt;
	// a client-supplied artifact ID alone is never publication authority.
	result, err := store.RegisterLogSegment(ctx, s.pool, id, request)
	if err != nil {
		return nil, rpcError(err)
	}
	if result.Decision == "ACCEPTED" {
		state, known := pb.AttemptState_value[result.State]
		if !known || result.State == "ASSIGNED" {
			return nil, status.Error(codes.Internal, "INVALID_LOG_RESULT")
		}
		// Exact replay may retain its acceptance after the attempt terminalizes.
		return &pb.MutationResponse{Decision: pb.Decision_ACCEPTED, State: pb.AttemptState(state)}, nil
	}
	return phaseResponse(result)
}
