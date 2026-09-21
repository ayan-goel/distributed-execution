package workerapi

import (
	"context"
	"slices"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) AcquireWork(ctx context.Context, request *pb.AcquireWorkRequest) (*pb.AcquireWorkResponse, error) {
	identity, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(request, identity.WorkerID); err != nil {
		return nil, err
	}
	result, err := store.AcquireWork(ctx, s.pool, identity, store.AcquisitionRequest{SessionID: request.GetSession().GetSessionId(), RequestID: request.GetRequestId()}, s.policy)
	if err != nil {
		return nil, rpcError(err)
	}
	return acquisitionResponse(result)
}

func acquisitionResponse(result store.AcquisitionResult) (*pb.AcquireWorkResponse, error) {
	outcomes := 0
	if result.Assignment != nil {
		outcomes++
	}
	if result.NoWorkReason != "" {
		outcomes++
	}
	if result.Decision != "" {
		outcomes++
	}
	// An unknown/ambiguous store result must never become a zero-valued grant.
	// Exactly one typed outcome is required at this authority boundary.
	invalid := status.Error(codes.Internal, "INVALID_ACQUISITION_RESULT")
	if outcomes != 1 {
		return nil, invalid
	}
	if result.NoWorkReason != "" {
		value, ok := pb.NoWorkReason_value[result.NoWorkReason]
		if !ok || value == 0 {
			return nil, invalid
		}
		return &pb.AcquireWorkResponse{Outcome: &pb.AcquireWorkResponse_NoWork{NoWork: pb.NoWorkReason(value)}}, nil
	}
	if result.Decision != "" {
		value, ok := pb.Decision_value[result.Decision]
		decision := pb.Decision(value)
		if !ok || !slices.Contains([]pb.Decision{pb.Decision_FENCED, pb.Decision_STOP_REQUESTED, pb.Decision_ALREADY_TERMINAL}, decision) {
			return nil, invalid
		}
		return &pb.AcquireWorkResponse{Outcome: &pb.AcquireWorkResponse_Rejected{Rejected: decision}}, nil
	}
	a := result.Assignment
	// Floor to milliseconds, never round an expired/submillisecond deadline up.
	// The worker must also subtract RPC elapsed time and its local safety margin.
	lease := a.LeaseExpiresAt.Sub(a.ServerTime).Milliseconds()
	phase := a.PhaseDeadline.Sub(a.ServerTime).Milliseconds()
	if lease <= 0 {
		return acquisitionResponse(store.AcquisitionResult{Decision: "FENCED"})
	}
	if phase <= 0 {
		return acquisitionResponse(store.AcquisitionResult{Decision: "STOP_REQUESTED"})
	}
	authority := a.Authority
	resources := a.Job.Spec.Resources
	assignment := &pb.Assignment{
		Authority:   &pb.AttemptAuthority{JobId: authority.JobID, AttemptId: authority.AttemptID, Generation: uint64(authority.Generation), WorkerId: authority.WorkerID, SessionId: authority.SessionID},
		ImageDigest: a.Job.Spec.Image, Argv: append(slices.Clone(a.Job.Spec.Command), a.Job.Spec.Args...),
		Resources:            &pb.Resources{CpuMillis: uint32(resources.CPUMillis), MemoryBytes: uint64(resources.MemoryMiB) << 20, ScratchBytes: uint64(resources.ScratchMiB) << 20},
		CanonicalJobSpecJson: a.CanonicalSpec, SpecSha256: a.SpecHash, LeaseDurationMs: uint64(lease), PhaseRemainingMs: uint64(phase), ServerTimeUnixMs: a.ServerTime.UnixMilli(),
	}
	return &pb.AcquireWorkResponse{Outcome: &pb.AcquireWorkResponse_Assignment{Assignment: assignment}}, nil
}
