package workerapi

import (
	"context"
	"math"
	"slices"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) RenewLeases(ctx context.Context, request *pb.RenewLeasesRequest) (*pb.RenewLeasesResponse, error) {
	identity, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(request, identity.WorkerID); err != nil {
		return nil, err
	}
	if len(request.GetAttempts()) < 1 || len(request.GetAttempts()) > store.MaxLeaseRenewalBatch {
		return nil, rpcError(store.ErrInvalid)
	}
	r := store.LeaseRenewal{SessionID: request.GetSession().GetSessionId(), RequestID: request.GetRequestId()}
	for _, a := range request.GetAttempts() {
		// PostgreSQL generations are signed bigints. Validate before conversion
		// so a large wire value cannot wrap into a different fencing generation.
		if a == nil || a.GetGeneration() > math.MaxInt64 {
			return nil, rpcError(store.ErrInvalid)
		}
		r.Attempts = append(r.Attempts, store.AttemptAuthority{JobID: a.GetJobId(), AttemptID: a.GetAttemptId(), WorkerID: a.GetWorkerId(), SessionID: a.GetSessionId(), Generation: int64(a.GetGeneration())})
	}
	grants, err := store.RenewLeases(ctx, s.pool, identity, r)
	if err != nil {
		return nil, rpcError(err)
	}
	return leaseResponse(grants)
}

func leaseResponse(grants []store.LeaseGrant) (*pb.RenewLeasesResponse, error) {
	invalid := status.Error(codes.Internal, "INVALID_LEASE_RESULT")
	if len(grants) < 1 || len(grants) > store.MaxLeaseRenewalBatch {
		return nil, invalid
	}
	response := &pb.RenewLeasesResponse{}
	for _, g := range grants {
		value, ok := pb.Decision_value[g.Decision]
		decision := pb.Decision(value)
		if !ok || !slices.Contains([]pb.Decision{pb.Decision_ACCEPTED, pb.Decision_FENCED, pb.Decision_STOP_REQUESTED, pb.Decision_ALREADY_TERMINAL}, decision) || g.Authority.Generation < 1 || g.ServerTime.IsZero() {
			return nil, invalid
		}
		a := g.Authority
		wire := &pb.LeaseResult{Authority: &pb.AttemptAuthority{JobId: a.JobID, AttemptId: a.AttemptID, WorkerId: a.WorkerID, SessionId: a.SessionID, Generation: uint64(a.Generation)}, Decision: decision, ServerTimeUnixMs: g.ServerTime.UnixMilli()}
		if decision == pb.Decision_ACCEPTED {
			// Floor signed durations before converting to uint64. An expired or
			// submillisecond grant must never wrap or round up into live authority.
			lease := g.LeaseExpiresAt.Sub(g.ServerTime)
			phase := g.PhaseDeadline.Sub(g.ServerTime).Milliseconds()
			if lease > store.InitialLease {
				return nil, invalid
			}
			switch {
			case lease.Milliseconds() <= 0:
				wire.Decision = pb.Decision_FENCED
			case phase <= 0:
				wire.Decision = pb.Decision_STOP_REQUESTED
			default:
				wire.RemainingMs = uint64(lease.Milliseconds())
				wire.PhaseRemainingMs = uint64(phase)
			}
		}
		// Rejection results retain identity for cleanup but expose no deadlines.
		response.Results = append(response.Results, wire)
	}
	return response, nil
}
