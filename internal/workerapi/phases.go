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

func (s *Service) ReportPhase(ctx context.Context, r *pb.ReportPhaseRequest) (*pb.MutationResponse, error) {
	id, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(r, id.WorkerID); err != nil {
		return nil, err
	}
	a := r.GetAuthority()
	if a.GetGeneration() > math.MaxInt64 || !slices.Contains([]pb.AttemptState{pb.AttemptState_STARTING, pb.AttemptState_RUNNING, pb.AttemptState_FINALIZING}, r.GetPhase()) {
		return nil, rpcError(store.ErrInvalid)
	}
	// Preserve optional presence: exit status zero means a confirmed successful
	// process exit, while nil means no exit was observed and cannot finalize.
	result, err := store.ReportPhase(ctx, s.pool, id, store.PhaseReport{Authority: store.AttemptAuthority{JobID: a.GetJobId(), AttemptID: a.GetAttemptId(), WorkerID: a.GetWorkerId(), SessionID: a.GetSessionId(), Generation: int64(a.GetGeneration())}, EventID: r.GetEventId(), Phase: r.GetPhase().String(), ContainerID: r.GetContainerId(), ExitCode: r.ExitCode})
	if err != nil {
		return nil, rpcError(err)
	}
	return phaseResponse(result)
}

func phaseResponse(r store.MutationResult) (*pb.MutationResponse, error) {
	stateValue, knownState := pb.AttemptState_value[r.State]
	state := pb.AttemptState(stateValue)
	active := slices.Contains([]pb.AttemptState{pb.AttemptState_ASSIGNED, pb.AttemptState_STARTING, pb.AttemptState_RUNNING, pb.AttemptState_FINALIZING}, state)
	terminal := slices.Contains([]pb.AttemptState{pb.AttemptState_SUCCEEDED, pb.AttemptState_FAILED, pb.AttemptState_LOST, pb.AttemptState_CANCELLED}, state)
	decisionValue, knownDecision := pb.Decision_value[r.Decision]
	decision := pb.Decision(decisionValue)
	valid := knownDecision && knownState
	switch decision {
	case pb.Decision_ACCEPTED:
		valid = valid && active && state != pb.AttemptState_ASSIGNED
	case pb.Decision_FENCED:
		valid = (valid && active) || r.State == ""
	case pb.Decision_STOP_REQUESTED:
		valid = valid && active
	case pb.Decision_ALREADY_TERMINAL:
		valid = valid && terminal
	default:
		valid = false
	}
	// Only a fenced unknown tuple may omit state. Never turn a malformed store
	// outcome into an acknowledgement or a terminal result on the wire.
	if !valid {
		return nil, status.Error(codes.Internal, "INVALID_PHASE_RESULT")
	}
	return &pb.MutationResponse{Decision: decision, State: state}, nil
}
