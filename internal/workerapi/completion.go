package workerapi

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"slices"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) CompleteAttempt(ctx context.Context, r *pb.CompleteAttemptRequest) (*pb.CompleteAttemptResponse, error) {
	id, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(r, id.WorkerID); err != nil {
		return nil, err
	}
	request, err := completionRequest(r)
	if err != nil {
		return nil, rpcError(err)
	}
	// Verified artifact references are already durable. Completion needs no object
	// I/O, so a storage outage cannot prevent an authenticated accepted replay.
	result, err := store.CompleteAttempt(ctx, s.pool, id, request)
	if err != nil {
		return nil, rpcError(err)
	}
	return completionResponse(result)
}

func completionRequest(r *pb.CompleteAttemptRequest) (store.CompletionRequest, error) {
	a := r.GetAuthority()
	if a == nil || a.GetGeneration() > math.MaxInt64 || len(r.GetOutputs()) > 64 || len(r.GetGaps()) > store.MaxCompletionGaps || len(r.GetMetricsJson()) > store.MaxCompletionMetricsBytes {
		return store.CompletionRequest{}, store.ErrInvalid
	}
	reason := r.GetReason().String()
	if r.GetReason() == pb.FailureReason_FAILURE_REASON_UNSPECIFIED {
		reason = ""
	}
	// Exit zero is observed success; absent exit is unknown. Preserve presence so
	// the store can compare completion evidence against the saved phase transition.
	request := store.CompletionRequest{Authority: store.AttemptAuthority{JobID: a.JobId, AttemptID: a.AttemptId, Generation: int64(a.Generation), WorkerID: a.WorkerId, SessionID: a.SessionId}, CompletionID: r.GetCompletionId(), PayloadSHA256: r.GetPayloadSha256(), ExitCode: r.ExitCode, Reason: reason, Stopped: r.GetStopped(), LogsComplete: r.GetLogsComplete(), MetricsJSON: r.GetMetricsJson()}
	for _, o := range r.GetOutputs() {
		if o == nil {
			return store.CompletionRequest{}, store.ErrInvalid
		}
		request.Outputs = append(request.Outputs, store.CompletionOutput{Name: o.Name, ArtifactID: o.ArtifactId})
	}
	for _, g := range r.GetGaps() {
		if g == nil || g.FirstSequence > math.MaxInt64 || g.LastSequence > math.MaxInt64 {
			return store.CompletionRequest{}, store.ErrInvalid
		}
		var stream string
		switch g.Stream {
		case pb.LogStream_STDOUT:
			stream = "stdout"
		case pb.LogStream_STDERR:
			stream = "stderr"
		default:
			return store.CompletionRequest{}, store.ErrInvalid
		}
		request.Gaps = append(request.Gaps, store.CompletionLogGap{Stream: stream, First: int64(g.FirstSequence), Last: int64(g.LastSequence)})
	}
	return request, nil
}

func completionResponse(r store.CompletionResult) (*pb.CompleteAttemptResponse, error) {
	stateValue, knownState := pb.AttemptState_value[r.State]
	state := pb.AttemptState(stateValue)
	active := slices.Contains([]pb.AttemptState{pb.AttemptState_ASSIGNED, pb.AttemptState_STARTING, pb.AttemptState_RUNNING, pb.AttemptState_FINALIZING}, state)
	terminal := slices.Contains([]pb.AttemptState{pb.AttemptState_SUCCEEDED, pb.AttemptState_FAILED, pb.AttemptState_CANCELLED, pb.AttemptState_LOST}, state)
	decisionValue, knownDecision := pb.Decision_value[r.Decision]
	decision := pb.Decision(decisionValue)
	valid := knownDecision && knownState
	switch decision {
	case pb.Decision_ACCEPTED:
		valid = valid && terminal && state != pb.AttemptState_LOST
	case pb.Decision_ALREADY_TERMINAL:
		valid = valid && terminal
	case pb.Decision_FENCED:
		valid = knownDecision && (knownState && active || r.State == "")
	case pb.Decision_STOP_REQUESTED:
		valid = valid && active
	default:
		valid = false
	}
	// Only accepted success may carry the canonical manifest. Reject ambiguous
	// internal results instead of turning missing evidence into wire-level success.
	if decision == pb.Decision_ACCEPTED && state == pb.AttemptState_SUCCEEDED {
		body := bytes.TrimSpace(r.Manifest)
		valid = valid && len(r.Manifest) <= 2<<20 && len(body) > 0 && body[0] == '{' && json.Valid(body)
	} else {
		valid = valid && len(r.Manifest) == 0
	}
	if !valid {
		return nil, status.Error(codes.Internal, "INVALID_COMPLETION_RESULT")
	}
	return &pb.CompleteAttemptResponse{Decision: decision, State: state, AcceptedManifestJson: r.Manifest}, nil
}
