package workerapi

import (
	"context"
	"testing"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPhaseRequiresTransportIdentity(t *testing.T) {
	if _, err := NewService(nil, store.AcquisitionPolicy{}).ReportPhase(context.Background(), &pb.ReportPhaseRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
}

func TestPhaseWireValidatesDecisionAndState(t *testing.T) {
	for _, tc := range []struct {
		decision, state string
		wireDecision    pb.Decision
		wireState       pb.AttemptState
	}{
		{"ACCEPTED", "STARTING", pb.Decision_ACCEPTED, pb.AttemptState_STARTING},
		{"ACCEPTED", "RUNNING", pb.Decision_ACCEPTED, pb.AttemptState_RUNNING},
		{"ACCEPTED", "FINALIZING", pb.Decision_ACCEPTED, pb.AttemptState_FINALIZING},
		{"FENCED", "", pb.Decision_FENCED, pb.AttemptState_ATTEMPT_STATE_UNSPECIFIED},
		{"FENCED", "ASSIGNED", pb.Decision_FENCED, pb.AttemptState_ASSIGNED},
		{"STOP_REQUESTED", "RUNNING", pb.Decision_STOP_REQUESTED, pb.AttemptState_RUNNING},
		{"ALREADY_TERMINAL", "LOST", pb.Decision_ALREADY_TERMINAL, pb.AttemptState_LOST},
	} {
		response, err := phaseResponse(store.MutationResult{Decision: tc.decision, State: tc.state})
		if err != nil || response.Decision != tc.wireDecision || response.State != tc.wireState {
			t.Fatal(response, err)
		}
	}
	for _, r := range []store.MutationResult{
		{}, {Decision: "NEW_DECISION", State: "RUNNING"}, {Decision: "ACCEPTED", State: "NEW_STATE"},
		{Decision: "ACCEPTED", State: ""}, {Decision: "ACCEPTED", State: "SUCCEEDED"}, {Decision: "ACCEPTED", State: "ASSIGNED"},
		{Decision: "STOP_REQUESTED", State: ""}, {Decision: "ALREADY_TERMINAL", State: "RUNNING"}, {Decision: "FENCED", State: "SUCCEEDED"},
	} {
		if _, err := phaseResponse(r); status.Code(err) != codes.Internal {
			t.Fatal("ambiguous phase response accepted", r, err)
		}
	}
}
