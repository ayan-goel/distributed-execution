package workerapi

import (
	"context"
	"math"
	"strings"
	"testing"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func validCompletionWire() *pb.CompleteAttemptRequest {
	exit := int32(0)
	return &pb.CompleteAttemptRequest{Authority: &pb.AttemptAuthority{JobId: uuid.NewString(), AttemptId: uuid.NewString(), WorkerId: uuid.NewString(), SessionId: uuid.NewString(), Generation: 1}, CompletionId: uuid.NewString(), ExitCode: &exit, Stopped: true, LogsComplete: true}
}

func TestCompletionRequiresIdentityAndValidEvidence(t *testing.T) {
	s := NewService(nil, store.AcquisitionPolicy{}, nil)
	if _, err := s.CompleteAttempt(context.Background(), nil); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
	base := validCompletionWire()
	id := store.WorkerIdentity{WorkerID: base.Authority.WorkerId, CredentialID: uuid.NewString()}
	ctx := context.WithValue(context.Background(), identityKey{}, id)
	for name, change := range map[string]func(*pb.CompleteAttemptRequest){
		"authority":        func(r *pb.CompleteAttemptRequest) { r.Authority = nil },
		"overflow":         func(r *pb.CompleteAttemptRequest) { r.Authority.Generation = math.MaxUint64 },
		"unknown reason":   func(r *pb.CompleteAttemptRequest) { r.Reason = 999 },
		"server reason":    func(r *pb.CompleteAttemptRequest) { r.Reason = pb.FailureReason_WORKER_LOST },
		"absent exit":      func(r *pb.CompleteAttemptRequest) { r.ExitCode = nil },
		"unconfirmed stop": func(r *pb.CompleteAttemptRequest) { r.Stopped = false },
		"nil output":       func(r *pb.CompleteAttemptRequest) { r.Outputs = []*pb.OutputReference{nil} },
		"outputs bound":    func(r *pb.CompleteAttemptRequest) { r.Outputs = make([]*pb.OutputReference, 65) },
		"gap stream": func(r *pb.CompleteAttemptRequest) {
			r.LogsComplete = false
			r.Gaps = []*pb.LogGap{{Stream: 999, FirstSequence: 1, LastSequence: 2}}
		},
		"gap overflow": func(r *pb.CompleteAttemptRequest) {
			r.LogsComplete = false
			r.Gaps = []*pb.LogGap{{Stream: pb.LogStream_STDOUT, FirstSequence: 1, LastSequence: math.MaxUint64}}
		},
		"nil gap":    func(r *pb.CompleteAttemptRequest) { r.Gaps = []*pb.LogGap{nil} },
		"gaps bound": func(r *pb.CompleteAttemptRequest) { r.Gaps = make([]*pb.LogGap, store.MaxCompletionGaps+1) },
		"metrics bound": func(r *pb.CompleteAttemptRequest) {
			r.MetricsJson = []byte(strings.Repeat(" ", store.MaxCompletionMetricsBytes+1))
		},
		"uuid":   func(r *pb.CompleteAttemptRequest) { r.CompletionId = "invalid" },
		"digest": func(r *pb.CompleteAttemptRequest) { r.PayloadSha256 = strings.Repeat("a", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			r := proto.Clone(base).(*pb.CompleteAttemptRequest)
			change(r)
			if _, err := s.CompleteAttempt(ctx, r); status.Code(err) != codes.InvalidArgument {
				t.Fatal("invalid evidence crossed boundary", err)
			}
		})
	}
	spoofed := proto.Clone(base).(*pb.CompleteAttemptRequest)
	spoofed.Authority.WorkerId = uuid.NewString()
	if _, err := s.CompleteAttempt(ctx, spoofed); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
}

func TestCompletionWirePreservesPresenceAndUnsignedBoundaries(t *testing.T) {
	wire := validCompletionWire()
	wire.Authority.Generation = math.MaxInt64
	wire.LogsComplete = false
	wire.Gaps = []*pb.LogGap{{Stream: pb.LogStream_STDERR, FirstSequence: 1, LastSequence: math.MaxInt64}}
	wire.Outputs = []*pb.OutputReference{{Name: "result", ArtifactId: uuid.NewString()}}
	wire.MetricsJson = []byte(`{"score":9007199254740993}`)
	r, err := completionRequest(wire)
	if err != nil || r.ExitCode == nil || *r.ExitCode != 0 || r.Authority.Generation != math.MaxInt64 || r.Reason != "" || r.Gaps[0].Last != math.MaxInt64 || r.Gaps[0].Stream != "stderr" || string(r.MetricsJSON) != string(wire.MetricsJson) || r.Outputs[0].ArtifactID != wire.Outputs[0].ArtifactId {
		t.Fatal(r, err)
	}
	wire.ExitCode = nil
	wire.Reason = pb.FailureReason_RUNTIME_UNAVAILABLE
	r, err = completionRequest(wire)
	if err != nil || r.ExitCode != nil || r.Reason != "RUNTIME_UNAVAILABLE" {
		t.Fatal("exit presence changed", r, err)
	}
}

func TestCompletionResponseRejectsAmbiguousPublication(t *testing.T) {
	for _, r := range []store.CompletionResult{
		{Decision: "ACCEPTED", State: "SUCCEEDED", Manifest: []byte(`{"version":1}`)},
		{Decision: "ACCEPTED", State: "FAILED"}, {Decision: "ACCEPTED", State: "CANCELLED"},
		{Decision: "ALREADY_TERMINAL", State: "LOST"}, {Decision: "ALREADY_TERMINAL", State: "SUCCEEDED"},
		{Decision: "FENCED"}, {Decision: "FENCED", State: "RUNNING"}, {Decision: "STOP_REQUESTED", State: "FINALIZING"},
	} {
		got, err := completionResponse(r)
		if err != nil || got.GetDecision().String() != r.Decision || r.State != "" && got.GetState().String() != r.State || string(got.GetAcceptedManifestJson()) != string(r.Manifest) {
			t.Fatal(got, err)
		}
	}
	for _, r := range []store.CompletionResult{
		{}, {Decision: "ACCEPTED", State: "RUNNING"}, {Decision: "ACCEPTED", State: "LOST"},
		{Decision: "ACCEPTED", State: "SUCCEEDED"}, {Decision: "ACCEPTED", State: "SUCCEEDED", Manifest: []byte(`null`)},
		{Decision: "ACCEPTED", State: "SUCCEEDED", Manifest: []byte(`[]`)},
		{Decision: "ACCEPTED", State: "SUCCEEDED", Manifest: []byte(`{"broken"`)},
		{Decision: "ACCEPTED", State: "FAILED", Manifest: []byte(`{}`)},
		{Decision: "FENCED", State: "SUCCEEDED"}, {Decision: "STOP_REQUESTED"},
		{Decision: "ALREADY_TERMINAL", State: "RUNNING"}, {Decision: "ALREADY_TERMINAL", State: "SUCCEEDED", Manifest: []byte(`{}`)},
		{Decision: "future", State: "FAILED"}, {Decision: "FENCED", State: "future"},
		{Decision: "ACCEPTED", State: "SUCCEEDED", Manifest: []byte(`{"x":"` + strings.Repeat("a", 2<<20) + `"}`)},
	} {
		if _, err := completionResponse(r); status.Code(err) != codes.Internal {
			t.Fatal("invalid publication result accepted", r.Decision, r.State, err)
		}
	}
}
