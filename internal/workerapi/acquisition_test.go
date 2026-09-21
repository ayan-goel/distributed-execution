package workerapi

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAcquisitionRequiresTransportIdentity(t *testing.T) {
	if _, err := NewService(nil, store.AcquisitionPolicy{}).AcquireWork(context.Background(), &pb.AcquireWorkRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatal("acquisition bypassed transport identity", err)
	}
	if _, err := NewService(nil, store.AcquisitionPolicy{}).ListAssignments(context.Background(), &pb.ListAssignmentsRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatal("assignment recovery bypassed transport identity", err)
	}
}

func TestAssignmentPageEnforcesEncodedMessageLimit(t *testing.T) {
	now := time.Now()
	a := store.WorkAssignment{CanonicalSpec: []byte(strings.Repeat("x", 2<<20)), ServerTime: now, LeaseExpiresAt: now.Add(time.Second), PhaseDeadline: now.Add(time.Second)}
	if _, err := assignmentPageResponse(store.AssignmentPage{Assignments: []store.WorkAssignment{a, a}}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversized inventory escaped the message bound", err)
	}
}

func TestAcquisitionWirePreservesAuthorityAndRemainingDeadlines(t *testing.T) {
	now := time.Unix(100, 0)
	a := store.WorkAssignment{Authority: store.AttemptAuthority{JobID: "job", AttemptID: "attempt", WorkerID: "worker", SessionID: "session", Generation: 2},
		Job:           spec.Job{Spec: spec.JobSpec{Image: "image@sha256:digest", Command: []string{"python", "run.py"}, Args: []string{"--seed", "1"}, Resources: spec.Resources{CPUMillis: 2000, MemoryMiB: 4096, ScratchMiB: 8192}}},
		CanonicalSpec: []byte(`{"spec":"fixture"}`), SpecHash: "hash", ServerTime: now, LeaseExpiresAt: now.Add(12500 * time.Millisecond), PhaseDeadline: now.Add(42500 * time.Millisecond)}
	response, err := acquisitionResponse(store.AcquisitionResult{Assignment: &a})
	if err != nil {
		t.Fatal(err)
	}
	wire := response.GetAssignment()
	if wire == nil || wire.GetAuthority().GetGeneration() != 2 || wire.GetAuthority().GetAttemptId() != "attempt" || wire.LeaseDurationMs != 12500 || wire.PhaseRemainingMs != 42500 || wire.ServerTimeUnixMs != now.UnixMilli() {
		t.Fatal("wire lost remaining authority", response)
	}
	if wire.Resources.MemoryBytes != 4096<<20 || wire.Resources.ScratchBytes != 8192<<20 || len(wire.Argv) != 4 || wire.Argv[2] != "--seed" || wire.SpecSha256 != "hash" || string(wire.CanonicalJobSpecJson) != string(a.CanonicalSpec) {
		t.Fatal("wire changed execution spec", wire)
	}
	a.ServerTime = a.LeaseExpiresAt
	if response, err := acquisitionResponse(store.AcquisitionResult{Assignment: &a}); err != nil || response.GetRejected() != pb.Decision_FENCED {
		t.Fatal("expired duration underflowed", response, err)
	}
	a.ServerTime = now
	a.PhaseDeadline = now.Add(500 * time.Microsecond)
	if response, err := acquisitionResponse(store.AcquisitionResult{Assignment: &a}); err != nil || response.GetRejected() != pb.Decision_STOP_REQUESTED {
		t.Fatal("submillisecond phase granted execution", response, err)
	}
}

func TestAcquisitionWireRejectsUnrecognizedOutcomes(t *testing.T) {
	for _, result := range []store.AcquisitionResult{{}, {NoWorkReason: "new-unknown-reason"}, {Decision: "new-unknown-decision"}, {NoWorkReason: "QUEUE_EMPTY", Decision: "FENCED"}} {
		if _, err := acquisitionResponse(result); status.Code(err) != codes.Internal {
			t.Fatal("invalid result became wire authority", result, err)
		}
	}
}
