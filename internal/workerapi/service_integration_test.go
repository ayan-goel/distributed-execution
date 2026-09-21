//go:build integration

package workerapi

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"math"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMTLSRegistrationHeartbeatRestartAndTakeover(t *testing.T) {
	pool := workerTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca, roots := testCA(t)
	serverCert := testLeaf(t, ca, x509.ExtKeyUsageServerAuth)
	cert := testLeaf(t, ca, x509.ExtKeyUsageClientAuth)
	policy := store.WorkerProvision{Name: "rpc-host", CertificateSHA256: sha256.Sum256(cert.Certificate[0]), Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4, Labels: map[string]string{"os": "linux", "architecture": "arm64"}, Projects: []string{"research"}}
	id, err := store.ProvisionWorker(ctx, pool, policy)
	if err != nil {
		t.Fatal(err)
	}
	start := func() (pb.WorkerServiceClient, func()) {
		t.Helper()
		server, err := NewServer(pool, serverCert, roots, NewService(pool, store.AcquisitionPolicy{}))
		if err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- server.Serve(listener) }()
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})))
		if err != nil {
			server.Stop()
			t.Fatal(err)
		}
		stopped := false
		stop := func() {
			if stopped {
				return
			}
			stopped = true
			_ = conn.Close()
			server.Stop()
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
		t.Cleanup(stop)
		return pb.NewWorkerServiceClient(conn), stop
	}
	client, stop := start()
	r := &pb.RegisterWorkerRequest{WorkerId: id.WorkerID, RequestId: uuid.NewString(), RequestedSessionId: uuid.NewString(), ProtocolVersion: 1, Allocatable: &pb.Resources{CpuMillis: 4000, MemoryBytes: 8192 << 20, ScratchBytes: 16384 << 20}, ExecutionSlots: 4, Labels: policy.Labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}}
	session, err := client.RegisterWorker(ctx, r)
	if err != nil || session.GetSessionGeneration() != 1 || !session.GetCleanupRequired() {
		t.Fatal("registration RPC failed", err)
	}
	h := &pb.HeartbeatRequest{Session: session.Session, RequestId: uuid.NewString(), ReportSequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}
	if result, err := client.Heartbeat(ctx, h); err != nil || result.GetReconcile() || result.GetDrain() {
		t.Fatal("heartbeat RPC did not make host ready", err)
	}
	acquire := &pb.AcquireWorkRequest{Session: session.Session, RequestId: uuid.NewString()}
	if result, err := client.AcquireWork(ctx, acquire); err != nil || result.GetNoWork() != pb.NoWorkReason_QUEUE_EMPTY {
		t.Fatal("empty queue RPC failed", result, err)
	}
	file, err := os.Open("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	job, err := spec.DecodeJob(file)
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	job.Spec.Inputs = nil
	job.Spec.Placement.Labels["architecture"] = "arm64"
	job.Spec.Image = "registry.example.org/eval@sha256:" + strings.Repeat("a", 64)
	canonical, hash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	queued, err := store.SubmitJob(ctx, pool, uuid.NewString(), hash, job)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := client.AcquireWork(ctx, acquire); err != nil || result.GetNoWork() != pb.NoWorkReason_QUEUE_EMPTY {
		t.Fatal("no-work replay acquired a later job", result, err)
	}
	acquire.RequestId = uuid.NewString()
	assigned, err := client.AcquireWork(ctx, acquire)
	if err != nil || assigned.GetAssignment() == nil {
		t.Fatal("assignment RPC failed", assigned, err)
	}
	assignment := assigned.GetAssignment()
	if assignment.GetAuthority().GetJobId() != queued.ID || assignment.GetAuthority().GetWorkerId() != id.WorkerID || assignment.GetAuthority().GetSessionId() != session.Session.SessionId || assignment.SpecSha256 != hash || string(assignment.CanonicalJobSpecJson) != string(canonical) || assignment.LeaseDurationMs == 0 || assignment.LeaseDurationMs > 30000 || assignment.Resources.MemoryBytes != uint64(job.Spec.Resources.MemoryMiB)<<20 {
		t.Fatal("wire assignment changed authoritative data", assignment)
	}
	spoofed := proto.Clone(acquire).(*pb.AcquireWorkRequest)
	spoofed.Session.WorkerId = uuid.NewString()
	if _, err := client.AcquireWork(ctx, spoofed); status.Code(err) != codes.PermissionDenied {
		t.Fatal("cross-worker acquisition accepted", err)
	}
	for _, change := range []func(*pb.RegisterWorkerRequest){func(r *pb.RegisterWorkerRequest) { r.Allocatable.MemoryBytes = math.MaxUint64 }, func(r *pb.RegisterWorkerRequest) { r.Allocatable.MemoryBytes++ }, func(r *pb.RegisterWorkerRequest) { r.ExecutionSlots = 1001 }, func(r *pb.RegisterWorkerRequest) { r.ProtocolVersion = 2 }} {
		bad := proto.Clone(r).(*pb.RegisterWorkerRequest)
		change(bad)
		if _, err := client.RegisterWorker(ctx, bad); status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid registration crossed RPC boundary", err)
		}
	}
	badHeartbeat := proto.Clone(h).(*pb.HeartbeatRequest)
	badHeartbeat.ReportSequence = math.MaxUint64
	if _, err := client.Heartbeat(ctx, badHeartbeat); status.Code(err) != codes.InvalidArgument {
		t.Fatal("heartbeat sequence overflow accepted", err)
	}
	stop()
	client, _ = start()
	if again, err := client.RegisterWorker(ctx, r); err != nil || again.GetSessionGeneration() != 1 || again.GetCleanupRequired() {
		t.Fatal("RPC restart lost reconciled session", err)
	}
	if replay, err := client.AcquireWork(ctx, acquire); err != nil || replay.GetAssignment().GetAuthority().GetAttemptId() != assignment.Authority.AttemptId || replay.GetAssignment().GetLeaseDurationMs() > assignment.LeaseDurationMs {
		t.Fatal("restart lost or renewed assignment", replay, err)
	}
	listing := &pb.ListAssignmentsRequest{Session: session.Session, PageSize: 1}
	if page, err := client.ListAssignments(ctx, listing); err != nil || len(page.GetAssignments()) != 1 || page.Assignments[0].GetAuthority().GetAttemptId() != assignment.Authority.AttemptId || page.Assignments[0].GetLeaseDurationMs() > assignment.LeaseDurationMs || page.NextAfterJobId != "" {
		t.Fatal("restart inventory changed authority", page, err)
	}
	badPage := proto.Clone(listing).(*pb.ListAssignmentsRequest)
	badPage.PageSize = 65
	if _, err := client.ListAssignments(ctx, badPage); status.Code(err) != codes.InvalidArgument {
		t.Fatal("unbounded page accepted", err)
	}
	badPage.PageSize = 1
	badPage.Session.WorkerId = uuid.NewString()
	if _, err := client.ListAssignments(ctx, badPage); status.Code(err) != codes.PermissionDenied {
		t.Fatal("cross-worker inventory accepted", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", assignment.Authority.AttemptId); err != nil {
		t.Fatal(err)
	}
	if expired, err := client.AcquireWork(ctx, acquire); err != nil || expired.GetRejected() != pb.Decision_FENCED {
		t.Fatal("expired replay returned authority", expired, err)
	}
	if page, err := client.ListAssignments(ctx, listing); err != nil || len(page.GetAssignments()) != 0 {
		t.Fatal("expired assignment appeared in inventory", page, err)
	}
	next := proto.Clone(r).(*pb.RegisterWorkerRequest)
	next.RequestId = uuid.NewString()
	next.RequestedSessionId = uuid.NewString()
	if _, err := client.RegisterWorker(ctx, next); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("concurrent live incarnation accepted", err)
	}
	if err := store.ApproveSessionTakeover(ctx, pool, id.WorkerID, r.RequestedSessionId, next.RequestedSessionId); err != nil {
		t.Fatal(err)
	}
	if replacement, err := client.RegisterWorker(ctx, next); err != nil || replacement.GetSessionGeneration() != 2 || !replacement.GetCleanupRequired() {
		t.Fatal("approved RPC takeover failed", err)
	}
	if _, err := client.Heartbeat(ctx, h); status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "SESSION_FENCED" {
		t.Fatal("old session still accepted RPCs", err)
	}
	if _, err := client.AcquireWork(ctx, acquire); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("old session replayed acquisition", err)
	}
	if _, err := client.ListAssignments(ctx, listing); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("old session recovered inventory", err)
	}
}
