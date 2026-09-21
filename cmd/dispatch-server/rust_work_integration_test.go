//go:build integration

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

func TestRustAcquisitionAndRecoveryThroughControlPlane(t *testing.T) {
	configureServerTestDatabase(t)
	p := testWorkerPKI(t)
	id := provisionTestWorker(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := openPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	registration := store.Registration{RequestID: uuid.NewString(), SessionID: uuid.NewString(), ProtocolVersion: 1, Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4, Labels: map[string]string{"os": "linux", "architecture": "arm64"}, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}}
	if _, err := store.RegisterSession(ctx, pool, id, registration); err != nil {
		t.Fatal(err)
	}
	// This test seeds a simulated healthy runtime in its disposable schema. The
	// Rust fixture only consumes authority; it never claims to inspect Docker.
	if _, err := store.RecordHeartbeat(ctx, pool, id, store.HeartbeatReport{RequestID: uuid.NewString(), SessionID: registration.SessionID, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}); err != nil {
		t.Fatal(err)
	}
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	serverCtx, stop := context.WithCancel(ctx)
	go func() {
		err := run(serverCtx, []string{"serve", "--dev-insecure", "--listen", "127.0.0.1:0", "--allow-registry", "index.docker.io", "--worker-listen", "127.0.0.1:0", "--worker-tls-cert", p.serverCert, "--worker-tls-key", p.serverKey, "--worker-client-ca", p.ca}, writer)
		_ = writer.CloseWithError(err)
		done <- err
	}()
	t.Cleanup(func() {
		stop()
		_ = reader.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	decoder := json.NewDecoder(reader)
	address := ""
	for range 2 {
		var event struct{ Msg, Address string }
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		if event.Msg == "worker_listening" {
			address = event.Address
		}
	}
	request := &pb.AcquireWorkRequest{Session: &pb.WorkerSession{WorkerId: id.WorkerID, SessionId: registration.SessionID}, RequestId: uuid.NewString()}
	invoke := func() *pb.AcquireWorkResponse {
		t.Helper()
		output, diagnostic, err := rustProtocolProbe(t, "work_probe", address, p, request)
		if err != nil {
			t.Fatal("Rust work protocol failed", err, diagnostic)
		}
		var response pb.AcquireWorkResponse
		if err := proto.Unmarshal(output, &response); err != nil {
			t.Fatal(err)
		}
		return &response
	}
	if reply := invoke(); reply.GetNoWork() != pb.NoWorkReason_QUEUE_EMPTY {
		t.Fatal("empty queue response changed", reply)
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
	job.Spec.Image = "example.org/test@sha256:" + strings.Repeat("a", 64)
	job.Spec.Resources = spec.Resources{CPUMillis: 1, MemoryMiB: 1, ScratchMiB: 1}
	job.Spec.Env["MESSAGE"] = "<tag> & λ \u2028"
	_, hash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := store.SubmitJob(ctx, pool, uuid.NewString(), hash, job); err != nil {
			t.Fatal(err)
		}
	}
	if reply := invoke(); reply.GetNoWork() != pb.NoWorkReason_QUEUE_EMPTY {
		t.Fatal("Rust retry consumed a later job", reply)
	}
	for range 3 {
		request.RequestId = uuid.NewString()
		if reply := invoke(); reply.GetAssignment() == nil {
			t.Fatal("Rust did not acquire", reply)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM attempts").Scan(&count); err != nil || count != 3 {
		t.Fatal("Rust replay duplicated work", count, err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM attempts WHERE state='FINALIZING' AND exit_code=0 AND container_id IS NOT NULL").Scan(&count); err != nil || count != 3 {
		t.Fatal("Rust phase reports lost progress or exit evidence", count, err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM attempt_phase_reports").Scan(&count); err != nil || count != 9 {
		t.Fatal("Rust phase retries duplicated report history", count, err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_events WHERE type='PHASE_CHANGED'").Scan(&count); err != nil || count != 9 {
		t.Fatal("Rust phase retries duplicated transitions", count, err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM worker_lease_requests").Scan(&count); err != nil || count != 3 {
		t.Fatal("Rust renewal replay duplicated batch grants", count, err)
	}
	var exact bool
	if err := pool.QueryRow(ctx, `SELECT bool_and(a.lease_expires_at=(r.results->0->>'LeaseExpiresAt')::timestamptz) FROM worker_lease_requests r JOIN attempts a ON a.id=(r.results->0->'Authority'->>'attemptId')::uuid`).Scan(&exact); err != nil || !exact {
		t.Fatal("Rust renewal retry changed the original grant", exact, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	if reply := invoke(); reply.GetRejected() != pb.Decision_FENCED {
		t.Fatal("Rust accepted expired acquisition", reply)
	}
}

type delayedGrantService struct {
	pb.UnimplementedWorkerServiceServer
	delayRenewal bool
	phase        *atomic.Int32
}

func (service delayedGrantService) ReportPhase(_ context.Context, r *pb.ReportPhaseRequest) (*pb.MutationResponse, error) {
	// The fault fixture acknowledges synthetic progress so the Rust probe reaches
	// the deliberately delayed renewal response; no database/runtime is involved.
	for {
		previous := service.phase.Load()
		if previous >= int32(r.Phase) || service.phase.CompareAndSwap(previous, int32(r.Phase)) {
			break
		}
	}
	return &pb.MutationResponse{Decision: pb.Decision_ACCEPTED, State: pb.AttemptState(service.phase.Load())}, nil
}

func (service delayedGrantService) AcquireWork(ctx context.Context, r *pb.AcquireWorkRequest) (*pb.AcquireWorkResponse, error) {
	job := spec.Job{APIVersion: spec.APIVersion, Kind: "Job", Metadata: spec.Metadata{Name: "late-grant", Project: "research"}, Spec: spec.JobSpec{
		Image: "example.org/test@sha256:" + strings.Repeat("a", 64), Command: []string{"true"},
		Resources:               spec.Resources{CPUMillis: 1, MemoryMiB: 1, ScratchMiB: 1},
		Timeouts:                spec.Timeouts{StartupSeconds: 300, ExecutionSeconds: 1800, FinalizationSeconds: 300},
		Retry:                   spec.Retry{MaxAttempts: 1, InitialBackoffSeconds: 5, MaxBackoffSeconds: 60},
		TerminationGraceSeconds: 10, Network: "disabled",
	}}
	raw, hash, err := job.Canonical()
	if err != nil {
		return nil, err
	}
	// The stale sample leaves only 100 ms after the lease margin. Delivery is
	// intentionally later, but still well within the five-second RPC timeout.
	reply := &pb.AcquireWorkResponse{Outcome: &pb.AcquireWorkResponse_Assignment{Assignment: &pb.Assignment{Authority: &pb.AttemptAuthority{JobId: uuid.NewString(), AttemptId: uuid.NewString(), Generation: 1, WorkerId: r.GetSession().GetWorkerId(), SessionId: r.GetSession().GetSessionId()}, Resources: &pb.Resources{CpuMillis: 1, MemoryBytes: 1 << 20, ScratchBytes: 1 << 20}, ImageDigest: job.Spec.Image, Argv: job.Spec.Command, CanonicalJobSpecJson: raw, SpecSha256: hash, LeaseDurationMs: 5100, PhaseRemainingMs: 300000}}}
	if service.delayRenewal {
		// The probe first replays acquisition; keep that authority stable so this
		// fixture reaches and isolates the delayed renewal response.
		reply.GetAssignment().Authority.JobId = r.RequestId
		reply.GetAssignment().Authority.AttemptId = r.RequestId
		reply.GetAssignment().LeaseDurationMs = 30000
		return reply, nil
	}
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (delayedGrantService) RenewLeases(ctx context.Context, r *pb.RenewLeasesRequest) (*pb.RenewLeasesResponse, error) {
	reply := &pb.RenewLeasesResponse{}
	for _, a := range r.Attempts {
		reply.Results = append(reply.Results, &pb.LeaseResult{Authority: a, Decision: pb.Decision_ACCEPTED, RemainingMs: 5100, PhaseRemainingMs: 300000})
	}
	// Only 100 ms remains after the safety margin. A successful response after
	// 200 ms must carry cleanup identity rather than fresh execution authority.
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestRustRejectsGrantDeliveredAfterLocalAuthorityExpires(t *testing.T) {
	for _, delayRenewal := range []bool{false, true} {
		name := "acquisition"
		diagnostic := "local authority expired"
		if delayRenewal {
			name = "renewal"
			diagnostic = "local renewal authority expired"
		}
		t.Run(name, func(t *testing.T) {
			testRustDelayedAuthority(t, delayedGrantService{delayRenewal: delayRenewal}, diagnostic)
		})
	}
}

func testRustDelayedAuthority(t *testing.T, service delayedGrantService, expectedDiagnostic string) {
	t.Helper()
	service.phase = &atomic.Int32{}
	p := testWorkerPKI(t)
	cert, err := tls.LoadX509KeyPair(p.serverCert, p.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: p.roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
	pb.RegisterWorkerServiceServer(server, service)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	request := &pb.AcquireWorkRequest{Session: &pb.WorkerSession{WorkerId: uuid.NewString(), SessionId: uuid.NewString()}, RequestId: uuid.NewString()}
	output, diagnostic, err := rustProtocolProbe(t, "work_probe", listener.Addr().String(), p, request)
	if err == nil || len(output) != 0 || !strings.Contains(diagnostic, expectedDiagnostic) {
		t.Fatal("Rust granted fresh authority on late receipt", err, diagnostic)
	}
}
