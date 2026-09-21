//go:build integration

package main

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type renewalLoopService struct {
	pb.WorkerServiceServer
	pool       *pgxpool.Pool
	t          *testing.T
	mu         sync.Mutex
	first      *pb.RenewLeasesRequest
	calls      int
	replayedAt time.Time
}

func (s *renewalLoopService) RenewLeases(ctx context.Context, r *pb.RenewLeasesRequest) (*pb.RenewLeasesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if len(r.Attempts) != 2 {
		s.t.Error("renewal loop changed batch size", len(r.Attempts))
	}
	if s.calls == 2 {
		if !proto.Equal(s.first, r) {
			s.t.Error("lost renewal reply changed request identity or payload")
		}
		s.replayedAt = time.Now()
	}
	if s.calls == 3 {
		if s.first.RequestId == r.RequestId || time.Since(s.replayedAt) < 4*time.Second {
			s.t.Error("next renewal period reused identity or ran without its interval")
		}
		// Expire real database authority after the successful replay. The actual
		// service must fence both attempts and the Rust loop must stop renewing.
		if _, err := s.pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second'"); err != nil {
			return nil, err
		}
	}
	reply, err := s.WorkerServiceServer.RenewLeases(ctx, r)
	if err != nil {
		return nil, err
	}
	expected := pb.Decision_ACCEPTED
	if s.calls >= 3 {
		expected = pb.Decision_FENCED
	}
	for _, result := range reply.Results {
		if result.Decision != expected {
			s.t.Error("unexpected durable lease decision", result.Decision, expected)
		}
	}
	if s.calls == 1 {
		// The grant has committed. Losing only its reply tests application replay,
		// rather than a connection failure that never reached PostgreSQL.
		s.first = proto.Clone(r).(*pb.RenewLeasesRequest)
		return nil, status.Error(codes.Unavailable, "injected lost renewal reply")
	}
	return reply, nil
}

func TestRustPeriodicRenewalReplaysCommittedBatchAndStopsOnFencing(t *testing.T) {
	configureServerTestDatabase(t)
	p := testWorkerPKI(t)
	id := provisionTestWorker(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	pool, err := openPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	r := store.Registration{RequestID: uuid.NewString(), SessionID: uuid.NewString(), ProtocolVersion: 1,
		Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4,
		Labels:       map[string]string{"os": "linux", "architecture": "arm64"},
		Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}}
	if _, err := store.RegisterSession(ctx, pool, id, r); err != nil {
		t.Fatal(err)
	}
	// These are protocol fixtures: seeded readiness is not proof of runtime health.
	if _, err := store.RecordHeartbeat(ctx, pool, id, store.HeartbeatReport{RequestID: uuid.NewString(), SessionID: r.SessionID, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}); err != nil {
		t.Fatal(err)
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
	_, hash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := store.SubmitJob(ctx, pool, uuid.NewString(), hash, job); err != nil {
			t.Fatal(err)
		}
		assigned, err := store.AcquireWork(ctx, pool, id, store.AcquisitionRequest{SessionID: r.SessionID, RequestID: uuid.NewString()}, store.AcquisitionPolicy{})
		if err != nil || assigned.Assignment == nil {
			t.Fatal("failed to seed assignment", assigned, err)
		}
	}
	certificate, err := tls.LoadX509KeyPair(p.serverCert, p.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	service := &renewalLoopService{WorkerServiceServer: workerapi.NewService(pool, store.AcquisitionPolicy{}), pool: pool, t: t}
	server, err := workerapi.NewServer(pool, certificate, p.roots, service)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	request := &pb.ListAssignmentsRequest{Session: &pb.WorkerSession{WorkerId: id.WorkerID, SessionId: r.SessionID}, PageSize: 64}
	_, diagnostic, err := rustProtocolProbe(t, "renewal_probe", listener.Addr().String(), p, request)
	if err != nil {
		t.Fatal("Rust renewal loop failed", err, diagnostic)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.calls != 3 {
		t.Fatal("unexpected renewal call count", service.calls)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM worker_lease_requests").Scan(&count); err != nil || count != 2 {
		t.Fatal("renewal loop did not preserve two durable batch identities", count, err)
	}
}
