//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type startupService struct {
	pb.WorkerServiceServer
	t            *testing.T
	oldContainer string
	mu           sync.Mutex
	lost         *pb.HeartbeatRequest
	replayed     bool
}

func (s *startupService) Heartbeat(ctx context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.RuntimeHealthy && r.ReconciliationComplete {
		if exec.CommandContext(ctx, "docker", "inspect", s.oldContainer).Run() == nil {
			s.t.Error("worker reported complete cleanup while old container still existed")
		}
	}
	reply, err := s.WorkerServiceServer.Heartbeat(ctx, r)
	if err != nil {
		return nil, err
	}
	if s.lost == nil {
		// Commit the real PostgreSQL report, then lose its reply. Physical cleanup
		// must continue, while this logical report retains its sequence and payload.
		s.lost = proto.Clone(r).(*pb.HeartbeatRequest)
		return nil, status.Error(codes.Unavailable, "injected lost heartbeat reply")
	}
	if r.ReportSequence == s.lost.ReportSequence {
		if !proto.Equal(r, s.lost) {
			s.t.Error("uncertain heartbeat changed before retry")
		}
		s.replayed = true
	}
	return reply, nil
}

func TestWorkerProcessReconcilesDockerBeforeAdvertisingReady(t *testing.T) {
	configureServerTestDatabase(t)
	p := testWorkerPKI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	docker := func(args ...string) string {
		t.Helper()
		output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("Docker fixture failed: %v: %s", err, output)
		}
		return strings.TrimSpace(string(output))
	}
	architecture := docker("version", "--format", "{{.Server.Arch}}")
	socket := strings.TrimPrefix(docker("context", "inspect", "--format", "{{.Endpoints.docker.Host}}"), "unix://")
	pool, err := openPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	resources := spec.Resources{CPUMillis: 500, MemoryMiB: 128, ScratchMiB: 64}
	labels := map[string]string{"os": "linux", "architecture": architecture}
	id, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{Name: "startup-worker", CertificateSHA256: sha256.Sum256(p.client.Certificate[0]), Resources: resources, Slots: 1, Labels: labels, Projects: []string{"research"}})
	if err != nil {
		t.Fatal(err)
	}
	old := store.Registration{RequestID: uuid.NewString(), SessionID: uuid.NewString(), ProtocolVersion: 1, Resources: resources, Slots: 1, Labels: labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.soft"}}
	if _, err := store.RegisterSession(ctx, pool, id, old); err != nil {
		t.Fatal(err)
	}
	image := "rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89"
	if exec.CommandContext(ctx, "docker", "image", "inspect", image).Run() != nil {
		docker("pull", image)
	}
	attempt, job := uuid.NewString(), uuid.NewString()
	container := docker("run", "-d", "--network", "none", "--name", "dispatch-"+attempt,
		"--label", "dev.dispatch.worker="+id.WorkerID, "--label", "dev.dispatch.session="+old.SessionID,
		"--label", "dev.dispatch.job="+job, "--label", "dev.dispatch.attempt="+attempt,
		"--label", "dev.dispatch.generation=1", "--label", "dev.dispatch.spec-sha256="+strings.Repeat("a", 64),
		"--label", "dev.dispatch.scratch-policy=soft-development", image, "sleep", "120")
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", container).Run() })
	certificate, err := tls.LoadX509KeyPair(p.serverCert, p.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	service := &startupService{WorkerServiceServer: workerapi.NewService(pool, store.AcquisitionPolicy{}, nil), t: t, oldContainer: container}
	server, err := workerapi.NewServer(pool, certificate, p.roots, service)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-serving })
	root := t.TempDir()
	for _, name := range []string{"state", "work"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	config, err := json.Marshal(map[string]any{
		"worker_id": id.WorkerID, "server_url": "https://" + listener.Addr().String(),
		"ca_cert": p.ca, "client_cert": p.clientCert, "client_key": p.clientKey,
		"journal_dir": filepath.Join(root, "state"), "workspace_root": filepath.Join(root, "work"),
		"docker_socket": socket, "cpu_millis": 500, "memory_mib": 128, "scratch_mib": 64, "execution_slots": 1, "labels": labels,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "worker.json")
	if err := os.WriteFile(path, config, 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := filepath.Abs("../../.local/cargo-target/debug/dispatch-worker")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, binary, "run", "--config", path, "--dev-soft-scratch")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostic bytes.Buffer
	command.Stderr = &diagnostic
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if !stopped {
			_ = command.Process.Kill()
			_ = command.Wait()
			stopped = true
		}
	}
	t.Cleanup(stop)
	decoder := json.NewDecoder(stdout)
	session := ""
	for {
		var event struct {
			Event     string
			SessionID string `json:"session_id"`
		}
		if err := decoder.Decode(&event); err != nil {
			stop()
			t.Fatal("worker stopped before readiness", err, diagnostic.String())
		}
		if event.Event == "session_pending" {
			session = event.SessionID
			if session == old.SessionID || session == "" {
				t.Fatal("worker reused old incarnation")
			}
			var current string
			if err := pool.QueryRow(ctx, "SELECT current_session_id::text FROM workers WHERE id=$1", id.WorkerID).Scan(&current); err != nil || current != old.SessionID {
				t.Fatal("old session changed before takeover", err, current)
			}
			// The test explicitly approves this replacement rather than sleeping
			// through the automatic inactivity window or weakening its policy.
			if err := store.ApproveSessionTakeover(ctx, pool, id.WorkerID, old.SessionID, session); err != nil {
				t.Fatal(err)
			}
		}
		if event.Event == "ready" {
			break
		}
	}
	var state string
	var reconciled bool
	var generation, sequence, registrations int64
	if err := pool.QueryRow(ctx, `SELECT w.state,w.reconciliation_complete,s.generation,s.heartbeat_sequence,
        (SELECT count(*) FROM worker_registrations r WHERE r.worker_id=w.id AND r.session_id=s.id)
        FROM workers w JOIN worker_sessions s ON s.id=w.current_session_id WHERE w.id=$1 AND s.id=$2`, id.WorkerID, session).Scan(&state, &reconciled, &generation, &sequence, &registrations); err != nil {
		t.Fatal(err)
	}
	if state != "READY" || !reconciled || generation != 2 || sequence < 2 || registrations != 1 {
		t.Fatal("unexpected startup state", state, reconciled, generation, sequence, registrations)
	}
	if exec.CommandContext(ctx, "docker", "inspect", container).Run() == nil {
		t.Fatal("old container survived readiness")
	}
	service.mu.Lock()
	replayed := service.replayed
	service.mu.Unlock()
	if !replayed {
		t.Fatal("lost heartbeat was not retried")
	}
	other := exec.CommandContext(ctx, binary, "run", "--config", path, "--dev-soft-scratch")
	output, err := other.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "Busy") {
		t.Fatal("concurrent worker acquired the same state directory", err, string(output))
	}
	if err := run(ctx, []string{"worker", "revoke", "--id", id.WorkerID, "--credential", id.CredentialID}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	// No jobs have been acquired in this slice, so terminal authentication failure
	// must end the process rather than retrying forever or reporting readiness.
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	select {
	case err := <-waited:
		stopped = true
		if err == nil || !strings.Contains(diagnostic.String(), "Unauthenticated") {
			t.Fatal("worker ignored revocation", err, diagnostic.String())
		}
	case <-time.After(12 * time.Second):
		_ = command.Process.Kill()
		<-waited
		stopped = true
		t.Fatal("worker did not exit after revocation")
	}
}
