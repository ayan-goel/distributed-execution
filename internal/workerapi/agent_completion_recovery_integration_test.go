//go:build integration

package workerapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type crashCompletionService struct {
	pb.WorkerServiceServer
	expected  *pb.CompleteAttemptRequest
	committed chan struct{}
	calls     atomic.Int32
	loseFirst bool
}

func (s *crashCompletionService) CompleteAttempt(ctx context.Context, r *pb.CompleteAttemptRequest) (*pb.CompleteAttemptResponse, error) {
	call := s.calls.Add(1)
	if !proto.Equal(r, s.expected) {
		return nil, status.Error(codes.Internal, "recovery changed completion")
	}
	if (s.loseFirst && call == 2) || (!s.loseFirst && call == 1) {
		// The replacement must keep the durable entry and retry a transient outage
		// instead of considering its startup scan complete after one failed call.
		return nil, status.Error(codes.Unavailable, "injected recovery outage")
	}
	reply, err := s.WorkerServiceServer.CompleteAttempt(ctx, r)
	if call == 1 && s.loseFirst && err == nil {
		// Commit, signal the parent to kill the worker, and withhold every byte
		// of the reply. Recovery must use disk evidence in a different process.
		close(s.committed)
		<-ctx.Done()
		return nil, status.Error(codes.Unavailable, "injected completion acknowledgement loss")
	}
	return reply, err
}

func TestAgentRecoversCommittedCompletionAfterProcessDeath(t *testing.T) {
	agentCompletionRecovery(t, true)
}

func TestAgentRecoversUnacceptedCompletionAsLost(t *testing.T) {
	agentCompletionRecovery(t, false)
}

func agentCompletionRecovery(t *testing.T, committed bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	docker := func(args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatal("Docker fixture unavailable", err, string(out))
		}
		return strings.TrimSpace(string(out))
	}
	architecture := docker("version", "--format", "{{.Server.Arch}}")
	socket := strings.TrimPrefix(docker("context", "inspect", "--format", "{{.Endpoints.docker.Host}}"), "unix://")
	pool, client, request, storageCalls := completionRPCFixture(t)
	page, err := client.ListAssignments(ctx, &pb.ListAssignmentsRequest{Session: &pb.WorkerSession{WorkerId: request.Authority.WorkerId, SessionId: request.Authority.SessionId}, PageSize: 1})
	if err != nil || len(page.GetAssignments()) != 1 {
		t.Fatal("assignment fixture missing", err)
	}
	ca, roots := testCA(t)
	cert := testLeaf(t, ca, x509.ExtKeyUsageClientAuth)
	fingerprint := sha256.Sum256(cert.Certificate[0])
	if _, err := pool.Exec(ctx, "INSERT INTO worker_credentials(worker_id,certificate_sha256) VALUES($1,$2)", request.Authority.WorkerId, fingerprint[:]); err != nil {
		t.Fatal(err)
	}
	service := &crashCompletionService{WorkerServiceServer: NewService(pool, store.AcquisitionPolicy{}, nil), expected: request, committed: make(chan struct{}), loseFirst: committed}
	server, err := NewServer(pool, testLeaf(t, ca, x509.ExtKeyUsageServerAuth), roots, service)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-serving; err != nil {
			t.Error(err)
		}
	})
	root := t.TempDir()
	for _, name := range []string{"state", "work"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	for name, block := range map[string]*pem.Block{
		"ca.pem":   {Type: "CERTIFICATE", Bytes: ca.Certificate[0]},
		"cert.pem": {Type: "CERTIFICATE", Bytes: cert.Certificate[0]},
		"key.pem":  {Type: "PRIVATE KEY", Bytes: key},
	} {
		if err := os.WriteFile(filepath.Join(root, name), pem.EncodeToMemory(block), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for name, message := range map[string]proto.Message{"assignment.pb": page.Assignments[0], "completion.pb": request} {
		raw, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	probe, err := filepath.Abs("../../.local/cargo-target/debug/examples/completion_recovery_probe")
	if err != nil {
		t.Fatal(err)
	}
	probeArgs := []string{"deliver", filepath.Join(root, "state"), filepath.Join(root, "assignment.pb"), filepath.Join(root, "completion.pb"), "https://" + listener.Addr().String(), filepath.Join(root, "ca.pem"), filepath.Join(root, "cert.pem"), filepath.Join(root, "key.pem")}
	if !committed {
		probeArgs[0] = "seed"
		if output, err := exec.CommandContext(ctx, probe, probeArgs...).CombinedOutput(); err != nil {
			t.Fatal("could not seed unaccepted completion", err, string(output))
		}
	} else {
		crashed := exec.CommandContext(ctx, probe, probeArgs...)
		var crashDiagnostic bytes.Buffer
		crashed.Stderr = &crashDiagnostic
		if err := crashed.Start(); err != nil {
			t.Fatal(err)
		}
		crashDone := make(chan error, 1)
		go func() { crashDone <- crashed.Wait() }()
		t.Cleanup(func() {
			if crashed.ProcessState == nil {
				_ = crashed.Process.Kill()
			}
		})
		select {
		case <-service.committed:
			_ = crashed.Process.Kill()
			<-crashDone
		case err := <-crashDone:
			t.Fatal("worker exited before lost acknowledgement", err, crashDiagnostic.String())
		case <-ctx.Done():
			_ = crashed.Process.Kill()
			<-crashDone
			t.Fatal("worker did not commit completion", crashDiagnostic.String())
		}
	}
	config, err := json.Marshal(map[string]any{
		"worker_id": request.Authority.WorkerId, "server_url": "https://" + listener.Addr().String(),
		"ca_cert": filepath.Join(root, "ca.pem"), "client_cert": filepath.Join(root, "cert.pem"), "client_key": filepath.Join(root, "key.pem"),
		"journal_dir": filepath.Join(root, "state"), "workspace_root": filepath.Join(root, "work"),
		"docker_socket": socket, "cpu_millis": 500, "memory_mib": 128, "scratch_mib": 64, "execution_slots": 1,
		"labels": map[string]string{"os": "linux", "architecture": architecture},
	})
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "worker.json")
	if err := os.WriteFile(configPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := filepath.Abs("../../.local/cargo-target/debug/dispatch-worker")
	if err != nil {
		t.Fatal(err)
	}
	previous := request.Authority.SessionId
	var firstReply pb.CompleteAttemptResponse
	for restart := range 2 {
		command := exec.CommandContext(ctx, binary, "run", "--config", configPath, "--dev-soft-scratch")
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
		for {
			var event struct {
				Event     string
				SessionID string `json:"session_id"`
				AttemptID string `json:"attempt_id"`
			}
			if err := decoder.Decode(&event); err != nil {
				stop()
				t.Fatal("agent stopped before recovery", err, diagnostic.String())
			}
			if event.Event == "session_pending" {
				if event.SessionID == previous || event.SessionID == "" {
					t.Fatal("recovery reused old incarnation")
				}
				if err := store.ApproveSessionTakeover(ctx, pool, request.Authority.WorkerId, previous, event.SessionID); err != nil {
					t.Fatal(err)
				}
				previous = event.SessionID
			}
			if event.Event == "completion_recovered" {
				if event.AttemptID != request.Authority.AttemptId {
					t.Fatal("recovered another attempt")
				}
				break
			}
		}
		stop()
		probeArgs[0] = "inspect"
		inspect := exec.CommandContext(ctx, probe, probeArgs...)
		var stderr bytes.Buffer
		inspect.Stderr = &stderr
		raw, err := inspect.Output()
		if err != nil {
			t.Fatal("completion acknowledgement was not durable", err, stderr.String())
		}
		var reply pb.CompleteAttemptResponse
		if err := proto.Unmarshal(raw, &reply); err != nil {
			t.Fatal(err)
		}
		expectedDecision, expectedState, expectedCalls := pb.Decision_ALREADY_TERMINAL, pb.AttemptState_LOST, int32(2)
		if committed {
			expectedDecision, expectedState, expectedCalls = pb.Decision_ACCEPTED, pb.AttemptState_SUCCEEDED, 3
		}
		if reply.Decision != expectedDecision || reply.State != expectedState {
			t.Fatal(&reply)
		}
		if restart == 0 {
			proto.Merge(&firstReply, &reply)
		} else if !proto.Equal(&firstReply, &reply) {
			t.Fatal("restart changed persisted reply")
		}
		if service.calls.Load() != expectedCalls {
			t.Fatal("resolved completion was resent", service.calls.Load())
		}
	}
	var manifest []byte
	var completions, events int
	var reservation string
	if !committed {
		var accepted, lost int
		if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM attempt_completions),(SELECT count(*) FROM job_events WHERE type='ATTEMPT_COMPLETED'),(SELECT count(*) FROM jobs WHERE accepted_attempt_id IS NOT NULL),(SELECT count(*) FROM attempts WHERE state='LOST')`).Scan(&completions, &events, &accepted, &lost); err != nil {
			t.Fatal(err)
		}
		if completions != 0 || events != 0 || accepted != 0 || lost != 1 || len(firstReply.AcceptedManifestJson) != 0 || storageCalls.Load() != 1 {
			t.Fatal("session recovery published an unaccepted old result", completions, events, accepted, lost)
		}
		return
	}
	if err := pool.QueryRow(ctx, `SELECT c.manifest_json,(SELECT count(*) FROM attempt_completions),(SELECT count(*) FROM job_events WHERE type='ATTEMPT_COMPLETED'),r.state FROM attempt_completions c JOIN reservations r ON r.attempt_id=c.attempt_id`).Scan(&manifest, &completions, &events, &reservation); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(manifest, firstReply.AcceptedManifestJson) || completions != 1 || events != 1 || reservation != "released" || storageCalls.Load() != 1 {
		t.Fatal("process recovery changed accepted result or repeated publication", completions, events, reservation, storageCalls.Load())
	}
}
