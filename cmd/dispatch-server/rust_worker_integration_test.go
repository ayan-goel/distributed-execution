//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

func rustSessionProbe(t *testing.T, address string, p workerPKI, request *pb.RegisterWorkerRequest) ([]byte, string, error) {
	t.Helper()
	return rustProtocolProbe(t, "session_probe", address, p, request)
}

func rustProtocolProbe(t *testing.T, fixture, address string, p workerPKI, request proto.Message) ([]byte, string, error) {
	t.Helper()
	binary, err := filepath.Abs("../../.local/cargo-target/debug/examples/" + fixture)
	if err != nil {
		t.Fatal(err)
	}
	input, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "https://"+address, p.ca, p.clientCert, p.clientKey)
	command.Stdin = bytes.NewReader(input)
	var output, diagnostic bytes.Buffer
	command.Stdout, command.Stderr = &output, &diagnostic
	err = command.Run()
	if ctx.Err() != nil {
		t.Fatal("Rust client exceeded its own request deadline")
	}
	return output.Bytes(), diagnostic.String(), err
}

func TestRustClientRegistersAndReplaysThroughControlPlane(t *testing.T) {
	configureServerTestDatabase(t)
	p := testWorkerPKI(t)
	id := provisionTestWorker(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := run(ctx, []string{"serve", "--dev-insecure", "--listen", "127.0.0.1:0", "--allow-registry", "index.docker.io", "--worker-listen", "127.0.0.1:0", "--worker-tls-cert", p.serverCert, "--worker-tls-key", p.serverKey, "--worker-client-ca", p.ca}, writer)
		_ = writer.CloseWithError(err)
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		_ = reader.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	decoder := json.NewDecoder(reader)
	var address string
	for range 2 {
		var event struct{ Msg, Address string }
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		if event.Msg == "worker_listening" {
			address = event.Address
		}
	}
	request := &pb.RegisterWorkerRequest{WorkerId: id.WorkerID, RequestId: uuid.NewString(), RequestedSessionId: uuid.NewString(), ProtocolVersion: 1, Allocatable: &pb.Resources{CpuMillis: 4000, MemoryBytes: 8192 << 20, ScratchBytes: 16384 << 20}, ExecutionSlots: 4, Labels: map[string]string{"os": "linux", "architecture": "arm64"}, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.soft"}}
	for range 2 {
		output, diagnostic, err := rustSessionProbe(t, address, p, request)
		if err != nil {
			t.Fatal("Rust registration failed", err, diagnostic)
		}
		var reply pb.RegisterWorkerResponse
		if err := proto.Unmarshal(output, &reply); err != nil {
			t.Fatal(err)
		}
		if reply.GetSession().GetSessionId() != request.RequestedSessionId || reply.SessionGeneration != 1 || !reply.CleanupRequired {
			t.Fatal("Rust replay changed session authority", &reply)
		}
	}
	pool, err := openPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var state string
	var reconciled, healthy bool
	var sequence int64
	if err := pool.QueryRow(ctx, `SELECT w.state,w.reconciliation_complete,w.runtime_healthy,s.heartbeat_sequence FROM workers w JOIN worker_sessions s ON s.worker_id=w.id WHERE w.id=$1`, id.WorkerID).Scan(&state, &reconciled, &healthy, &sequence); err != nil {
		t.Fatal(err)
	}
	if state != "QUARANTINED" || reconciled || healthy || sequence != 1 {
		t.Fatal("protocol fixture falsely reported runtime readiness", state, reconciled, healthy, sequence)
	}
	wrongCA := p
	wrongCA.ca = testWorkerPKI(t).ca
	if _, _, err := rustSessionProbe(t, address, wrongCA, request); err == nil {
		t.Fatal("Rust trusted an unrelated server CA")
	}
	if err := run(ctx, []string{"worker", "revoke", "--id", id.WorkerID, "--credential", id.CredentialID}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, diagnostic, err := rustSessionProbe(t, address, p, request); err == nil || !strings.Contains(diagnostic, "Unauthenticated") {
		t.Fatal("Rust ignored revoked certificate", err, diagnostic)
	}
}

func TestRustClientBoundsStalledRPC(t *testing.T) {
	p := testWorkerPKI(t)
	certificate, err := tls.LoadX509KeyPair(p.serverCert, p.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan bool, 1)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: p.roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})), grpc.UnaryInterceptor(func(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		deadline, ok := ctx.Deadline()
		observed <- ok && time.Until(deadline) <= 5*time.Second
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	pb.RegisterWorkerServiceServer(server, &pb.UnimplementedWorkerServiceServer{})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	started := time.Now()
	_, _, err = rustSessionProbe(t, listener.Addr().String(), p, &pb.RegisterWorkerRequest{})
	if err == nil || time.Since(started) > 8*time.Second {
		t.Fatal("Rust RPC timeout was not enforced", err)
	}
	select {
	case bounded := <-observed:
		if !bounded {
			t.Fatal("Rust omitted server deadline")
		}
	default:
		t.Fatal("Rust did not reach authenticated RPC handler")
	}
}

func TestRustClientBoundsStalledTLSHandshake(t *testing.T) {
	p := testWorkerPKI(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() { conn, _ := listener.Accept(); accepted <- conn }()
	t.Cleanup(func() {
		_ = listener.Close()
		if conn := <-accepted; conn != nil {
			_ = conn.Close()
		}
	})
	started := time.Now()
	_, _, err = rustSessionProbe(t, listener.Addr().String(), p, &pb.RegisterWorkerRequest{})
	if err == nil || time.Since(started) > 8*time.Second {
		t.Fatal("Rust TLS handshake was not bounded", err)
	}
}
