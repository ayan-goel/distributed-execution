//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestControlPlaneCommandServesHTTPAndMTLSWorkers(t *testing.T) {
	configureServerTestDatabase(t)
	p := testWorkerPKI(t)
	id := provisionTestWorker(t, p)
	args := []string{"serve", "--dev-insecure", "--listen", "127.0.0.1:0", "--allow-registry", "index.docker.io", "--worker-listen", "127.0.0.1:0", "--worker-tls-cert", p.serverCert, "--worker-tls-key", p.serverKey, "--worker-client-ca", p.ca}
	start := func() (string, pb.WorkerServiceClient, func()) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		reader, writer := io.Pipe()
		done := make(chan error, 1)
		go func() { err := run(ctx, args, writer); _ = writer.CloseWithError(err); done <- err }()
		decoder := json.NewDecoder(reader)
		addresses := map[string]string{}
		for range 2 {
			var event struct{ Msg, Address string }
			if err := decoder.Decode(&event); err != nil {
				cancel()
				t.Fatal("listener startup failed", err)
			}
			addresses[event.Msg] = event.Address
		}
		conn, err := grpc.NewClient(addresses["worker_listening"], grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: p.roots, Certificates: []tls.Certificate{p.client}, MinVersion: tls.VersionTLS13})))
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		stopped := false
		stop := func() {
			if stopped {
				return
			}
			stopped = true
			_ = conn.Close()
			cancel()
			if err := <-done; err != nil {
				t.Error("combined server shutdown failed", err)
			}
			_ = reader.Close()
		}
		t.Cleanup(stop)
		return "http://" + addresses["http_listening"], pb.NewWorkerServiceClient(conn), stop
	}
	address, client, stop := start()
	httpClient := &http.Client{Timeout: 3 * time.Second}
	defer httpClient.CloseIdleConnections()
	response, err := httpClient.Get(address + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("HTTP listener not healthy", response.StatusCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := &pb.RegisterWorkerRequest{WorkerId: id.WorkerID, RequestId: uuid.NewString(), RequestedSessionId: uuid.NewString(), ProtocolVersion: 1, Allocatable: &pb.Resources{CpuMillis: 4000, MemoryBytes: 8192 << 20, ScratchBytes: 16384 << 20}, ExecutionSlots: 4, Labels: map[string]string{"os": "linux", "architecture": "arm64"}, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.soft"}}
	session, err := client.RegisterWorker(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	h := &pb.HeartbeatRequest{Session: session.Session, RequestId: uuid.NewString(), ReportSequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}
	if reply, err := client.Heartbeat(ctx, h); err != nil || reply.GetDrain() || reply.GetReconcile() {
		t.Fatal("command listener did not register a ready host", err)
	}
	stop()
	_, client, _ = start()
	if recovered, err := client.RegisterWorker(ctx, r); err != nil || recovered.GetSessionGeneration() != 1 || recovered.GetCleanupRequired() {
		t.Fatal("command restart lost durable worker session", err)
	}
	if err := run(ctx, []string{"worker", "revoke", "--id", id.WorkerID, "--credential", id.CredentialID}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Heartbeat(ctx, h); status.Code(err) != codes.Unauthenticated {
		t.Fatal("running listener ignored command revocation", err)
	}
}

func TestInvalidWorkerTLSDoesNotAnnouncePartialStartup(t *testing.T) {
	configureServerTestDatabase(t)
	p := testWorkerPKI(t)
	var output bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := run(ctx, []string{"serve", "--dev-insecure", "--allow-registry", "index.docker.io", "--worker-listen", "127.0.0.1:0", "--worker-tls-cert", p.serverCert, "--worker-tls-key", p.clientKey, "--worker-client-ca", p.ca}, &output)
	if err == nil || output.Len() != 0 {
		t.Fatal("invalid worker TLS announced a listener", err, output.String())
	}
}
