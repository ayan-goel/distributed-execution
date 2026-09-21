//go:build integration

package workerapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type lostCompletionReply struct {
	pb.WorkerServiceServer
	expected *pb.CompleteAttemptRequest
	calls    atomic.Int32
}

func (s *lostCompletionReply) CompleteAttempt(ctx context.Context, request *pb.CompleteAttemptRequest) (*pb.CompleteAttemptResponse, error) {
	n := s.calls.Add(1)
	if n <= 3 && !proto.Equal(request, s.expected) {
		return nil, status.Error(codes.Internal, "retry changed original completion evidence")
	}
	reply, err := s.WorkerServiceServer.CompleteAttempt(ctx, request)
	if n == 1 && err == nil {
		// Inject loss after the real database commit. The retry must use the
		// saved completion, not repeat publication or require live object storage.
		return nil, status.Error(codes.Unavailable, "injected lost acknowledgement")
	}
	return reply, err
}

func TestRustCompletionRecoversLostAcknowledgementThroughMTLS(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason pb.FailureReason
		state  pb.AttemptState
	}{
		{"success", pb.FailureReason_FAILURE_REASON_UNSPECIFIED, pb.AttemptState_SUCCEEDED},
		{"failure", pb.FailureReason_TRANSFER_FAILED, pb.AttemptState_FAILED},
		{"cancel", pb.FailureReason_USER_CANCELLED, pb.AttemptState_CANCELLED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, _, request, storageCalls := completionRPCFixture(t)
			request.Reason = tc.reason
			signWireCompletion(t, request)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if tc.state == pb.AttemptState_CANCELLED {
				if _, err := pool.Exec(ctx, "UPDATE jobs SET state='CANCELLING',cancel_requested=true"); err != nil {
					t.Fatal(err)
				}
			}
			ca, roots := testCA(t)
			cert := testLeaf(t, ca, x509.ExtKeyUsageClientAuth)
			fingerprint := sha256.Sum256(cert.Certificate[0])
			// A second fixture credential uses the normal authentication path;
			// no test bypass injects a worker identity into the Rust connection.
			if _, err := pool.Exec(ctx, "INSERT INTO worker_credentials(worker_id,certificate_sha256) VALUES($1,$2)", request.Authority.WorkerId, fingerprint[:]); err != nil {
				t.Fatal(err)
			}
			service := &lostCompletionReply{WorkerServiceServer: NewService(pool, store.AcquisitionPolicy{}, nil), expected: request}
			server, err := NewServer(pool, testLeaf(t, ca, x509.ExtKeyUsageServerAuth), roots, service)
			if err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
			dir := t.TempDir()
			key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
			if err != nil {
				t.Fatal(err)
			}
			for name, block := range map[string]*pem.Block{
				"ca.pem":   {Type: "CERTIFICATE", Bytes: ca.Certificate[0]},
				"cert.pem": {Type: "CERTIFICATE", Bytes: cert.Certificate[0]},
				"key.pem":  {Type: "PRIVATE KEY", Bytes: key},
			} {
				if err := os.WriteFile(filepath.Join(dir, name), pem.EncodeToMemory(block), 0600); err != nil {
					t.Fatal(err)
				}
			}
			input, err := proto.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			binary, err := filepath.Abs("../../.local/cargo-target/debug/examples/completion_probe")
			if err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(ctx, binary, "https://"+listener.Addr().String(), filepath.Join(dir, "ca.pem"), filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"))
			command.Stdin = bytes.NewReader(input)
			var output, diagnostic bytes.Buffer
			command.Stdout, command.Stderr = &output, &diagnostic
			if err := command.Run(); err != nil {
				t.Fatal("Rust completion failed", err, diagnostic.String())
			}
			var reply pb.CompleteAttemptResponse
			if err := proto.Unmarshal(output.Bytes(), &reply); err != nil {
				t.Fatal(err)
			}
			if reply.Decision != pb.Decision_ACCEPTED || reply.State != tc.state {
				t.Fatal(&reply)
			}
			var manifest []byte
			var completions, events int
			var reservation string
			if err := pool.QueryRow(ctx, `SELECT c.manifest_json,(SELECT count(*) FROM attempt_completions),(SELECT count(*) FROM job_events WHERE type='ATTEMPT_COMPLETED'),r.state FROM attempt_completions c JOIN reservations r ON r.attempt_id=c.attempt_id`).Scan(&manifest, &completions, &events, &reservation); err != nil {
				t.Fatal(err)
			}
			if tc.state == pb.AttemptState_SUCCEEDED {
				if !bytes.Equal(manifest, reply.AcceptedManifestJson) {
					t.Fatal("Rust changed canonical manifest bytes")
				}
			} else if len(reply.AcceptedManifestJson) != 0 {
				t.Fatal("unsuccessful completion returned accepted outputs")
			}
			if completions != 1 || events != 1 || reservation != "released" || service.calls.Load() != 5 || storageCalls.Load() != 1 {
				t.Fatal("retry changed publication or storage accounting", completions, events, reservation, service.calls.Load(), storageCalls.Load())
			}
		})
	}
}
