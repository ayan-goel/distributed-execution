//go:build integration

package workerapi

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type identityService struct {
	pb.UnimplementedWorkerServiceServer
}

func (identityService) RegisterWorker(ctx context.Context, r *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	id, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "missing verified identity")
	}
	return &pb.RegisterWorkerResponse{Session: &pb.WorkerSession{WorkerId: id.WorkerID}}, nil
}

func workerTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("DISPATCH_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("integration database required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	root, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("workerapi_%d", time.Now().UnixNano())
	if _, err := root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, err := root.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		root.Close()
		if err != nil {
			t.Error(err)
		}
	})
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES('research',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestMTLSWorkerIdentityAndLiveRevocation(t *testing.T) {
	ctx := context.Background()
	pool := workerTestPool(t)
	ca, roots := testCA(t)
	serverCert := testLeaf(t, ca, x509.ExtKeyUsageServerAuth)
	clientCert := testLeaf(t, ca, x509.ExtKeyUsageClientAuth)
	provision := func(cert tls.Certificate) store.WorkerIdentity {
		t.Helper()
		identity, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{Name: "test-host", CertificateSHA256: sha256.Sum256(cert.Certificate[0]), Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4, Projects: []string{"research"}, Labels: map[string]string{"os": "linux", "architecture": "arm64"}})
		if err != nil {
			t.Fatal(err)
		}
		return identity
	}
	identity := provision(clientCert)
	server, err := NewServer(pool, serverCert, roots, identityService{})
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
	connect := func(cert *tls.Certificate) pb.WorkerServiceClient {
		t.Helper()
		config := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}
		if cert != nil {
			config.Certificates = []tls.Certificate{*cert}
		}
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(config)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return pb.NewWorkerServiceClient(conn)
	}
	call := func(client pb.WorkerServiceClient, id string) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		response, err := client.RegisterWorker(ctx, &pb.RegisterWorkerRequest{WorkerId: id})
		if err == nil && response.GetSession().GetWorkerId() != id {
			t.Fatal("handler received wrong host identity")
		}
		return err
	}
	client := connect(&clientCert)
	if err := call(client, identity.WorkerID); err != nil {
		t.Fatal("provisioned mTLS peer rejected", err)
	}
	if err := call(client, "00000000-0000-0000-0000-000000000001"); status.Code(err) != codes.PermissionDenied {
		t.Fatal("host impersonation accepted", err)
	}
	unknown := testLeaf(t, ca, x509.ExtKeyUsageClientAuth)
	if err := call(connect(&unknown), identity.WorkerID); status.Code(err) != codes.Unauthenticated {
		t.Fatal("unprovisioned certificate accepted", err)
	}
	if err := call(connect(nil), identity.WorkerID); status.Code(err) != codes.Unavailable {
		t.Fatal("missing client certificate accepted")
	}
	otherCA, _ := testCA(t)
	foreign := testLeaf(t, otherCA, x509.ExtKeyUsageClientAuth)
	// Enroll these fingerprints so database rejection cannot mask a missing TLS
	// chain or client-purpose check. Neither peer may reach the RPC handler.
	foreignIdentity := provision(foreign)
	if err := call(connect(&foreign), foreignIdentity.WorkerID); status.Code(err) != codes.Unavailable {
		t.Fatal("untrusted client certificate passed TLS", err)
	}
	wrongUsageIdentity := provision(serverCert)
	if err := call(connect(&serverCert), wrongUsageIdentity.WorkerID); status.Code(err) != codes.Unavailable {
		t.Fatal("server-only certificate passed client authentication", err)
	}
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	_, err = client.RegisterWorker(callCtx, &pb.RegisterWorkerRequest{WorkerId: identity.WorkerID, Labels: map[string]string{"large": strings.Repeat("x", 4<<20)}})
	cancel()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversized worker request accepted", err)
	}
	if err := store.RevokeWorkerCredential(ctx, pool, identity.WorkerID, identity.CredentialID); err != nil {
		t.Fatal(err)
	}
	if err := call(client, identity.WorkerID); status.Code(err) != codes.Unauthenticated {
		t.Fatal("existing connection ignored revocation", err)
	}
}
