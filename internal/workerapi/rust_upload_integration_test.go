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
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type lostUploadReplyService struct {
	pb.WorkerServiceServer
	creates   atomic.Int32
	finalizes atomic.Int32
}

func (s *lostUploadReplyService) CreateUpload(ctx context.Context, r *pb.CreateUploadRequest) (*pb.CreateUploadResponse, error) {
	response, err := s.WorkerServiceServer.CreateUpload(ctx, r)
	if err == nil && s.creates.Add(1) == 1 {
		return nil, status.Error(codes.Unavailable, "injected lost upload grant")
	}
	return response, err
}

func (s *lostUploadReplyService) FinalizeUpload(ctx context.Context, r *pb.FinalizeUploadRequest) (*pb.FinalizeUploadResponse, error) {
	response, err := s.WorkerServiceServer.FinalizeUpload(ctx, r)
	if err == nil && s.finalizes.Add(1) == 1 {
		return nil, status.Error(codes.Unavailable, "injected lost artifact acknowledgement")
	}
	return response, err
}

// Verify the Rust data path against the same isolated versioned storage profile
// as the Go client. A separate credential binds it to the existing fixture worker.
func verifyRustUpload(t *testing.T, pool *pgxpool.Pool, objects *objectstore.Store, declaration *pb.CreateUploadRequest, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	request := proto.Clone(declaration).(*pb.CreateUploadRequest)
	request.RequestId = uuid.NewString()
	ca, roots := testCA(t)
	cert := testLeaf(t, ca, x509.ExtKeyUsageClientAuth)
	fingerprint := sha256.Sum256(cert.Certificate[0])
	if _, err := pool.Exec(ctx, "INSERT INTO worker_credentials(worker_id,certificate_sha256) VALUES($1,$2)", request.Authority.WorkerId, fingerprint[:]); err != nil {
		t.Fatal(err)
	}
	service := &lostUploadReplyService{WorkerServiceServer: NewService(pool, store.AcquisitionPolicy{}, objects)}
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
	raw, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{"declaration.pb": raw, "result": []byte(body)} {
		if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	probe, err := filepath.Abs("../../.local/cargo-target/debug/examples/upload_probe")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, probe, "https://"+listener.Addr().String(), filepath.Join(root, "ca.pem"), filepath.Join(root, "cert.pem"), filepath.Join(root, "key.pem"), filepath.Join(root, "declaration.pb"), filepath.Join(root, "result"), uuid.NewString())
	var diagnostic bytes.Buffer
	command.Stderr = &diagnostic
	output, err := command.Output()
	if err != nil {
		t.Fatal("Rust upload failed", err, diagnostic.String())
	}
	var result pb.FinalizeUploadResponse
	if err := proto.Unmarshal(output, &result); err != nil {
		t.Fatal(err)
	}
	if result.ArtifactId == "" || result.GetObject().GetVersionId() == "" || result.GetObject().GetSizeBytes() != request.SizeBytes || result.GetObject().GetSha256() != request.Sha256 {
		t.Fatal("invalid Rust artifact evidence")
	}
	if err := objects.Verify(ctx, objectstore.Object{Key: result.Object.Key, Version: result.Object.VersionId, Size: int64(result.Object.SizeBytes), SHA256: result.Object.Sha256}); err != nil {
		t.Fatal("Rust uploaded incorrect bytes", err)
	}
	var uploads, artifacts int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM artifact_uploads WHERE worker_id=$1 AND request_id=$2", request.Authority.WorkerId, request.RequestId).Scan(&uploads); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM artifacts a JOIN artifact_uploads u ON a.upload_id=u.upload_id WHERE u.worker_id=$1 AND u.request_id=$2 AND a.id=$3 AND a.object_version=$4", request.Authority.WorkerId, request.RequestId, result.ArtifactId, result.Object.VersionId).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if uploads != 1 || artifacts != 1 || service.creates.Load() != 2 || service.finalizes.Load() != 3 {
		t.Fatal("Rust retry changed durable upload identity", uploads, artifacts, service.creates.Load(), service.finalizes.Load())
	}
}
