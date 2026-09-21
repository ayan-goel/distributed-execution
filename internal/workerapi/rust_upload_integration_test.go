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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type lostUploadReplyService struct {
	pb.WorkerServiceServer
	creates      atomic.Int32
	finalizes    atomic.Int32
	declaration  atomic.Pointer[pb.CreateUploadRequest]
	finalization atomic.Pointer[pb.FinalizeUploadRequest]
	committed    chan struct{}
	deny         atomic.Bool
}

func (s *lostUploadReplyService) CreateUpload(ctx context.Context, r *pb.CreateUploadRequest) (*pb.CreateUploadResponse, error) {
	if s.deny.Load() {
		return nil, status.Error(codes.Unavailable, "fixture control unavailable")
	}
	s.declaration.CompareAndSwap(nil, proto.Clone(r).(*pb.CreateUploadRequest))
	if !proto.Equal(s.declaration.Load(), r) {
		return nil, status.Error(codes.Internal, "upload retry changed declaration")
	}
	response, err := s.WorkerServiceServer.CreateUpload(ctx, r)
	if err == nil && s.creates.Add(1) == 1 {
		return nil, status.Error(codes.Unavailable, "injected lost upload grant")
	}
	return response, err
}

func (s *lostUploadReplyService) FinalizeUpload(ctx context.Context, r *pb.FinalizeUploadRequest) (*pb.FinalizeUploadResponse, error) {
	if s.deny.Load() {
		return nil, status.Error(codes.Unavailable, "fixture control unavailable")
	}
	s.finalization.CompareAndSwap(nil, proto.Clone(r).(*pb.FinalizeUploadRequest))
	if !proto.Equal(s.finalization.Load(), r) {
		return nil, status.Error(codes.Internal, "upload retry changed version evidence")
	}
	response, err := s.WorkerServiceServer.FinalizeUpload(ctx, r)
	if err == nil && s.finalizes.Add(1) == 1 {
		// Hold every reply byte until the parent kills the process. A new process
		// must resolve the journaled version instead of repeating the file upload.
		close(s.committed)
		<-ctx.Done()
		return nil, status.Error(codes.Unavailable, "injected lost artifact acknowledgement")
	}
	return response, err
}

// Verify the Rust data path against the same isolated versioned storage profile
// as the Go client. A separate credential binds it to the existing fixture worker.
func verifyRustUpload(t *testing.T, pool *pgxpool.Pool, worker pb.WorkerServiceClient, objects *objectstore.Store, admin *s3.Client, declaration *pb.CreateUploadRequest, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	request := proto.Clone(declaration).(*pb.CreateUploadRequest)
	page, err := worker.ListAssignments(ctx, &pb.ListAssignmentsRequest{Session: &pb.WorkerSession{WorkerId: request.Authority.WorkerId, SessionId: request.Authority.SessionId}, PageSize: 1})
	if err != nil || len(page.GetAssignments()) != 1 {
		t.Fatal("missing upload assignment fixture", err)
	}
	ca, roots := testCA(t)
	cert := testLeaf(t, ca, x509.ExtKeyUsageClientAuth)
	fingerprint := sha256.Sum256(cert.Certificate[0])
	if _, err := pool.Exec(ctx, "INSERT INTO worker_credentials(worker_id,certificate_sha256) VALUES($1,$2)", request.Authority.WorkerId, fingerprint[:]); err != nil {
		t.Fatal(err)
	}
	service := &lostUploadReplyService{WorkerServiceServer: NewService(pool, store.AcquisitionPolicy{}, objects), committed: make(chan struct{})}
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
	if err := os.Mkdir(filepath.Join(root, "state"), 0700); err != nil {
		t.Fatal(err)
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
	raw, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := proto.Marshal(page.Assignments[0])
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{"declaration.pb": raw, "assignment.pb": assignment, "result": []byte(body)} {
		if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	probe, err := filepath.Abs("../../.local/cargo-target/debug/examples/upload_probe")
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"https://" + listener.Addr().String(), filepath.Join(root, "ca.pem"), filepath.Join(root, "cert.pem"), filepath.Join(root, "key.pem"), filepath.Join(root, "declaration.pb"), filepath.Join(root, "result"), filepath.Join(root, "state"), filepath.Join(root, "assignment.pb")}
	// Before any durable storage version exists, lost local bytes must fail
	// explicitly without creating an upload or claiming a verified artifact.
	if err := os.Rename(filepath.Join(root, "result"), filepath.Join(root, "source")); err != nil {
		t.Fatal(err)
	}
	missing := exec.CommandContext(ctx, probe, args...)
	if output, err := missing.CombinedOutput(); err == nil || !bytes.Contains(output, []byte("output source is unavailable")) || service.declaration.Load() != nil {
		t.Fatal("missing source did not fail before network mutation", err, string(output))
	}
	if err := os.Rename(filepath.Join(root, "source"), filepath.Join(root, "result")); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, probe, args...)
	var diagnostic bytes.Buffer
	command.Stderr = &diagnostic
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	firstProcess := command.Process
	t.Cleanup(func() { _ = firstProcess.Kill() })
	select {
	case <-service.committed:
		_ = command.Process.Kill()
		<-done
	case err := <-done:
		t.Fatal("upload exited before committed reply was lost", err, diagnostic.String())
	case <-ctx.Done():
		_ = command.Process.Kill()
		<-done
		t.Fatal("upload did not reach committed finalization", diagnostic.String())
	}
	// The only source is gone. Successful recovery must use the immutable version
	// already saved before the RPC; a cached acknowledgement needs no RPC at all.
	if err := os.Remove(filepath.Join(root, "result")); err != nil {
		t.Fatal(err)
	}
	var result *pb.FinalizeUploadResponse
	for restart := range 2 {
		command = exec.CommandContext(ctx, probe, args...)
		var diagnostic bytes.Buffer
		command.Stderr = &diagnostic
		output, err := command.Output()
		if err != nil {
			t.Fatal("journaled Rust upload recovery failed", err, diagnostic.String())
		}
		var reply pb.FinalizeUploadResponse
		if err := proto.Unmarshal(output, &reply); err != nil {
			t.Fatal(err)
		}
		if restart == 0 {
			result = &reply
		} else if !proto.Equal(&reply, result) {
			t.Fatal("reopen changed artifact acknowledgement")
		}
		service.deny.Store(true)
	}
	if result.ArtifactId == "" || result.GetObject().GetVersionId() == "" || result.GetObject().GetSizeBytes() != request.SizeBytes || result.GetObject().GetSha256() != request.Sha256 {
		t.Fatal("invalid Rust artifact evidence")
	}
	if err := objects.Verify(ctx, objectstore.Object{Key: result.Object.Key, Version: result.Object.VersionId, Size: int64(result.Object.SizeBytes), SHA256: result.Object.Sha256}); err != nil {
		t.Fatal("Rust uploaded incorrect bytes", err)
	}
	var uploads, artifacts int
	request.RequestId = service.declaration.Load().RequestId
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM artifact_uploads WHERE worker_id=$1 AND request_id=$2", request.Authority.WorkerId, request.RequestId).Scan(&uploads); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM artifacts a JOIN artifact_uploads u ON a.upload_id=u.upload_id WHERE u.worker_id=$1 AND u.request_id=$2 AND a.id=$3 AND a.object_version=$4", request.Authority.WorkerId, request.RequestId, result.ArtifactId, result.Object.VersionId).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if uploads != 1 || artifacts != 1 || service.creates.Load() != 2 || service.finalizes.Load() != 2 {
		t.Fatal("Rust retry changed durable upload identity", uploads, artifacts, service.creates.Load(), service.finalizes.Load())
	}
	versions, err := admin.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String("dispatch-test"), Prefix: aws.String(result.Object.Key)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToBool(versions.IsTruncated) || len(versions.DeleteMarkers) != 0 || len(versions.Versions) != 1 || aws.ToString(versions.Versions[0].VersionId) != result.Object.VersionId {
		t.Fatal("recovery repeated PUT or changed the immutable object version")
	}
}
