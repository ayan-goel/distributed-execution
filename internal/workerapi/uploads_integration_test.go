//go:build integration

package workerapi

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func enabledVersioning(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	_, _ = fmt.Fprint(w, `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`)
}

func uploadRPCFixture(t *testing.T, handler http.HandlerFunc) (*pgxpool.Pool, pb.WorkerServiceClient, *pb.CreateUploadRequest) {
	t.Helper()
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	objects, err := objectstore.New(objectstore.Config{Endpoint: backend.URL, Region: "us-east-1", Bucket: "dispatch-test", AccessKey: "fixture-access", SecretKey: "fixture-secret", AllowLoopbackHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	return uploadRPCFixtureWithStorage(t, objects)
}

func uploadRPCFixtureWithStorage(t *testing.T, objects *objectstore.Store) (*pgxpool.Pool, pb.WorkerServiceClient, *pb.CreateUploadRequest) {
	t.Helper()
	ctx := context.Background()
	pool := workerTestPool(t)
	ca, roots := testCA(t)
	cert := testLeaf(t, ca, x509.ExtKeyUsageClientAuth)
	p := store.WorkerProvision{Name: "upload-host", CertificateSHA256: sha256.Sum256(cert.Certificate[0]), Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4, Projects: []string{"research"}, Labels: map[string]string{"os": "linux", "architecture": "arm64"}}
	id, err := store.ProvisionWorker(ctx, pool, p)
	if err != nil {
		t.Fatal(err)
	}
	reg := store.Registration{RequestID: uuid.NewString(), SessionID: uuid.NewString(), ProtocolVersion: 1, Resources: p.Resources, Slots: p.Slots, Labels: p.Labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}}
	if _, err := store.RegisterSession(ctx, pool, id, reg); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordHeartbeat(ctx, pool, id, store.HeartbeatReport{RequestID: uuid.NewString(), SessionID: reg.SessionID, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	job, err := spec.DecodeJob(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	job.Spec.Inputs = nil
	job.Spec.Placement.Labels["architecture"] = "arm64"
	job.Spec.Image = "registry.example.org/eval@sha256:" + strings.Repeat("a", 64)
	job.Spec.Outputs = []spec.Output{{Name: "result", Path: "/outputs/result", MaxBytes: 1024, Required: true}}
	_, hash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SubmitJob(ctx, pool, uuid.NewString(), hash, job); err != nil {
		t.Fatal(err)
	}
	acquired, err := store.AcquireWork(ctx, pool, id, store.AcquisitionRequest{SessionID: reg.SessionID, RequestID: uuid.NewString()}, store.AcquisitionPolicy{})
	if err != nil || acquired.Assignment == nil {
		t.Fatal(acquired, err)
	}
	a := acquired.Assignment.Authority
	exit := int32(0)
	for _, phase := range []store.PhaseReport{
		{Authority: a, EventID: uuid.NewString(), Phase: "STARTING"},
		{Authority: a, EventID: uuid.NewString(), Phase: "FINALIZING", ContainerID: strings.Repeat("a", 64), ExitCode: &exit},
	} {
		if result, err := store.ReportPhase(ctx, pool, id, phase); err != nil || result.Decision != "ACCEPTED" {
			t.Fatal(result, err)
		}
	}
	server, err := NewServer(pool, testLeaf(t, ca, x509.ExtKeyUsageServerAuth), roots, NewService(pool, store.AcquisitionPolicy{}, objects))
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
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pool, pb.NewWorkerServiceClient(conn), &pb.CreateUploadRequest{Authority: &pb.AttemptAuthority{JobId: a.JobID, AttemptId: a.AttemptID, WorkerId: a.WorkerID, SessionId: a.SessionID, Generation: uint64(a.Generation)}, RequestId: uuid.NewString(), Name: "result", Kind: pb.ArtifactKind_OUTPUT, SizeBytes: 32, Sha256: strings.Repeat("a", 64), PartCount: 1}
}

func TestRealMTLSUploadAndFinalizationPinVerifiedVersion(t *testing.T) {
	realMTLSArtifactFlow(t, false)
}

func TestRealAcceptedDownloadReturnsOriginalVerifiedVersion(t *testing.T) {
	realMTLSArtifactFlow(t, true)
}

func realMTLSArtifactFlow(t *testing.T, complete bool) {
	t.Helper()
	endpoint := os.Getenv("DISPATCH_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires the combined PostgreSQL and object storage fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := objectstore.Config{Endpoint: endpoint, Region: "us-east-1", Bucket: "dispatch-test", AccessKey: os.Getenv("DISPATCH_TEST_S3_ACCESS_KEY"), SecretKey: os.Getenv("DISPATCH_TEST_S3_SECRET_KEY"), AllowLoopbackHTTP: true}
	objects, err := objectstore.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	admin := s3.New(s3.Options{BaseEndpoint: aws.String(endpoint), Region: cfg.Region, UsePathStyle: true, Credentials: awscredentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""), Retryer: aws.NopRetryer{}, HTTPClient: &http.Client{Timeout: 5 * time.Second}})
	ready := time.Now().Add(15 * time.Second)
	for {
		_, err := admin.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(cfg.Bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
		if err == nil {
			break
		}
		if time.Now().After(ready) {
			t.Fatal("isolated object store did not become ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
	pool, client, request := uploadRPCFixtureWithStorage(t, objects)
	body := "worker output through authenticated RPC"
	hash := sha256.Sum256([]byte(body))
	request.SizeBytes = uint64(len(body))
	request.Sha256 = hex.EncodeToString(hash[:])
	grant, err := client.CreateUpload(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	put := func(grant *pb.CreateUploadResponse, body string) (int, string) {
		t.Helper()
		r, err := http.NewRequestWithContext(ctx, "PUT", grant.UploadUrl, strings.NewReader(body))
		if err != nil {
			t.Fatal("invalid signed URL")
		}
		for name, value := range grant.RequiredHeaders {
			r.Header.Set(name, value)
		}
		response, err := (&http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(r)
		if err != nil {
			t.Fatal("scoped upload transport failed")
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 65536))
		return response.StatusCode, response.Header.Get("X-Amz-Version-Id")
	}
	code, version := put(grant, body)
	if code != http.StatusOK || version == "" || version == "null" {
		t.Fatal("RPC grant could not upload a version", code)
	}
	object := objectstore.Object{Key: grant.ObjectKey, Version: version, Size: int64(request.SizeBytes), SHA256: request.Sha256}
	if err := objects.Verify(ctx, object); err != nil {
		t.Fatal("RPC grant changed declared bytes", err)
	}
	// Replace the current object with different bytes via fixture administration.
	// Finalization must read the requested version, never the current key or ETag.
	wrong, err := admin.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String(grant.ObjectKey), Body: strings.NewReader(strings.Repeat("x", len(body))), ContentLength: aws.Int64(int64(len(body)))})
	if err != nil || aws.ToString(wrong.VersionId) == "" {
		t.Fatal("could not create alternate version")
	}
	finalize := &pb.FinalizeUploadRequest{Authority: request.Authority, RequestId: uuid.NewString(), UploadId: grant.UploadId, Object: &pb.ObjectVersion{Key: grant.ObjectKey, VersionId: aws.ToString(wrong.VersionId), SizeBytes: request.SizeBytes, Sha256: request.Sha256}}
	if result, err := client.FinalizeUpload(ctx, finalize); result != nil || status.Convert(err).Message() != "OBJECT_INTEGRITY_MISMATCH" {
		t.Fatal("wrong version passed finalization", err)
	}
	finalize.RequestId = uuid.NewString()
	finalize.Object.VersionId = version
	verified, err := client.FinalizeUpload(ctx, finalize)
	if err != nil || verified.GetArtifactId() == "" || !proto.Equal(verified.GetObject(), finalize.Object) {
		t.Fatal("exact version was not registered", err)
	}
	if replay, err := client.FinalizeUpload(ctx, finalize); err != nil || !proto.Equal(replay, verified) {
		t.Fatal("finalization replay changed artifact", err)
	}
	verifyRustUpload(t, pool, objects, request, body)
	if code, _ := put(grant, strings.Repeat("x", len(body))); code == http.StatusOK {
		t.Fatal("wire grant accepted wrong checksum")
	}
	other := proto.Clone(grant).(*pb.CreateUploadResponse)
	other.UploadUrl = strings.Replace(other.UploadUrl, grant.UploadId, uuid.NewString(), 1)
	if code, _ := put(other, body); code == http.StatusOK {
		t.Fatal("wire grant authorized another object key")
	}
	if complete {
		verifyAcceptedDownload(t, pool, client, objects, request.Authority, verified, body)
		return
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET state='CANCELLING',cancel_requested=true"); err != nil {
		t.Fatal(err)
	}
	if result, err := client.CreateUpload(ctx, request); result != nil || status.Convert(err).Message() != "UPLOAD_STOP_REQUESTED" {
		t.Fatal("cancelled attempt reminted a grant", err)
	}
	if result, err := client.FinalizeUpload(ctx, finalize); result != nil || status.Convert(err).Message() != "UPLOAD_STOP_REQUESTED" {
		t.Fatal("cancelled attempt reused verification authority", err)
	}
	// Already issued capabilities can still write only their isolated key. Exact
	// versions protect prior bytes; finalization/publication needs separate fencing.
	code, later := put(grant, body)
	if code != http.StatusOK || later == version || later == "" || later == "null" {
		t.Fatal("upload replay did not yield a separate version", code)
	}
	if err := objects.Verify(ctx, object); err != nil {
		t.Fatal("reused capability changed the original version", err)
	}
	var saved string
	if err := pool.QueryRow(ctx, "SELECT object_version FROM artifacts WHERE id=$1", verified.ArtifactId).Scan(&saved); err != nil || saved != version {
		t.Fatal("later upload rebound verified artifact", err)
	}
}

func TestMTLSUploadGrantUsesDurableScopeAndReplay(t *testing.T) {
	pool, client, request := uploadRPCFixture(t, enabledVersioning)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var lease, phase time.Time
	if err := pool.QueryRow(ctx, "SELECT lease_expires_at,phase_deadline FROM attempts").Scan(&lease, &phase); err != nil {
		t.Fatal(err)
	}
	first, err := client.CreateUpload(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(first.UploadUrl)
	if err != nil {
		t.Fatal("invalid grant URL")
	}
	if !strings.HasSuffix(parsed.Path, "/"+first.ObjectKey) || !strings.Contains(first.ObjectKey, "/attempts/"+request.Authority.AttemptId+"/uploads/"+first.UploadId) || parsed.Query().Get("X-Amz-Expires") != "30" || len(first.Parts) != 0 || first.ExpiresUnixMs <= time.Now().UnixMilli() || first.ExpiresUnixMs > time.Now().Add(30*time.Second).UnixMilli() {
		t.Fatal("grant scope or lifetime changed")
	}
	signed := parsed.Query().Get("X-Amz-SignedHeaders")
	if !strings.Contains(signed, "content-length") || !strings.Contains(signed, "x-amz-checksum-sha256") || first.RequiredHeaders["X-Amz-Checksum-Sha256"] != base64.StdEncoding.EncodeToString([]byte(strings.Repeat("\xaa", 32))) {
		t.Fatal("grant did not bind declared bytes")
	}
	again, err := client.CreateUpload(ctx, request)
	if err != nil || again.GetUploadId() != first.UploadId || again.GetObjectKey() != first.ObjectKey {
		t.Fatal("replay changed upload identity", err)
	}
	var count, events int
	var afterLease, afterPhase time.Time
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM artifact_uploads),(SELECT count(*) FROM job_events WHERE type='UPLOAD_CREATED'),lease_expires_at,phase_deadline FROM attempts").Scan(&count, &events, &afterLease, &afterPhase); err != nil || count != 1 || events != 1 || !lease.Equal(afterLease) || !phase.Equal(afterPhase) {
		t.Fatal("grant replay changed authority or accounting", err)
	}
	for _, tc := range []struct {
		mutate func(*pb.CreateUploadRequest)
		code   codes.Code
	}{
		{func(r *pb.CreateUploadRequest) { r.Sha256 = strings.Repeat("b", 64) }, codes.AlreadyExists},
		{func(r *pb.CreateUploadRequest) { r.Authority.WorkerId = uuid.NewString() }, codes.PermissionDenied},
		{func(r *pb.CreateUploadRequest) { r.RequestId = uuid.NewString(); r.Name = "undeclared" }, codes.InvalidArgument},
		{func(r *pb.CreateUploadRequest) { r.Authority.Generation++ }, codes.FailedPrecondition},
	} {
		bad := proto.Clone(request).(*pb.CreateUploadRequest)
		tc.mutate(bad)
		if response, err := client.CreateUpload(ctx, bad); response != nil || status.Code(err) != tc.code {
			t.Fatal("invalid upload returned capability", err)
		}
	}
}

func TestUploadRechecksAuthorityAfterStorageWaitWithoutHoldingLocks(t *testing.T) {
	for _, tc := range []struct {
		name, mutation, reason string
		code                   codes.Code
	}{
		{"lease", "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second'", "UPLOAD_FENCED", codes.FailedPrecondition},
		{"phase", "UPDATE attempts SET phase_deadline=clock_timestamp()-interval '1 second'", "UPLOAD_STOP_REQUESTED", codes.FailedPrecondition},
		{"cancel", "UPDATE jobs SET state='CANCELLING',cancel_requested=true", "UPLOAD_STOP_REQUESTED", codes.FailedPrecondition},
		{"credential", "UPDATE worker_credentials SET revoked_at=clock_timestamp()", "UNAUTHORIZED_WORKER", codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, release := make(chan struct{}, 1), make(chan struct{})
			var once sync.Once
			pool, client, request := uploadRPCFixture(t, func(w http.ResponseWriter, r *http.Request) {
				started <- struct{}{}
				select {
				case <-release:
					enabledVersioning(w, r)
				case <-r.Context().Done():
				}
			})
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			errors := make(chan error, 1)
			go func() {
				result, err := client.CreateUpload(ctx, request)
				if result != nil {
					err = fmt.Errorf("returned a capability after authority changed")
				}
				errors <- err
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("storage check did not start")
			}
			// The mutation must commit while the storage request is still blocked.
			// This proves signing does not retain credential/job/attempt row locks.
			mutationCtx, stop := context.WithTimeout(ctx, time.Second)
			_, err := pool.Exec(mutationCtx, tc.mutation)
			stop()
			if err != nil {
				t.Fatal("storage I/O held database locks", err)
			}
			once.Do(func() { close(release) })
			if err := <-errors; status.Code(err) != tc.code || status.Convert(err).Message() != tc.reason {
				t.Fatal("stale grant crossed RPC boundary", err)
			}
		})
	}
}

func TestUploadStorageFailurePreservesRetryIdentity(t *testing.T) {
	var mode atomic.Int32
	pool, client, request := uploadRPCFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load() {
		case 0:
			http.Error(w, "private backend diagnostic", http.StatusInternalServerError)
		case 1:
			_, _ = fmt.Fprint(w, `<VersioningConfiguration/>`)
		default:
			enabledVersioning(w, r)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for index, reason := range []string{"OBJECT_STORAGE_UNAVAILABLE", "OBJECT_VERSIONING_REQUIRED"} {
		mode.Store(int32(index))
		if response, err := client.CreateUpload(ctx, request); response != nil || status.Convert(err).Message() != reason {
			t.Fatal("storage failure leaked a grant or diagnostic", err)
		}
	}
	mode.Store(2)
	if result, err := client.CreateUpload(ctx, request); err != nil || result.GetUploadId() == "" {
		t.Fatal("retry did not recover", err)
	}
	var count, events int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM artifact_uploads),(SELECT count(*) FROM job_events WHERE type='UPLOAD_CREATED')").Scan(&count, &events); err != nil || count != 1 || events != 1 {
		t.Fatal("failed signing consumed extra declarations", count, events, err)
	}
}
