//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/objectstore"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRustExecutionPublishesVerifiedOutputAndReplaysCompletion(t *testing.T) {
	testRustLaunchScenario(t, "publish")
}
func TestRustExecutionRetainsDiagnosticOutputWithoutAcceptingFailedJob(t *testing.T) {
	testRustLaunchScenario(t, "publish_failed")
}

func TestRustExecutionCompletesOutputAndTransferFailures(t *testing.T) {
	for _, mode := range []string{"publish_missing", "publish_oversized", "publish_failed_missing", "publish_transfer_failed"} {
		t.Run(mode, func(t *testing.T) { testRustLaunchScenario(t, mode) })
	}
}

type publicationEvidence struct {
	puts                            atomic.Int32
	create                          *pb.CreateUploadRequest
	finalize                        *pb.FinalizeUploadRequest
	complete                        *pb.CompleteAttemptRequest
	creates, finalizes, completions int
}

func (s *launchService) CreateUpload(ctx context.Context, r *pb.CreateUploadRequest) (*pb.CreateUploadResponse, error) {
	reply, err := s.WorkerServiceServer.CreateUpload(ctx, r)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publication.creates++
	if s.publication.create == nil {
		s.publication.create = proto.Clone(r).(*pb.CreateUploadRequest)
		return nil, status.Error(codes.Unavailable, "injected lost grant reply")
	}
	if !proto.Equal(s.publication.create, r) {
		s.t.Error("upload declaration changed on retry")
	}
	return reply, nil
}
func (s *launchService) FinalizeUpload(ctx context.Context, r *pb.FinalizeUploadRequest) (*pb.FinalizeUploadResponse, error) {
	reply, err := s.WorkerServiceServer.FinalizeUpload(ctx, r)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publication.finalizes++
	if s.publication.finalize == nil {
		s.publication.finalize = proto.Clone(r).(*pb.FinalizeUploadRequest)
		return nil, status.Error(codes.Unavailable, "injected lost artifact reply")
	}
	if !proto.Equal(s.publication.finalize, r) {
		s.t.Error("artifact finalization changed on retry")
	}
	return reply, nil
}
func (s *launchService) CompleteAttempt(ctx context.Context, r *pb.CompleteAttemptRequest) (*pb.CompleteAttemptResponse, error) {
	reply, err := s.WorkerServiceServer.CompleteAttempt(ctx, r)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publication.completions++
	if s.publication.complete == nil {
		// Acceptance has committed. The Rust probe must observe terminal lease
		// rejection before retrying this exact durable completion, without uploads.
		s.publication.complete = proto.Clone(r).(*pb.CompleteAttemptRequest)
		return nil, status.Error(codes.Unavailable, "injected lost completion reply")
	}
	if !proto.Equal(s.publication.complete, r) {
		s.t.Error("completion changed on retry")
	}
	return reply, nil
}

func publicationStorage(t *testing.T, mode string, evidence *publicationEvidence) *objectstore.Store {
	if mode == "publish_transfer_failed" {
		// Grant issuance stays available while the data plane rejects every PUT.
		// No artifact bytes or versions are fabricated by this failure fixture.
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Query().Has("versioning") {
				_, _ = w.Write([]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
				return
			}
			if r.Method == http.MethodPut {
				evidence.puts.Add(1)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		t.Cleanup(backend.Close)
		objects, err := objectstore.New(objectstore.Config{Endpoint: backend.URL, Region: "us-east-1", Bucket: "dispatch-test", AccessKey: "fixture", SecretKey: "fixture", AllowLoopbackHTTP: true})
		if err != nil {
			t.Fatal(err)
		}
		return objects
	}
	t.Helper()
	endpoint := os.Getenv("DISPATCH_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires the combined PostgreSQL and object storage fixture")
	}
	cfg := objectstore.Config{Endpoint: endpoint, Region: "us-east-1", Bucket: "dispatch-test", AccessKey: os.Getenv("DISPATCH_TEST_S3_ACCESS_KEY"), SecretKey: os.Getenv("DISPATCH_TEST_S3_SECRET_KEY"), AllowLoopbackHTTP: true}
	objects, err := objectstore.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	admin := s3.New(s3.Options{BaseEndpoint: aws.String(endpoint), Region: cfg.Region, UsePathStyle: true, Credentials: awscredentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""), Retryer: aws.NopRetryer{}, HTTPClient: &http.Client{Timeout: 5 * time.Second}})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		_, err = admin.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(cfg.Bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
		if err == nil {
			return objects
		}
		if ctx.Err() != nil {
			t.Fatal("isolated storage did not become ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func verifyPublication(t *testing.T, ctx context.Context, pool *pgxpool.Pool, objects *objectstore.Store, evidence *publicationEvidence, attempt, artifact, wantState string) {
	t.Helper()
	if evidence.creates != 2 || evidence.finalizes != 2 || evidence.completions != 2 || artifact == "" {
		t.Fatal("missing exact publication retries", evidence.creates, evidence.finalizes, evidence.completions)
	}
	if evidence.complete.LogsComplete || !evidence.complete.Stopped || len(evidence.complete.Outputs) != 1 || evidence.complete.Outputs[0].ArtifactId != artifact {
		t.Fatal("completion did not bind observed output and stopped evidence")
	}
	wantReason := pb.FailureReason_FAILURE_REASON_UNSPECIFIED
	if wantState == "FAILED" {
		wantReason = pb.FailureReason_APPLICATION_EXIT
	}
	if evidence.complete.Reason != wantReason {
		t.Fatal("incorrect failure classification", evidence.complete.Reason)
	}
	var jobState, reservation string
	var accepted *string
	var manifest []byte
	var completions, artifacts, uploads, events int
	err := pool.QueryRow(ctx, `SELECT j.state,r.state,j.accepted_attempt_id::text,j.accepted_manifest,
        (SELECT count(*) FROM attempt_completions WHERE attempt_id=a.id),
        (SELECT count(*) FROM artifacts WHERE attempt_id=a.id),
        (SELECT count(*) FROM artifact_uploads WHERE attempt_id=a.id),
        (SELECT count(*) FROM job_events WHERE job_id=j.id AND type='ATTEMPT_COMPLETED')
        FROM attempts a JOIN jobs j ON j.id=a.job_id JOIN reservations r ON r.attempt_id=a.id WHERE a.id=$1`, attempt).
		Scan(&jobState, &reservation, &accepted, &manifest, &completions, &artifacts, &uploads, &events)
	if err != nil || jobState != wantState || reservation != "released" || completions != 1 || artifacts != 1 || uploads != 1 || events != 1 {
		t.Fatal("publication was not atomic and singular", jobState, reservation, completions, artifacts, uploads, events, err)
	}
	object := evidence.finalize.Object
	exact := objectstore.Object{Key: object.Key, Version: object.VersionId, Size: int64(object.SizeBytes), SHA256: object.Sha256}
	if err := objects.Verify(ctx, exact); err != nil {
		t.Fatal("published artifact did not match immutable storage version", err)
	}
	if wantState == "FAILED" {
		if accepted != nil || len(manifest) != 0 {
			t.Fatal("failed output became the accepted result")
		}
		return
	}
	var result struct {
		Outputs []struct {
			ArtifactID string `json:"artifactId"`
			Object     struct {
				Key     string `json:"key"`
				Version string `json:"version"`
			} `json:"object"`
		} `json:"outputs"`
		LogsComplete bool `json:"logsComplete"`
	}
	if err := json.Unmarshal(manifest, &result); err != nil || accepted == nil || *accepted != attempt || len(result.Outputs) != 1 || result.LogsComplete {
		t.Fatal("invalid accepted result", err)
	}
	if result.Outputs[0].ArtifactID != artifact || result.Outputs[0].Object.Key != exact.Key || result.Outputs[0].Object.Version != exact.Version {
		t.Fatal("accepted result did not pin the verified artifact")
	}
}

func verifyFailedPublication(t *testing.T, ctx context.Context, pool *pgxpool.Pool, evidence *publicationEvidence, attempt, mode string) {
	t.Helper()
	wantReason := pb.FailureReason_OUTPUT_INVALID
	wantCreates, wantUploads, wantPuts := 0, 0, int32(0)
	if mode == "publish_failed_missing" {
		wantReason = pb.FailureReason_APPLICATION_EXIT
	}
	if mode == "publish_transfer_failed" {
		wantReason = pb.FailureReason_TRANSFER_FAILED
		// Three delivery rounds: one lost grant response, then two failed PUTs.
		wantCreates, wantUploads, wantPuts = 3, 1, 2
	}
	if evidence.creates != wantCreates || evidence.finalizes != 0 || evidence.completions != 2 || evidence.puts.Load() != wantPuts {
		t.Fatal("unexpected failed-publication retries", evidence.creates, evidence.finalizes, evidence.completions, evidence.puts.Load())
	}
	request := evidence.complete
	if request == nil || request.Reason != wantReason || !request.Stopped || request.LogsComplete || len(request.Outputs) != 0 {
		t.Fatal("incorrect durable failed completion")
	}
	var state, reservation, reason string
	var accepted *string
	var completions, artifacts, uploads int
	err := pool.QueryRow(ctx, `SELECT j.state,r.state,a.reason,j.accepted_attempt_id::text,
        (SELECT count(*) FROM attempt_completions WHERE attempt_id=a.id),
        (SELECT count(*) FROM artifacts WHERE attempt_id=a.id),
        (SELECT count(*) FROM artifact_uploads WHERE attempt_id=a.id)
        FROM attempts a JOIN jobs j ON j.id=a.job_id JOIN reservations r ON r.attempt_id=a.id WHERE a.id=$1`, attempt).
		Scan(&state, &reservation, &reason, &accepted, &completions, &artifacts, &uploads)
	if err != nil || state != "FAILED" || reservation != "released" || reason != wantReason.String() || accepted != nil || completions != 1 || artifacts != 0 || uploads != wantUploads {
		t.Fatal("failure did not terminalize atomically", state, reservation, reason, completions, artifacts, uploads, err)
	}
}
