//go:build integration

package workerapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/objectstore"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRealMTLSMultipartPartGrantsAndExactFinalization(t *testing.T) {
	endpoint := os.Getenv("DISPATCH_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires combined PostgreSQL and object storage fixtures")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
			t.Fatal("isolated backend not ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
	pool, client, request := uploadRPCFixtureWithStorage(t, objects, 16<<20)
	body := append(bytes.Repeat([]byte("a"), 5<<20), []byte("end")...)
	hash := func(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
	request.SizeBytes = uint64(len(body))
	request.Sha256 = hash(body)
	request.PartCount = 2
	request.PartSizeBytes = 5 << 20
	grant, err := client.CreateUpload(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if grant.UploadId == "" || grant.UploadUrl != "" || len(grant.Parts) != 0 || grant.PartCount != 2 || grant.PartSizeBytes != 5<<20 {
		t.Fatal("initialization returned unbounded or incorrect grants")
	}
	if replay, err := client.CreateUpload(ctx, request); err != nil || !proto.Equal(replay, grant) {
		t.Fatal("multipart initialization replay changed identity", err)
	}
	var parts []*pb.CompletedPart
	for number := uint32(1); number <= 2; number++ {
		start := int(number-1) * (5 << 20)
		part := body[start:min(start+(5<<20), len(body))]
		r := &pb.GrantUploadPartRequest{Authority: request.Authority, UploadId: grant.UploadId, Number: number, Sha256: hash(part)}
		g, err := client.GrantUploadPart(ctx, r)
		if err != nil || g.Number != number || g.ExpiresUnixMs <= time.Now().UnixMilli() {
			t.Fatal("invalid part grant", err)
		}
		put, err := http.NewRequestWithContext(ctx, "PUT", g.Url, bytes.NewReader(part))
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range g.RequiredHeaders {
			put.Header.Set(k, v)
		}
		response, err := http.DefaultClient.Do(put)
		if err != nil {
			t.Fatal("part PUT failed")
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 65536))
		_ = response.Body.Close()
		if response.StatusCode != 200 || response.Header.Get("ETag") == "" {
			t.Fatal("part upload rejected", response.StatusCode)
		}
		parts = append(parts, &pb.CompletedPart{Number: number, Etag: response.Header.Get("ETag"), Sha256: r.Sha256})
		changed := proto.Clone(r).(*pb.GrantUploadPartRequest)
		changed.Sha256 = hash([]byte("different"))
		if _, err := client.GrantUploadPart(ctx, changed); status.Code(err) != codes.AlreadyExists {
			t.Fatal("part retry replaced bytes", err)
		}
	}
	finalize := &pb.FinalizeUploadRequest{Authority: request.Authority, RequestId: uuid.NewString(), UploadId: grant.UploadId, Object: &pb.ObjectVersion{Key: grant.ObjectKey, SizeBytes: request.SizeBytes, Sha256: request.Sha256}, Parts: parts}
	for _, change := range []func(*pb.ObjectVersion){
		func(o *pb.ObjectVersion) { o.Key += "-other" },
		func(o *pb.ObjectVersion) { o.SizeBytes-- },
		func(o *pb.ObjectVersion) { o.Sha256 = hash([]byte("other")) },
	} {
		wrong := proto.Clone(finalize).(*pb.FinalizeUploadRequest)
		change(wrong.Object)
		if _, err := client.FinalizeUpload(ctx, wrong); status.Code(err) != codes.InvalidArgument {
			t.Fatal("mismatched object reached multipart completion", err)
		}
	}
	verified, err := client.FinalizeUpload(ctx, finalize)
	if err != nil || verified.GetArtifactId() == "" || verified.GetObject().GetVersionId() == "" || verified.Object.Sha256 != request.Sha256 {
		t.Fatal("multipart did not produce verified exact version", err)
	}
	if replay, err := client.FinalizeUpload(ctx, finalize); err != nil || !proto.Equal(replay, verified) {
		t.Fatal("multipart finalization replay changed version", err)
	}
	if err := objects.Verify(ctx, objectstore.Object{Key: verified.Object.Key, Version: verified.Object.VersionId, Size: int64(request.SizeBytes), SHA256: request.Sha256}); err != nil {
		t.Fatal("completed bytes failed verification", err)
	}
	var stored, artifacts, partCount int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM artifact_multipart_completions WHERE object_version IS NOT NULL),(SELECT count(*) FROM artifacts),(SELECT count(*) FROM artifact_upload_parts)").Scan(&stored, &artifacts, &partCount); err != nil || stored != 1 || artifacts != 1 || partCount != 2 {
		t.Fatal("multipart lifecycle duplicated or lost metadata", stored, artifacts, partCount, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET state='CANCELLING',cancel_requested=true"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GrantUploadPart(ctx, &pb.GrantUploadPartRequest{Authority: request.Authority, UploadId: grant.UploadId, Number: 1, Sha256: parts[0].Sha256}); status.Convert(err).Message() != "UPLOAD_STOP_REQUESTED" {
		t.Fatal("cancelled multipart reminted a part capability", err)
	}
}
