//go:build integration

package workerapi

import (
	"context"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRealMTLSLogSegmentRegistrationPinsVerifiedArtifact(t *testing.T) {
	pool, client, upload := uploadRPCFixture(t, enabledVersioning)
	upload.Kind, upload.Name, upload.SizeBytes = pb.ArtifactKind_LOG, "stdout", 3
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	grant, err := client.CreateUpload(ctx, upload)
	if err != nil {
		t.Fatal(err)
	}
	id := store.WorkerIdentity{WorkerID: upload.Authority.WorkerId}
	if err := pool.QueryRow(ctx, "SELECT id::text FROM worker_credentials WHERE worker_id=$1", id.WorkerID).Scan(&id.CredentialID); err != nil {
		t.Fatal(err)
	}
	verified, err := store.FinalizeUpload(ctx, pool, id, store.FinalizeUploadRequest{
		Authority: store.AttemptAuthority{JobID: upload.Authority.JobId, AttemptID: upload.Authority.AttemptId, WorkerID: upload.Authority.WorkerId, SessionID: upload.Authority.SessionId, Generation: int64(upload.Authority.Generation)},
		RequestID: uuid.NewString(), UploadID: grant.UploadId,
		Object: store.ArtifactObject{Key: grant.ObjectKey, Version: "log-version", SizeBytes: int64(upload.SizeBytes), SHA256: upload.Sha256},
	}, func(context.Context, store.ArtifactObject) error { return nil })
	if err != nil || verified.Artifact == nil {
		t.Fatal(verified, err)
	}
	r := &pb.RegisterLogSegmentRequest{Authority: upload.Authority, RequestId: uuid.NewString(), ArtifactId: verified.Artifact.ArtifactID, Stream: pb.LogStream_STDOUT, FirstSequence: 1, LastSequence: 3, Gaps: []*pb.LogGap{{Stream: pb.LogStream_STDOUT, FirstSequence: 2, LastSequence: 2}}}
	first, err := client.RegisterLogSegment(ctx, r)
	if err != nil || first.GetDecision() != pb.Decision_ACCEPTED || first.GetState() != pb.AttemptState_FINALIZING {
		t.Fatal(first, err)
	}
	if replay, err := client.RegisterLogSegment(ctx, r); err != nil || replay.GetDecision() != pb.Decision_ACCEPTED {
		t.Fatal("exact registration replay changed", replay, err)
	}
	r.Gaps[0].Stream = pb.LogStream_STDERR
	if _, err := client.RegisterLogSegment(ctx, r); status.Code(err) != codes.InvalidArgument {
		t.Fatal("cross-stream gap accepted", err)
	}
	r.Gaps[0].Stream = pb.LogStream_STDOUT
	r.LastSequence = 4
	if _, err := client.RegisterLogSegment(ctx, r); status.Code(err) != codes.AlreadyExists {
		t.Fatal("changed retry retained registration identity", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM log_segments").Scan(&count); err != nil || count != 1 {
		t.Fatal("gRPC retry duplicated catalog row", count, err)
	}
}
