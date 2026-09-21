package workerapi

import (
	"context"
	"errors"
	"math"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) FinalizeUpload(ctx context.Context, r *pb.FinalizeUploadRequest) (*pb.FinalizeUploadResponse, error) {
	id, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(r, id.WorkerID); err != nil {
		return nil, err
	}
	a, o := r.GetAuthority(), r.GetObject()
	if a.GetGeneration() > math.MaxInt64 || o == nil || o.GetSizeBytes() > uint64(store.MaxUploadBytes) || len(r.GetParts()) != 0 {
		return nil, rpcError(store.ErrInvalid)
	}
	if s.objects == nil {
		return nil, status.Error(codes.FailedPrecondition, "OBJECT_STORAGE_NOT_CONFIGURED")
	}
	request := store.FinalizeUploadRequest{Authority: store.AttemptAuthority{JobID: a.GetJobId(), AttemptID: a.GetAttemptId(), WorkerID: a.GetWorkerId(), SessionID: a.GetSessionId(), Generation: int64(a.GetGeneration())}, RequestID: r.GetRequestId(), UploadID: r.GetUploadId(), Object: store.ArtifactObject{Key: o.GetKey(), Version: o.GetVersionId(), SizeBytes: int64(o.GetSizeBytes()), SHA256: o.GetSha256()}}
	// The store invokes this only after matching durable upload scope, with no
	// transaction open, and must recheck authority before recording its success.
	result, err := store.FinalizeUpload(ctx, s.pool, id, request, func(ctx context.Context, o store.ArtifactObject) error {
		return s.objects.Verify(ctx, objectstore.Object{Key: o.Key, Version: o.Version, Size: o.SizeBytes, SHA256: o.SHA256})
	})
	if err != nil {
		switch {
		case errors.Is(err, objectstore.ErrIntegrity):
			return nil, status.Error(codes.FailedPrecondition, "OBJECT_INTEGRITY_MISMATCH")
		case errors.Is(err, objectstore.ErrInvalid), errors.Is(err, objectstore.ErrUnavailable), errors.Is(err, objectstore.ErrVersioning):
			return nil, uploadStorageError(err)
		default:
			return nil, rpcError(err)
		}
	}
	if err := uploadDecision(result.Decision); err != nil {
		return nil, err
	}
	artifact := result.Artifact
	if artifact == nil || artifact.ArtifactID == "" || artifact.UploadID != request.UploadID || artifact.Object != request.Object {
		return nil, status.Error(codes.Internal, "INVALID_ARTIFACT_RESULT")
	}
	return &pb.FinalizeUploadResponse{ArtifactId: artifact.ArtifactID, Object: &pb.ObjectVersion{Key: artifact.Object.Key, VersionId: artifact.Object.Version, SizeBytes: uint64(artifact.Object.SizeBytes), Sha256: artifact.Object.SHA256}}, nil
}
