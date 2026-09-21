package workerapi

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) CreateUpload(ctx context.Context, r *pb.CreateUploadRequest) (*pb.CreateUploadResponse, error) {
	id, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(r, id.WorkerID); err != nil {
		return nil, err
	}
	a := r.GetAuthority()
	if a.GetGeneration() > math.MaxInt64 || r.GetSizeBytes() > uint64(store.MaxUploadBytes) || r.GetPartCount() != 1 || !slices.Contains([]pb.ArtifactKind{pb.ArtifactKind_OUTPUT, pb.ArtifactKind_LOG, pb.ArtifactKind_MANIFEST}, r.GetKind()) {
		return nil, rpcError(store.ErrInvalid)
	}
	if s.objects == nil {
		return nil, status.Error(codes.FailedPrecondition, "OBJECT_STORAGE_NOT_CONFIGURED")
	}
	request := store.UploadRequest{Authority: store.AttemptAuthority{JobID: a.GetJobId(), AttemptID: a.GetAttemptId(), WorkerID: a.GetWorkerId(), SessionID: a.GetSessionId(), Generation: int64(a.GetGeneration())}, RequestID: r.GetRequestId(), Kind: r.GetKind().String(), LogicalName: r.GetName(), SizeBytes: int64(r.GetSizeBytes()), SHA256: r.GetSha256(), PartCount: int(r.GetPartCount())}
	initial, err := store.CreateUpload(ctx, s.pool, id, request)
	if err != nil {
		return nil, rpcError(err)
	}
	if err := uploadAllowed(initial); err != nil {
		return nil, err
	}
	upload := initial.Upload
	// Storage versioning checks run after the declaration transaction commits.
	// Capabilities last 30 seconds and authorize only this attempt's object key;
	// they can outlive ownership but never authorize accepting a job result.
	grant, err := s.objects.PresignUpload(ctx, upload.ObjectKey, upload.SizeBytes, upload.SHA256, 30*time.Second)
	if err != nil {
		return nil, uploadStorageError(err)
	}
	// Replaying the exact declaration rechecks live credentials/session, lease,
	// phase and cancellation after storage I/O without extending any authority.
	current, err := store.CreateUpload(ctx, s.pool, id, request)
	if err != nil {
		return nil, rpcError(err)
	}
	if err := uploadAllowed(current); err != nil {
		return nil, err
	}
	if *current.Upload != *upload || grant.Method != "PUT" {
		return nil, status.Error(codes.Internal, "INVALID_UPLOAD_RESULT")
	}
	if err := ctx.Err(); err != nil {
		return nil, rpcError(err)
	}
	if !grant.ExpiresAt.After(time.Now()) {
		return nil, status.Error(codes.Unavailable, "UPLOAD_GRANT_EXPIRED")
	}
	headers := make(map[string]string, len(grant.Headers))
	for name, values := range grant.Headers {
		headers[name] = strings.Join(values, ",")
	}
	return &pb.CreateUploadResponse{UploadId: upload.UploadID, ObjectKey: upload.ObjectKey, UploadUrl: grant.URL, RequiredHeaders: headers, ExpiresUnixMs: grant.ExpiresAt.UnixMilli()}, nil
}

func uploadAllowed(result store.UploadResult) error {
	switch result.Decision {
	case "ACCEPTED":
		if result.Upload != nil {
			return nil
		}
	case "FENCED", "STOP_REQUESTED", "ALREADY_TERMINAL":
		return status.Error(codes.FailedPrecondition, "UPLOAD_"+result.Decision)
	}
	return status.Error(codes.Internal, "INVALID_UPLOAD_RESULT")
}

func uploadStorageError(err error) error {
	// SDK diagnostics can contain signed URLs. Expose stable reasons only.
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return rpcError(err)
	case errors.Is(err, objectstore.ErrVersioning):
		return status.Error(codes.FailedPrecondition, "OBJECT_VERSIONING_REQUIRED")
	case errors.Is(err, objectstore.ErrInvalid):
		return status.Error(codes.InvalidArgument, "OBJECT_STORAGE_LIMIT")
	default:
		return status.Error(codes.Unavailable, "OBJECT_STORAGE_UNAVAILABLE")
	}
}
