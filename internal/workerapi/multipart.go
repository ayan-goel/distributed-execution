package workerapi

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func multipartStorageUpload(u store.UploadRecord) objectstore.MultipartUpload {
	return objectstore.MultipartUpload{Key: u.ObjectKey, UploadID: u.BackendUploadID, Size: u.SizeBytes, PartSize: u.PartSizeBytes}
}

func (s *Service) createMultipart(ctx context.Context, id store.WorkerIdentity, r store.UploadRequest, result store.UploadResult) (*pb.CreateUploadResponse, error) {
	if result.Upload.BackendUploadID == "" {
		u := result.Upload
		backend, err := s.objects.BeginIdentifiedMultipart(ctx, u.ObjectKey, u.SizeBytes, u.PartSizeBytes, u.InitializationID)
		if err != nil {
			return nil, uploadStorageError(err)
		}
		bound, bindErr := store.BindMultipartUpload(ctx, s.pool, id, r, backend.UploadID)
		if bindErr != nil && !errors.Is(bindErr, store.ErrConflict) {
			// A lost commit reply can hide a successful binding. Preserve this
			// backend until replay or reconciliation establishes its ownership.
			return nil, rpcError(bindErr)
		}
		if bindErr != nil || bound.Decision != "ACCEPTED" {
			// Only the winning backend identity can receive capabilities. Cleanup
			// uses a separate bounded context so request cancellation cannot skip it.
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			abortErr := s.objects.AbortMultipart(cleanup, backend)
			cancel()
			if abortErr != nil {
				return nil, uploadStorageError(abortErr)
			}
			if bindErr == nil {
				return nil, uploadDecision(bound.Decision)
			}
		}
	}
	// Declaration replay rechecks current authority after storage initialization
	// and returns the winning binding without exposing the backend upload ID.
	current, err := store.CreateUpload(ctx, s.pool, id, r)
	if err != nil {
		return nil, rpcError(err)
	}
	if err := uploadAllowed(current); err != nil {
		return nil, err
	}
	u := current.Upload
	if u.BackendUploadID == "" {
		return nil, status.Error(codes.Unavailable, "MULTIPART_INITIALIZATION_PENDING")
	}
	return &pb.CreateUploadResponse{UploadId: u.UploadID, ObjectKey: u.ObjectKey, PartCount: uint32(u.PartCount), PartSizeBytes: uint64(u.PartSizeBytes)}, nil
}

func (s *Service) GrantUploadPart(ctx context.Context, r *pb.GrantUploadPartRequest) (*pb.UploadPart, error) {
	id, ok := Identity(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "UNAUTHORIZED_WORKER")
	}
	if err := bindWorker(r, id.WorkerID); err != nil {
		return nil, err
	}
	a := r.GetAuthority()
	if a.GetGeneration() > math.MaxInt64 || r.GetNumber() == 0 || r.GetNumber() > store.MaxUploadParts {
		return nil, rpcError(store.ErrInvalid)
	}
	if s.objects == nil {
		return nil, status.Error(codes.FailedPrecondition, "OBJECT_STORAGE_NOT_CONFIGURED")
	}
	request := store.MultipartPartRequest{Authority: store.AttemptAuthority{JobID: a.GetJobId(), AttemptID: a.GetAttemptId(), WorkerID: a.GetWorkerId(), SessionID: a.GetSessionId(), Generation: int64(a.GetGeneration())}, UploadID: r.GetUploadId(), Number: int(r.GetNumber()), SHA256: r.GetSha256()}
	initial, err := store.PrepareMultipartPart(ctx, s.pool, id, request)
	if err != nil {
		return nil, rpcError(err)
	}
	if err := uploadAllowed(initial); err != nil {
		return nil, err
	}
	grant, err := s.objects.PresignPart(ctx, multipartStorageUpload(*initial.Upload), int32(request.Number), request.SHA256, 30*time.Second)
	if err != nil {
		return nil, uploadStorageError(err)
	}
	current, err := store.PrepareMultipartPart(ctx, s.pool, id, request)
	if err != nil {
		return nil, rpcError(err)
	}
	if err := uploadAllowed(current); err != nil {
		return nil, err
	}
	if *current.Upload != *initial.Upload || grant.Method != "PUT" {
		return nil, status.Error(codes.Internal, "INVALID_UPLOAD_RESULT")
	}
	if err := ctx.Err(); err != nil {
		return nil, rpcError(err)
	}
	if !grant.ExpiresAt.After(time.Now()) {
		return nil, status.Error(codes.Unavailable, "UPLOAD_GRANT_EXPIRED")
	}
	headers := make(map[string]string, len(grant.Headers))
	for k, v := range grant.Headers {
		headers[k] = strings.Join(v, ",")
	}
	return &pb.UploadPart{Number: r.GetNumber(), Url: grant.URL, RequiredHeaders: headers, ExpiresUnixMs: grant.ExpiresAt.UnixMilli()}, nil
}

func (s *Service) completeMultipart(ctx context.Context, id store.WorkerIdentity, r *pb.FinalizeUploadRequest, finalize store.FinalizeUploadRequest) (string, error) {
	request := store.MultipartCompletionRequest{Authority: finalize.Authority, RequestID: finalize.RequestID, UploadID: finalize.UploadID, Object: &finalize.Object}
	for _, part := range r.GetParts() {
		if part == nil || part.GetNumber() > store.MaxUploadParts {
			return "", rpcError(store.ErrInvalid)
		}
		request.Parts = append(request.Parts, store.MultipartCompletionPart{Number: int(part.GetNumber()), ETag: part.GetEtag(), SHA256: part.GetSha256()})
	}
	result, err := store.CompleteMultipartUpload(ctx, s.pool, id, request, func(ctx context.Context, u store.UploadRecord, parts []store.MultipartCompletionPart) (string, error) {
		storageParts := make([]objectstore.CompletedPart, len(parts))
		for i, p := range parts {
			storageParts[i] = objectstore.CompletedPart{Number: int32(p.Number), ETag: p.ETag, SHA256: p.SHA256}
		}
		backend := multipartStorageUpload(u)
		version, err := s.objects.CompleteMultipart(ctx, backend, storageParts)
		if errors.Is(err, objectstore.ErrMultipartGone) || errors.Is(err, objectstore.ErrUnavailable) {
			// Concurrent prepared requests or a lost reply can remove the backend
			// upload. Recover only the identified exact version; never use latest.
			return s.objects.RecoverMultipartVersion(ctx, backend, u.InitializationID)
		}
		return version, err
	})
	if err != nil {
		switch {
		case errors.Is(err, objectstore.ErrIntegrity):
			return "", status.Error(codes.FailedPrecondition, "OBJECT_INTEGRITY_MISMATCH")
		case errors.Is(err, objectstore.ErrInvalid), errors.Is(err, objectstore.ErrUnavailable), errors.Is(err, objectstore.ErrVersioning), errors.Is(err, objectstore.ErrMultipartGone):
			return "", uploadStorageError(err)
		default:
			return "", rpcError(err)
		}
	}
	if err := uploadDecision(result.Decision); err != nil {
		return "", err
	}
	if result.Version == "" {
		return "", status.Error(codes.Internal, "INVALID_MULTIPART_RESULT")
	}
	return result.Version, nil
}
