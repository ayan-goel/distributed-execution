package workerapi

import (
	"context"
	"math"
	"testing"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateUploadRejectsMissingIdentityAndInvalidWireFields(t *testing.T) {
	s := NewService(nil, store.AcquisitionPolicy{}, nil)
	if _, err := s.CreateUpload(context.Background(), nil); status.Code(err) != codes.Unauthenticated {
		t.Fatal("upload bypassed transport identity", err)
	}
	id := store.WorkerIdentity{WorkerID: uuid.NewString(), CredentialID: uuid.NewString()}
	ctx := context.WithValue(context.Background(), identityKey{}, id)
	for _, change := range []func(*pb.CreateUploadRequest){
		func(r *pb.CreateUploadRequest) { r.Authority = nil },
		func(r *pb.CreateUploadRequest) { r.Authority.Generation = math.MaxUint64 },
		func(r *pb.CreateUploadRequest) { r.SizeBytes = math.MaxUint64 },
		func(r *pb.CreateUploadRequest) { r.Kind = pb.ArtifactKind(999) },
		func(r *pb.CreateUploadRequest) { r.Kind = pb.ArtifactKind_ARTIFACT_KIND_UNSPECIFIED },
		func(r *pb.CreateUploadRequest) { r.PartCount = 2 },
	} {
		r := &pb.CreateUploadRequest{Authority: &pb.AttemptAuthority{WorkerId: id.WorkerID, Generation: 1}, Kind: pb.ArtifactKind_OUTPUT, PartCount: 1}
		change(r)
		if _, err := s.CreateUpload(ctx, r); status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid upload wire fields accepted", err)
		}
	}
	r := &pb.CreateUploadRequest{Authority: &pb.AttemptAuthority{WorkerId: uuid.NewString()}}
	if _, err := s.CreateUpload(ctx, r); status.Code(err) != codes.PermissionDenied {
		t.Fatal("cross-worker upload accepted", err)
	}
	r = &pb.CreateUploadRequest{Authority: &pb.AttemptAuthority{WorkerId: id.WorkerID, Generation: 1}, Kind: pb.ArtifactKind_OUTPUT, PartCount: 1}
	if _, err := s.CreateUpload(ctx, r); status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "OBJECT_STORAGE_NOT_CONFIGURED" {
		t.Fatal("missing object store was not explicit", err)
	}
}
