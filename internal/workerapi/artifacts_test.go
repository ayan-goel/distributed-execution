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

func TestFinalizeUploadRequiresIdentityAndBoundedSinglePartFields(t *testing.T) {
	s := NewService(nil, store.AcquisitionPolicy{}, nil)
	if _, err := s.FinalizeUpload(context.Background(), nil); status.Code(err) != codes.Unauthenticated {
		t.Fatal("finalization bypassed identity", err)
	}
	id := store.WorkerIdentity{WorkerID: uuid.NewString(), CredentialID: uuid.NewString()}
	ctx := context.WithValue(context.Background(), identityKey{}, id)
	for _, change := range []func(*pb.FinalizeUploadRequest){
		func(r *pb.FinalizeUploadRequest) { r.Authority = nil },
		func(r *pb.FinalizeUploadRequest) { r.Authority.Generation = math.MaxUint64 },
		func(r *pb.FinalizeUploadRequest) { r.Object = nil },
		func(r *pb.FinalizeUploadRequest) { r.Object.SizeBytes = math.MaxUint64 },
		func(r *pb.FinalizeUploadRequest) { r.Parts = []*pb.CompletedPart{{Number: 1, Etag: "untrusted"}} },
	} {
		r := &pb.FinalizeUploadRequest{Authority: &pb.AttemptAuthority{WorkerId: id.WorkerID, Generation: 1}, Object: &pb.ObjectVersion{}}
		change(r)
		if _, err := s.FinalizeUpload(ctx, r); status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid finalization crossed boundary", err)
		}
	}
	r := &pb.FinalizeUploadRequest{Authority: &pb.AttemptAuthority{WorkerId: uuid.NewString(), Generation: 1}, Object: &pb.ObjectVersion{}}
	if _, err := s.FinalizeUpload(ctx, r); status.Code(err) != codes.PermissionDenied {
		t.Fatal("cross-worker finalization accepted", err)
	}
	r.Authority.WorkerId = id.WorkerID
	if _, err := s.FinalizeUpload(ctx, r); status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "OBJECT_STORAGE_NOT_CONFIGURED" {
		t.Fatal("missing storage not explicit", err)
	}
}
