package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestFinalizeUploadIdentityBindsExactObject(t *testing.T) {
	upload := uploadRequest()
	r := FinalizeUploadRequest{Authority: upload.Authority, RequestID: uuid.NewString(), UploadID: uuid.NewString(), Object: ArtifactObject{Key: "projects/p/jobs/j/attempts/a/uploads/u", Version: "opaque+/=version", SizeBytes: upload.SizeBytes, SHA256: upload.SHA256}}
	first, err := r.hash(r.Authority.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	replay := r
	replay.RequestID = uuid.NewString()
	if hash, err := replay.hash(r.Authority.WorkerID); err != nil || hash != first {
		t.Fatal("request ID changed payload digest", err)
	}
	for _, change := range []func(*FinalizeUploadRequest){
		func(r *FinalizeUploadRequest) { r.UploadID = "bad" },
		func(r *FinalizeUploadRequest) { r.RequestID = uuid.Nil.String() },
		func(r *FinalizeUploadRequest) { r.Object.Key = "../shared" },
		func(r *FinalizeUploadRequest) { r.Object.Version = "null" },
		func(r *FinalizeUploadRequest) { r.Object.Version = "" },
		func(r *FinalizeUploadRequest) { r.Object.Version = "bad\nversion" },
		func(r *FinalizeUploadRequest) { r.Object.Version = strings.Repeat("x", 1025) },
		func(r *FinalizeUploadRequest) { r.Object.SizeBytes = -1 },
		func(r *FinalizeUploadRequest) { r.Object.SizeBytes = MaxUploadBytes + 1 },
		func(r *FinalizeUploadRequest) { r.Object.SHA256 = "bad" },
		func(r *FinalizeUploadRequest) { r.Authority.Generation = 0 },
	} {
		bad := r
		change(&bad)
		if _, err := bad.hash(r.Authority.WorkerID); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid finalization accepted", err)
		}
	}
	for _, change := range []func(*FinalizeUploadRequest){
		func(r *FinalizeUploadRequest) { r.Object.Version = "different" },
		func(r *FinalizeUploadRequest) { r.Object.Key += "other" },
		func(r *FinalizeUploadRequest) { r.Object.SizeBytes++ },
		func(r *FinalizeUploadRequest) { r.UploadID = uuid.NewString() },
	} {
		changed := r
		change(&changed)
		if hash, err := changed.hash(r.Authority.WorkerID); err != nil || hash == first {
			t.Fatal("changed object retained digest", err)
		}
	}
}
