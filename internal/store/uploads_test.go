package store

import (
	"errors"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

func uploadRequest() UploadRequest {
	return UploadRequest{Authority: AttemptAuthority{JobID: uuid.NewString(), AttemptID: uuid.NewString(), WorkerID: uuid.NewString(), SessionID: uuid.NewString(), Generation: 1}, RequestID: uuid.NewString(), Kind: "OUTPUT", LogicalName: "result", SizeBytes: 32, SHA256: strings.Repeat("a", 64), PartCount: 1}
}

func TestUploadRequestIdentityAndValidation(t *testing.T) {
	base := uploadRequest()
	first, err := base.hash(base.Authority.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	replay := base
	replay.RequestID = uuid.NewString()
	if hash, err := replay.hash(base.Authority.WorkerID); err != nil || hash != first {
		t.Fatal("request UUID changed payload identity", err)
	}
	for _, change := range []func(*UploadRequest){
		func(r *UploadRequest) { r.RequestID = "bad" }, func(r *UploadRequest) { r.Authority.JobID = uuid.Nil.String() },
		func(r *UploadRequest) { r.Authority.AttemptID = "bad" }, func(r *UploadRequest) { r.Authority.WorkerID = uuid.NewString() },
		func(r *UploadRequest) { r.Authority.SessionID = "bad" }, func(r *UploadRequest) { r.Authority.Generation = 0 },
		func(r *UploadRequest) { r.LogicalName = "../result" }, func(r *UploadRequest) { r.LogicalName = strings.Repeat("a", 129) },
		func(r *UploadRequest) { r.Kind = "DATASET" }, func(r *UploadRequest) { r.SizeBytes = -1 },
		func(r *UploadRequest) { r.SizeBytes = MaxUploadBytes + 1 }, func(r *UploadRequest) { r.SHA256 = "BAD" },
		func(r *UploadRequest) { r.PartCount = 0 }, func(r *UploadRequest) { r.PartCount = 2 },
	} {
		r := base
		change(&r)
		if _, err := r.hash(base.Authority.WorkerID); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid upload accepted", err)
		}
	}
	for _, change := range []func(*UploadRequest){
		func(r *UploadRequest) { r.Authority.Generation++ }, func(r *UploadRequest) { r.SizeBytes++ },
		func(r *UploadRequest) { r.SHA256 = strings.Repeat("b", 64) }, func(r *UploadRequest) { r.LogicalName = "other" },
		func(r *UploadRequest) { r.Kind = "MANIFEST" },
	} {
		r := base
		change(&r)
		if hash, err := r.hash(base.Authority.WorkerID); err != nil || hash == first {
			t.Fatal("changed payload retained digest", err)
		}
	}
}

func TestUploadDeclarationsRequireDeclaredOutputsAndAppropriatePhase(t *testing.T) {
	job := spec.Job{Spec: spec.JobSpec{Outputs: []spec.Output{{Name: "result", MaxBytes: 32}}}}
	r := uploadRequest()
	if err := validateUploadDeclaration(r, job, "FINALIZING"); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"ASSIGNED", "STARTING", "RUNNING"} {
		if err := validateUploadDeclaration(r, job, state); !errors.Is(err, ErrConflict) {
			t.Fatal("premature output upload", state, err)
		}
	}
	r.SizeBytes = 33
	if err := validateUploadDeclaration(r, job, "FINALIZING"); !errors.Is(err, ErrInvalid) {
		t.Fatal("declaration size exceeded", err)
	}
	r.SizeBytes = 0
	r.LogicalName = "unknown"
	if err := validateUploadDeclaration(r, job, "FINALIZING"); !errors.Is(err, ErrInvalid) {
		t.Fatal("undeclared output", err)
	}
	r.Kind = "LOG"
	r.LogicalName = "stdout"
	for _, state := range []string{"STARTING", "RUNNING", "FINALIZING"} {
		if err := validateUploadDeclaration(r, job, state); err != nil {
			t.Fatal(err)
		}
	}
	r.LogicalName = "arbitrary"
	if err := validateUploadDeclaration(r, job, "RUNNING"); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid log stream", err)
	}
	r.Kind = "MANIFEST"
	r.LogicalName = "result"
	if err := validateUploadDeclaration(r, job, "FINALIZING"); err != nil {
		t.Fatal(err)
	}
}
