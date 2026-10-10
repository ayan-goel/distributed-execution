package store

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestMultipartUploadPlanIsBoundedAndPartOfReplayIdentity(t *testing.T) {
	r := uploadRequest()
	r.SizeBytes = MaxUploadBytes + 3
	r.PartSizeBytes = 8 << 20
	r.PartCount = 9
	first, err := r.hash(r.Authority.WorkerID)
	if err != nil {
		t.Fatal("valid multipart plan rejected", err)
	}
	changed := r
	changed.PartSizeBytes = 16 << 20
	changed.PartCount = 5
	if hash, err := changed.hash(r.Authority.WorkerID); err != nil || hash == first {
		t.Fatal("part plan not bound to request", err)
	}
	for _, mutate := range []func(*UploadRequest){func(r *UploadRequest) { r.PartCount++ }, func(r *UploadRequest) { r.PartSizeBytes = 0 }, func(r *UploadRequest) { r.PartSizeBytes = (5 << 20) - 1 }, func(r *UploadRequest) { r.PartSizeBytes = MaxUploadBytes + 1 }, func(r *UploadRequest) { r.SizeBytes = MaxMultipartUploadBytes + 1 }, func(r *UploadRequest) { r.PartCount = 10001 }, func(r *UploadRequest) { r.Kind = "LOG" }, func(r *UploadRequest) { r.Kind = "MANIFEST" }} {
		bad := r
		mutate(&bad)
		if _, err := bad.hash(r.Authority.WorkerID); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid multipart plan accepted", err)
		}
	}
	single := uploadRequest()
	body, err := json.Marshal(single)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "partSizeBytes") {
		t.Fatal("single-part canonical payload changed")
	}
	single.PartSizeBytes = 5 << 20
	if _, err := single.hash(single.Authority.WorkerID); !errors.Is(err, ErrInvalid) {
		t.Fatal("single-part upload accepted multipart plan")
	}
}
