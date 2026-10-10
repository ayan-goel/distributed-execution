package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestMultipartCompletionHashBindsOrderedBoundedPartEvidence(t *testing.T) {
	u := uploadRequest()
	r := MultipartCompletionRequest{Authority: u.Authority, RequestID: uuid.NewString(), UploadID: uuid.NewString(), Parts: []MultipartCompletionPart{{Number: 1, ETag: "first", SHA256: u.SHA256}, {Number: 2, ETag: "second", SHA256: u.SHA256}}}
	before, err := r.hash(r.Authority.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	changed := r
	changed.Parts = append([]MultipartCompletionPart(nil), r.Parts...)
	changed.Parts[1].ETag = "other"
	if after, err := changed.hash(r.Authority.WorkerID); err != nil || before == after {
		t.Fatal("changed evidence kept replay identity", err)
	}
	for _, parts := range [][]MultipartCompletionPart{nil, r.Parts[:1], {r.Parts[1], r.Parts[0]}, {{Number: 1, ETag: "bad\nvalue", SHA256: u.SHA256}, r.Parts[1]}, {{Number: 1, ETag: strings.Repeat("a", 1025), SHA256: u.SHA256}, r.Parts[1]}, {{Number: 1, ETag: "first", SHA256: "not-a-hash"}, r.Parts[1]}} {
		changed := r
		changed.Parts = parts
		if _, err := changed.hash(r.Authority.WorkerID); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid part evidence accepted", err)
		}
	}
}
