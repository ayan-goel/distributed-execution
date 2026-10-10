//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestMultipartPartDeclarationsBindContentAcrossConcurrentRetry(t *testing.T) {
	pool, id, r := uploadFixture(t)
	ctx := context.Background()
	r.SizeBytes = (5 << 20) + 3
	r.PartSizeBytes = 5 << 20
	r.PartCount = 2
	bound, err := BindMultipartUpload(ctx, pool, id, r, "backend")
	if err != nil {
		t.Fatal(err)
	}
	request := MultipartPartRequest{Authority: r.Authority, UploadID: bound.Upload.UploadID, Number: 1, SHA256: r.SHA256}
	var wg sync.WaitGroup
	failures := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := PrepareMultipartPart(ctx, pool, id, request)
			if err == nil && (result.Decision != "ACCEPTED" || result.Upload == nil || result.Upload.UploadID != request.UploadID) {
				err = errors.New("changed part grant identity")
			}
			failures <- err
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	changed := request
	changed.SHA256 = strings.Repeat("b", 64)
	if _, err := PrepareMultipartPart(ctx, pool, id, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("part checksum was rebound", err)
	}
	completion := MultipartCompletionRequest{Authority: r.Authority, RequestID: uuid.NewString(), UploadID: request.UploadID, Parts: []MultipartCompletionPart{{Number: 1, ETag: "one", SHA256: r.SHA256}, {Number: 2, ETag: "two", SHA256: r.SHA256}}}
	if _, err := CompleteMultipartUpload(ctx, pool, id, completion, func(context.Context, UploadRecord, []MultipartCompletionPart) (string, error) {
		t.Fatal("ungranted part reached storage completion")
		return "", nil
	}); !errors.Is(err, ErrConflict) {
		t.Fatal("completion accepted unbound parts", err)
	}
	request.Number = 2
	if _, err := PrepareMultipartPart(ctx, pool, id, request); err != nil {
		t.Fatal(err)
	}
	completion.Parts[1].SHA256 = strings.Repeat("b", 64)
	if _, err := CompleteMultipartUpload(ctx, pool, id, completion, func(context.Context, UploadRecord, []MultipartCompletionPart) (string, error) {
		t.Fatal("changed part evidence reached storage")
		return "", nil
	}); !errors.Is(err, ErrConflict) {
		t.Fatal("completion replaced granted content", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE artifact_upload_parts SET sha256=$2 WHERE upload_id=$1", request.UploadID, strings.Repeat("b", 64)); err == nil {
		t.Fatal("SQL replaced part evidence")
	}
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", r.Authority.AttemptID); err != nil {
		t.Fatal(err)
	}
	result, err := PrepareMultipartPart(ctx, pool, id, request)
	if err != nil || result.Decision != "FENCED" || result.Upload != nil {
		t.Fatal("expired part replay acquired authority", result, err)
	}
}
