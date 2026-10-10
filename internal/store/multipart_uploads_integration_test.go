//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestMultipartDeclarationAndBackendIdentitySurviveConcurrentReplay(t *testing.T) {
	pool, id, r := uploadFixture(t, MaxMultipartUploadBytes)
	r.SizeBytes = MaxUploadBytes + 3
	r.PartSizeBytes = 8 << 20
	r.PartCount = 9
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan UploadResult, 16)
	failures := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := CreateUpload(ctx, pool, id, r)
			results <- result
			failures <- err
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first *UploadRecord
	for result := range results {
		if result.Decision != "ACCEPTED" || result.Upload == nil || !canonicalUUID(result.Upload.InitializationID) || result.Upload.PartSizeBytes != r.PartSizeBytes {
			t.Fatal("invalid durable plan", result)
		}
		if first == nil {
			first = result.Upload
		} else if *first != *result.Upload {
			t.Fatal("replay changed upload identity")
		}
	}
	type binding struct {
		result UploadResult
		err    error
	}
	bound := make(chan binding, 16)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := BindMultipartUpload(ctx, pool, id, r, fmt.Sprintf("backend-%d", i))
			bound <- binding{result, err}
		}()
	}
	wg.Wait()
	close(bound)
	var winner *UploadRecord
	conflicts := 0
	for b := range bound {
		if errors.Is(b.err, ErrConflict) {
			conflicts++
			continue
		}
		if b.err != nil || b.result.Upload == nil {
			t.Fatal(b.err)
		}
		winner = b.result.Upload
	}
	if winner == nil || conflicts != 15 || winner.InitializationID != first.InitializationID || winner.UploadID != first.UploadID {
		t.Fatal("backend initialization was rebound", conflicts)
	}
	replay, err := BindMultipartUpload(ctx, pool, id, r, winner.BackendUploadID)
	if err != nil || replay.Upload == nil || *replay.Upload != *winner {
		t.Fatal("binding replay changed persisted identity", err)
	}
	reloaded, err := CreateUpload(ctx, pool, id, r)
	if err != nil || reloaded.Upload == nil || *reloaded.Upload != *winner {
		t.Fatal("declaration replay lost backend binding", err)
	}
	var uploads, bindings, events int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifact_uploads),(SELECT count(*) FROM artifact_multipart_uploads),
		(SELECT count(*) FROM job_events WHERE type='MULTIPART_INITIALIZED')`).Scan(&uploads, &bindings, &events); err != nil || uploads != 1 || bindings != 1 || events != 1 {
		t.Fatal("replay duplicated durable state", uploads, bindings, events, err)
	}
	changed := r
	changed.PartSizeBytes = 16 << 20
	changed.PartCount = 5
	if _, err := CreateUpload(ctx, pool, id, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("changed plan did not conflict", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE artifact_multipart_uploads SET backend_upload_id='replacement' WHERE upload_id=$1", winner.UploadID); err == nil {
		t.Fatal("SQL rebound backend upload")
	}
	if _, err := pool.Exec(ctx, "UPDATE artifact_multipart_uploads SET initialization_id=$2 WHERE upload_id=$1", winner.UploadID, uuid.NewString()); err == nil {
		t.Fatal("SQL rebound initialization identity")
	}
}

func TestMultipartBindingCannotCommitAfterAuthorityExpires(t *testing.T) {
	pool, id, r := uploadFixture(t)
	r.SizeBytes = (5 << 20) + 3
	r.PartSizeBytes = 5 << 20
	r.PartCount = 2
	created, err := CreateUpload(context.Background(), pool, id, r)
	if err != nil || created.Upload == nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", r.Authority.AttemptID); err != nil {
		t.Fatal(err)
	}
	result, err := BindMultipartUpload(context.Background(), pool, id, r, "stale-backend")
	if err != nil || result.Decision != "FENCED" || result.Upload != nil {
		t.Fatal("expired owner initialized upload", result, err)
	}
	var backend *string
	if err := pool.QueryRow(context.Background(), "SELECT backend_upload_id FROM artifact_multipart_uploads WHERE upload_id=$1", created.Upload.UploadID).Scan(&backend); err != nil || backend != nil {
		t.Fatal("fenced binding changed durable state", err)
	}
}
