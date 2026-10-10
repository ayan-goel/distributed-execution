//go:build integration

package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
)

func TestRealMultipartStorageVersionsIntegrityAndScopedAbort(t *testing.T) {
	endpoint := os.Getenv("DISPATCH_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires scripts/test-objectstore.sh")
	}
	cfg := config(endpoint)
	cfg.MaxObjectBytes = 16 << 20
	cfg.AccessKey, cfg.SecretKey = os.Getenv("DISPATCH_TEST_S3_ACCESS_KEY"), os.Getenv("DISPATCH_TEST_S3_SECRET_KEY")
	store, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ready := time.Now().Add(15 * time.Second)
	for {
		if _, err := store.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(cfg.Bucket)}); err == nil {
			break
		}
		if time.Now().After(ready) {
			t.Fatal("isolated backend not ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// This test owns a fresh bucket; versioning changes never affect other tests.
	cfg.Bucket = "dispatch-multipart-" + uuid.NewString()
	if _, err := store.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)}); err != nil {
		t.Fatal("create isolated multipart bucket", err)
	}
	store, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(cfg.Bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatal(err)
	}
	put := func(grant Grant, body []byte) (int, string) {
		t.Helper()
		r, err := http.NewRequestWithContext(ctx, grant.Method, grant.URL, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Header = grant.Headers.Clone()
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal("scoped part transfer failed")
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 65536))
		return response.StatusCode, response.Header.Get("ETag")
	}
	hash := func(body []byte) string { sum := sha256.Sum256(body); return hex.EncodeToString(sum[:]) }
	key := "outputs/multipart-result"
	body := append(bytes.Repeat([]byte("a"), int(MinMultipartPartBytes)), []byte("last part")...)
	uploadBody := func(body []byte) Object {
		t.Helper()
		upload, err := store.BeginMultipart(ctx, key, int64(len(body)), MinMultipartPartBytes)
		if err != nil {
			t.Fatal("begin multipart", err)
		}
		t.Cleanup(func() {
			if err := store.AbortMultipart(context.Background(), upload); err != nil {
				t.Error("scoped upload cleanup", err)
			}
		})
		var parts []CompletedPart
		for start, number := int64(0), int32(1); start < int64(len(body)); start, number = start+upload.PartSize, number+1 {
			part := body[start:min(start+upload.PartSize, int64(len(body)))]
			grant, err := store.PresignPart(ctx, upload, number, hash(part), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if number == 2 {
				tampered := bytes.Clone(part)
				tampered[0] ^= 1
				if status, _ := put(grant, tampered); status != 400 {
					t.Fatal("part checksum tampering was not rejected", status)
				}
			}
			status, etag := put(grant, part)
			if status != 200 || etag == "" {
				t.Fatalf("multipart part %d failed: %d", number, status)
			}
			parts = append(parts, CompletedPart{Number: number, ETag: etag, SHA256: hash(part)})
		}
		version, err := store.CompleteMultipart(ctx, upload, parts)
		if err != nil {
			t.Fatal("complete multipart", err)
		}
		object := Object{Key: key, Version: version, Size: int64(len(body)), SHA256: hash(body)}
		if err := store.Verify(ctx, object); err != nil {
			t.Fatal("verify full multipart content", err)
		}
		return object
	}
	original := uploadBody(body)
	body[0] = 'b'
	replacement := uploadBody(body)
	if original.Version == replacement.Version {
		t.Fatal("multipart overwrite reused version")
	}
	if err := store.Verify(ctx, original); err != nil {
		t.Fatal("multipart overwrite changed original", err)
	}
	wrong := original
	wrong.SHA256 = replacement.SHA256
	if err := store.Verify(ctx, wrong); !errors.Is(err, ErrIntegrity) {
		t.Fatal("multipart ETag bypassed SHA-256 verification", err)
	}
	abandoned, err := store.BeginMultipart(ctx, key, int64(len(body)), MinMultipartPartBytes)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := store.PresignPart(ctx, abandoned, 1, hash(body[:MinMultipartPartBytes]), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := put(grant, body[:MinMultipartPartBytes]); status != 200 {
		t.Fatal("abandoned part upload failed", status)
	}
	cfg.MaxObjectBytes = 1024
	reducedStore, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := reducedStore.AbortMultipart(ctx, abandoned); err != nil {
			t.Fatal("replayed scoped abort", err)
		}
	}
	if status, _ := put(grant, body[:MinMultipartPartBytes]); status != 404 {
		t.Fatal("aborted part capability still accepted", status)
	}
	if err := store.Verify(ctx, original); err != nil {
		t.Fatal("abort deleted completed version", err)
	}
	t.Log("two multipart versions independently verified; repeated abort removed only its upload")
}
