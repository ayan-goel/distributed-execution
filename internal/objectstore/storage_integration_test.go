//go:build integration

package objectstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestRealVersionedStoragePreservesVerifiedVersionsAndRejectsTampering(t *testing.T) {
	endpoint := os.Getenv("DISPATCH_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires scripts/test-objectstore.sh")
	}
	cfg := config(endpoint)
	cfg.AccessKey, cfg.SecretKey = os.Getenv("DISPATCH_TEST_S3_ACCESS_KEY"), os.Getenv("DISPATCH_TEST_S3_SECRET_KEY")
	store, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ready := time.Now().Add(15 * time.Second)
	for {
		_, err := store.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(cfg.Bucket)})
		if err == nil {
			break
		}
		if time.Now().After(ready) {
			t.Fatal("isolated storage fixture did not become ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := store.CheckVersioning(ctx); !errors.Is(err, ErrVersioning) {
		t.Fatal("unversioned bucket was accepted", err)
	}
	if _, err := store.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(cfg.Bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckVersioning(ctx); err != nil {
		t.Fatal(err)
	}

	key, body := "projects/p/jobs/j/attempts/a/uploads/u", "original result"
	put := func(grant Grant, body string) (int, string) {
		t.Helper()
		r, err := http.NewRequestWithContext(ctx, grant.Method, grant.URL, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Header = grant.Headers.Clone()
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal("scoped transfer failed")
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 65536))
		return response.StatusCode, response.Header.Get("X-Amz-Version-Id")
	}
	for _, boundary := range []struct{ key, body string }{
		{"outputs/empty", ""},
		{"outputs/limit", strings.Repeat("x", int(cfg.MaxObjectBytes))},
	} {
		grant, err := store.PresignUpload(ctx, boundary.key, int64(len(boundary.body)), checksum(boundary.body), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		status, version := put(grant, boundary.body)
		if status != http.StatusOK {
			t.Fatal("boundary upload failed", boundary.key, status)
		}
		if err := store.Verify(ctx, Object{Key: boundary.key, Version: version, Size: int64(len(boundary.body)), SHA256: checksum(boundary.body)}); err != nil {
			t.Fatal("boundary verification failed", err)
		}
	}
	grant, err := store.PresignUpload(ctx, key, int64(len(body)), checksum(body), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	status, v1 := put(grant, body)
	if status != http.StatusOK || v1 == "" || v1 == "null" {
		t.Fatal("versioned upload failed", status)
	}
	original := Object{Key: key, Version: v1, Size: int64(len(body)), SHA256: checksum(body)}
	if err := store.Verify(ctx, original); err != nil {
		t.Fatal(err)
	}

	status, replayVersion := put(grant, body)
	if status != http.StatusOK || replayVersion == v1 || replayVersion == "" || replayVersion == "null" {
		t.Fatal("reused grant did not create a separate version", status)
	}
	status, _ = put(grant, "tampered result")
	if status != http.StatusBadRequest {
		t.Fatal("checksum tampering accepted", status)
	}
	status, _ = put(grant, "x")
	if status < 400 {
		t.Fatal("signed byte count changed", status)
	}
	changed := grant
	changedURL, _ := url.Parse(grant.URL)
	changedURL.Path += "-other-key"
	changed.URL = changedURL.String()
	status, _ = put(changed, body)
	if status < 400 {
		t.Fatal("signature tampering accepted")
	}
	short, err := store.PresignUpload(ctx, key, int64(len(body)), checksum(body), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	status, _ = put(short, body)
	if status < 400 {
		t.Fatal("expired upload capability accepted", status)
	}

	replacement := "replacement bytes"
	newGrant, err := store.PresignUpload(ctx, key, int64(len(replacement)), checksum(replacement), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	status, v2 := put(newGrant, replacement)
	if status != http.StatusOK || v2 == v1 {
		t.Fatal("replacement did not receive a new version")
	}
	if err := store.Verify(ctx, original); err != nil {
		t.Fatal("overwrite changed original version", err)
	}
	download, err := store.PresignDownload(ctx, original, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Get(download.URL)
	if err != nil {
		t.Fatal("scoped download failed")
	}
	downloaded, err := io.ReadAll(io.LimitReader(response.Body, 1025))
	_ = response.Body.Close()
	if err != nil || response.StatusCode != 200 || string(downloaded) != body {
		t.Fatal("download did not pin verified bytes", err)
	}

	wrong := original
	wrong.Version = v2
	if err := store.Verify(ctx, wrong); !errors.Is(err, ErrIntegrity) {
		t.Fatal("wrong version passed content verification", err)
	}
	if _, err := store.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String(key), VersionId: aws.String(v1)}); err != nil {
		t.Fatal(err)
	}
	if err := store.Verify(ctx, original); err == nil {
		t.Fatal("deleted version fell back to latest")
	}
	if _, err := store.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(cfg.Bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PresignUpload(ctx, key, 3, checksum("abc"), time.Minute); !errors.Is(err, ErrVersioning) {
		t.Fatal("suspended versioning issued a grant", err)
	}

	// A backend checksum is additional evidence. Verification still reads and
	// hashes exact-version bytes rather than trusting ETag or client metadata.
	hash, _ := hex.DecodeString(checksum(body))
	if grant.Headers.Get("X-Amz-Checksum-Sha256") != "" && grant.Headers.Get("X-Amz-Checksum-Sha256") != base64.StdEncoding.EncodeToString(hash) {
		t.Fatal("wrong signed checksum")
	}
}
