//go:build integration

package api

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
)

func TestHTTPDatasetCompletionVerifiesRealObjectVersion(t *testing.T) {
	endpoint := os.Getenv("DISPATCH_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires scripts/test-objectstore.sh")
	}
	pool := apiPool(t)
	ctx := context.Background()
	cfg := objectstore.Config{Endpoint: endpoint, Region: "us-east-1", Bucket: "dispatch-test",
		AccessKey: os.Getenv("DISPATCH_TEST_S3_ACCESS_KEY"), SecretKey: os.Getenv("DISPATCH_TEST_S3_SECRET_KEY"), AllowLoopbackHTTP: true}
	objects, err := objectstore.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	admin := s3.New(s3.Options{BaseEndpoint: aws.String(endpoint), Region: cfg.Region, UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		Retryer:     aws.NopRetryer{}, HTTPClient: &http.Client{Timeout: 5 * time.Second}})
	ready, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		_, err = admin.PutBucketVersioning(ready, &s3.PutBucketVersioningInput{Bucket: aws.String(cfg.Bucket),
			VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
		if err == nil {
			break
		}
		if ready.Err() != nil {
			t.Fatal("isolated object store unavailable")
		}
		time.Sleep(100 * time.Millisecond)
	}
	submitter, _, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := store.IssueToken(ctx, pool, "research", store.RoleRead)
	if err != nil {
		t.Fatal(err)
	}
	foreign, _, err := store.IssueToken(ctx, pool, "other", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hello dataset\n")
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0644, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	digest := func(body []byte) string { sum := sha256.Sum256(body); return hex.EncodeToString(sum[:]) }
	h := New(pool, nil, objects)
	declaration, _ := json.Marshal(map[string]any{"requestId": uuid.NewString(), "name": "input-v1",
		"sizeBytes": archive.Len(), "sha256": digest(archive.Bytes())})
	w := call(h, http.MethodPost, "/v1/datasets/uploads", submitter, "", declaration)
	var session DatasetUploadSession
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &session) != nil || session.UploadID == "" {
		t.Fatal("upload session unavailable", w.Code, w.Body.String())
	}
	put := func(body []byte, signed bool) string {
		t.Helper()
		if signed {
			req, err := http.NewRequest(http.MethodPut, session.UploadURL, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header = session.RequiredHeaders.Clone()
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			if response.StatusCode != 200 {
				t.Fatal("signed upload failed", response.StatusCode)
			}
			return response.Header.Get("X-Amz-Version-Id")
		}
		result, err := admin.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String(session.ObjectKey), Body: bytes.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		return aws.ToString(result.VersionId)
	}
	manifest := store.DatasetManifest{Format: "tar.v1", Files: []store.DatasetFile{{Path: "input.txt", SizeBytes: int64(len(data)), SHA256: digest(data)}}}
	completePath := "/v1/datasets/uploads/" + session.UploadID + "/complete"
	complete := func(version, token string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]any{"version": version, "manifest": manifest})
		return call(h, http.MethodPost, completePath, token, "", body)
	}
	if w := complete("any-version", reader); w.Code != 403 {
		t.Fatal("read token completed a dataset", w.Code)
	}
	corruptVersion := put([]byte("tampered archive"), false)
	if corruptVersion == "" || corruptVersion == "null" {
		t.Fatal("fixture did not create immutable corrupt version")
	}
	if w := complete(corruptVersion, submitter); w.Code != 422 {
		t.Fatal("corrupt exact version was accepted", w.Code, w.Body.String())
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM datasets WHERE upload_id=$1", session.UploadID).Scan(&count); err != nil || count != 0 {
		t.Fatal("corrupt version left a dataset registration", count, err)
	}
	goodVersion := put(archive.Bytes(), true)
	if goodVersion == "" || goodVersion == "null" || goodVersion == corruptVersion {
		t.Fatal("signed upload did not create an immutable version")
	}
	w = complete(goodVersion, submitter)
	var registered DatasetRegistration
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &registered) != nil || registered.DatasetID == "" ||
		registered.Name != "input-v1" || registered.ObjectVersion != goodVersion || registered.Replayed {
		t.Fatal("verified registration failed", w.Code, w.Body.String())
	}
	w = complete(goodVersion, submitter)
	var replay DatasetRegistration
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &replay) != nil || replay.DatasetID != registered.DatasetID || !replay.Replayed {
		t.Fatal("completion replay changed registration", w.Code, w.Body.String())
	}
	if w := complete(corruptVersion, submitter); w.Code != 409 {
		t.Fatal("changed version replay was accepted", w.Code)
	}
	if w := complete(goodVersion, foreign); w.Code != 404 {
		t.Fatal("other project inspected dataset registration", w.Code)
	}
}
