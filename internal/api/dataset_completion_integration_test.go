//go:build integration

package api

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/client"
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
	requestID := uuid.NewString()
	declaration, _ := json.Marshal(map[string]any{"requestId": requestID, "name": "input-v1",
		"sizeBytes": archive.Len(), "sha256": digest(archive.Bytes())})
	w := call(h, http.MethodPost, "/v1/datasets/uploads", submitter, "", declaration)
	var session DatasetUploadSession
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &session) != nil || session.UploadID == "" {
		t.Fatal("upload session unavailable", w.Code, w.Body.String())
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
	corrupt, err := admin.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String(session.ObjectKey), Body: bytes.NewReader([]byte("tampered archive"))})
	if err != nil {
		t.Fatal(err)
	}
	corruptVersion := aws.ToString(corrupt.VersionId)
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
	server := httptest.NewServer(h)
	defer server.Close()
	c, err := client.New(server.URL, submitter, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	clientSession, err := c.CreateDatasetUpload(ctx, requestID, "input-v1", int64(archive.Len()), digest(archive.Bytes()))
	if err != nil || clientSession.UploadID != session.UploadID || !clientSession.Replayed {
		t.Fatal("client could not recover the upload declaration", err)
	}
	goodVersion, err := c.UploadDatasetBytes(ctx, clientSession, archive.Bytes())
	if err != nil {
		t.Fatal("client could not transfer signed dataset bytes", err)
	}
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
	cliRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(cliRoot, "sample.txt"), []byte("CLI dataset"), 0600); err != nil {
		t.Fatal(err)
	}
	getenv := func(name string) string {
		switch name {
		case "DISPATCH_URL":
			return server.URL
		case "DISPATCH_TOKEN":
			return submitter
		case "DISPATCH_DEV_INSECURE":
			return "1"
		}
		return ""
	}
	var cliOutput, cliErrors bytes.Buffer
	code := cli.Run(ctx, []string{"dataset", "upload", cliRoot, "--name", "cli-v1", "--json"}, getenv, &cliOutput, &cliErrors)
	var cliDataset client.DatasetRegistration
	if code != 0 || json.Unmarshal(cliOutput.Bytes(), &cliDataset) != nil || cliDataset.DatasetID == "" ||
		cliDataset.Name != "cli-v1" || cliDataset.ObjectVersion == "" {
		t.Fatal("real CLI dataset workflow failed", code, cliOutput.String(), cliErrors.String())
	}
}
