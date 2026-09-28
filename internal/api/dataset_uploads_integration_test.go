//go:build integration

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

type datasetUploadSigner func(context.Context, string, int64, string, time.Duration) (objectstore.Grant, error)

func (f datasetUploadSigner) PresignUpload(ctx context.Context, key string, size int64, hash string, ttl time.Duration) (objectstore.Grant, error) {
	return f(ctx, key, size, hash, ttl)
}
func (datasetUploadSigner) PresignDownload(context.Context, objectstore.Object, time.Duration) (objectstore.Grant, error) {
	return objectstore.Grant{}, objectstore.ErrInvalid
}

func TestHTTPDatasetUploadSessionIsScopedAndReplayable(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
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
	var project string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM projects WHERE name='research'").Scan(&project); err != nil {
		t.Fatal(err)
	}
	var grants int
	var failSigning bool
	var revokeTokenID string
	signer := datasetUploadSigner(func(_ context.Context, key string, size int64, hash string, ttl time.Duration) (objectstore.Grant, error) {
		if !strings.HasPrefix(key, "projects/") || !strings.Contains(key, "/datasets/uploads/") || size != 3 || hash != strings.Repeat("a", 64) || ttl != time.Minute {
			t.Fatal("signer received an unscoped declaration", key, size, hash, ttl)
		}
		grants++
		if failSigning {
			return objectstore.Grant{}, errors.New("storage down")
		}
		if revokeTokenID != "" {
			if err := store.RevokeToken(ctx, pool, "research", revokeTokenID); err != nil {
				t.Fatal(err)
			}
			revokeTokenID = ""
		}
		return objectstore.Grant{URL: "https://storage.example.test/upload", Method: http.MethodPut,
			Headers: http.Header{"X-Test-Signed": {"yes"}}, ExpiresAt: time.Now().Add(ttl)}, nil
	})
	h := New(pool, nil, signer)
	request := map[string]any{"requestId": uuid.NewString(), "name": "antibodies-v1", "sizeBytes": 3, "sha256": strings.Repeat("a", 64)}
	body, _ := json.Marshal(request)
	path := "/v1/datasets/uploads"
	if w := call(h, http.MethodPost, path, reader, "", body); w.Code != 403 || grants != 0 {
		t.Fatal("read token reserved a dataset upload", w.Code, grants)
	}
	w := call(h, http.MethodPost, path, submitter, "", body)
	var first DatasetUploadSession
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &first) != nil || first.UploadID == "" ||
		first.ObjectKey != "projects/"+project+"/datasets/uploads/"+first.UploadID ||
		first.UploadURL == "" || first.Method != http.MethodPut || first.Replayed || grants != 1 {
		t.Fatal("scoped upload session was not created", w.Code, w.Body.String(), grants)
	}
	w = call(h, http.MethodPost, path, submitter, "", body)
	var replay DatasetUploadSession
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &replay) != nil || replay.UploadID != first.UploadID || !replay.Replayed || grants != 2 {
		t.Fatal("uncertain upload request did not replay", w.Code, w.Body.String(), grants)
	}
	request["sha256"] = strings.Repeat("b", 64)
	changed, _ := json.Marshal(request)
	if w := call(h, http.MethodPost, path, submitter, "", changed); w.Code != 409 || grants != 2 {
		t.Fatal("changed replay received a grant", w.Code, grants)
	}
	if w := call(h, http.MethodPost, path, foreign, "", body); w.Code != 201 || strings.Contains(w.Body.String(), first.ObjectKey) {
		t.Fatal("foreign project reused a dataset key", w.Code, w.Body.String())
	}
	failSigning = true
	request["sha256"] = strings.Repeat("a", 64)
	request["requestId"] = uuid.NewString()
	retryBody, _ := json.Marshal(request)
	if w := call(h, http.MethodPost, path, submitter, "", retryBody); w.Code != 503 || strings.Contains(w.Body.String(), "storage down") {
		t.Fatal("storage failure leaked or lost retry status", w.Code, w.Body.String())
	}
	failSigning = false
	w = call(h, http.MethodPost, path, submitter, "", retryBody)
	var afterFailure DatasetUploadSession
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &afterFailure) != nil || !afterFailure.Replayed ||
		!strings.HasPrefix(afterFailure.ObjectKey, "projects/"+project+"/datasets/uploads/") {
		t.Fatal("failed signing did not preserve replayable declaration", w.Code, w.Body.String())
	}
	for _, invalid := range [][]byte{
		[]byte(`{"requestId":"bad","name":"antibodies-v1","sizeBytes":3,"sha256":"` + strings.Repeat("a", 64) + `"}`),
		[]byte(`{"requestId":"` + uuid.NewString() + `","name":"antibodies-v1","sizeBytes":3,"sha256":"` + strings.Repeat("a", 64) + `","key":"caller-choice"}`),
		[]byte(strings.Repeat("x", 4097)),
	} {
		if w := call(h, http.MethodPost, path, submitter, "", invalid); w.Code != 422 && w.Code != 413 {
			t.Fatal("invalid upload declaration accepted", w.Code, w.Body.String())
		}
	}
	revoked, tokenID, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	revokeTokenID = tokenID
	request["requestId"] = uuid.NewString()
	revokedBody, _ := json.Marshal(request)
	if w := call(h, http.MethodPost, path, revoked, "", revokedBody); w.Code != 401 || strings.Contains(w.Body.String(), "storage.example.test") {
		t.Fatal("revoked token received an upload capability", w.Code, w.Body.String())
	}
}
