package client

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDatasetUploadTransfersOnlyDeclaredBytesAndReturnsVersion(t *testing.T) {
	const uploadID = "00000000-0000-0000-0000-000000000011"
	const projectID = "00000000-0000-0000-0000-000000000013"
	const requestID = "00000000-0000-0000-0000-000000000014"
	key := "projects/" + projectID + "/datasets/uploads/" + uploadID
	content := []byte("archive bytes")
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])
	var transfers atomic.Int32
	object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		transfers.Add(1)
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1024))
		if r.Method != http.MethodPut || r.URL.Path != "/dispatch-test/"+key || string(body) != string(content) ||
			r.ContentLength != int64(len(content)) || r.Header.Get("Authorization") != "" ||
			r.Header.Get("Cookie") != "" || r.Header.Get("X-Amz-Checksum-Sha256") != base64.StdEncoding.EncodeToString(sum[:]) {
			t.Error("dataset transfer lost integrity or leaked project credentials")
		}
		w.Header().Set("X-Amz-Version-Id", "immutable-version")
		w.WriteHeader(200)
	}))
	defer object.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(DatasetUploadSession{UploadID: uploadID, ObjectKey: key,
			UploadURL: object.URL + "/dispatch-test/" + key + "?signature=secret",
			Method:    http.MethodPut, RequiredHeaders: http.Header{"X-Amz-Checksum-Sha256": {base64.StdEncoding.EncodeToString(sum[:])}},
			ExpiresAt: time.Now().Add(time.Minute)})
	}))
	defer api.Close()
	c, err := New(api.URL, "private-token", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	session, err := c.CreateDatasetUpload(context.Background(), requestID, "data-v1", int64(len(content)), digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.UploadDatasetBytes(context.Background(), session, []byte("archive bytex")); err == nil || transfers.Load() != 0 {
		t.Fatal("changed bytes reached object storage", err)
	}
	version, err := c.UploadDatasetBytes(context.Background(), session, content)
	if err != nil || version != "immutable-version" || transfers.Load() != 1 {
		t.Fatal("valid dataset transfer did not return exact version", version, err, transfers.Load())
	}
	expired := session
	expired.ExpiresAt = time.Now().Add(-time.Second)
	if _, err := c.UploadDatasetBytes(context.Background(), expired, content); err == nil || transfers.Load() != 1 {
		t.Fatal("expired upload capability was used", err)
	}
}

func TestDatasetTransferRejectsRedirectAndMissingVersion(t *testing.T) {
	content := []byte("archive bytes")
	sum := sha256.Sum256(content)
	key := "projects/00000000-0000-0000-0000-000000000013/datasets/uploads/00000000-0000-0000-0000-000000000011"
	var redirected atomic.Int32
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Add(1) }))
	defer redirectTarget.Close()
	for name, handler := range map[string]http.HandlerFunc{
		"redirect": func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, redirectTarget.URL, http.StatusTemporaryRedirect)
		},
		"no version": func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) },
	} {
		t.Run(name, func(t *testing.T) {
			object := httptest.NewServer(handler)
			defer object.Close()
			c, err := New("http://127.0.0.1:1", "private-token", true, nil)
			if err != nil {
				t.Fatal(err)
			}
			session := DatasetUploadSession{UploadID: "00000000-0000-0000-0000-000000000011", ObjectKey: key,
				UploadURL: object.URL + "/dispatch-test/" + key, Method: http.MethodPut,
				ExpiresAt: time.Now().Add(time.Minute), declaredSize: int64(len(content)),
				declaredSHA256: hex.EncodeToString(sum[:])}
			if _, err := c.UploadDatasetBytes(context.Background(), session, content); err == nil {
				t.Fatal("unversioned or redirected dataset transfer accepted")
			}
		})
	}
	if redirected.Load() != 0 {
		t.Fatal("transfer followed a redirect")
	}
}
