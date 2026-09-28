package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDatasetClientBindsUploadAndCompletionResponses(t *testing.T) {
	const uploadID = "00000000-0000-0000-0000-000000000011"
	const datasetID = "00000000-0000-0000-0000-000000000012"
	const projectID = "00000000-0000-0000-0000-000000000013"
	const requestID = "00000000-0000-0000-0000-000000000014"
	const version = "immutable-version"
	key := "projects/" + projectID + "/datasets/uploads/" + uploadID
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("Authorization") != "Bearer private-token" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("dataset metadata request lost authentication or content type")
		}
		if requests == 1 {
			var body struct {
				RequestID string `json:"requestId"`
				Name      string `json:"name"`
				SizeBytes int64  `json:"sizeBytes"`
				SHA256    string `json:"sha256"`
			}
			if r.URL.Path != "/v1/datasets/uploads" || json.NewDecoder(r.Body).Decode(&body) != nil ||
				body.RequestID != requestID || body.Name != "data-v1" || body.SizeBytes != 3 || body.SHA256 != strings.Repeat("a", 64) {
				t.Error("wrong upload declaration")
			}
			w.WriteHeader(201)
			_ = json.NewEncoder(w).Encode(DatasetUploadSession{UploadID: uploadID, ObjectKey: key,
				UploadURL: "https://storage.example.test/dispatch-test/" + key + "?signature=private",
				Method:    http.MethodPut, RequiredHeaders: http.Header{"X-Amz-Checksum-Sha256": {"signed"}},
				ExpiresAt: time.Now().Add(time.Minute)})
			return
		}
		var body struct {
			Version  string          `json:"version"`
			Manifest DatasetManifest `json:"manifest"`
		}
		if r.URL.Path != "/v1/datasets/uploads/"+uploadID+"/complete" || json.NewDecoder(r.Body).Decode(&body) != nil ||
			body.Version != version || body.Manifest.Files[0].Path != "input.txt" {
			t.Error("wrong dataset completion request")
		}
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(DatasetRegistration{DatasetID: datasetID, Name: "data-v1", UploadID: uploadID,
			ObjectKey: key, ObjectVersion: version, SizeBytes: 3, SHA256: strings.Repeat("a", 64), Manifest: body.Manifest})
	}))
	defer server.Close()
	c, err := New(server.URL, "private-token", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	session, err := c.CreateDatasetUpload(context.Background(), requestID, "data-v1", 3, strings.Repeat("a", 64))
	if err != nil || session.UploadID != uploadID || session.ObjectKey != key {
		t.Fatal("upload response rejected", err)
	}
	manifest := DatasetManifest{Format: "tar.v1", Files: []DatasetFile{{Path: "input.txt", SizeBytes: 3, SHA256: strings.Repeat("b", 64)}}}
	registered, err := c.CompleteDatasetUpload(context.Background(), session, version, manifest)
	if err != nil || registered.DatasetID != datasetID || registered.ObjectVersion != version || requests != 2 {
		t.Fatal("completion response rejected", registered, err, requests)
	}
}

func TestDatasetClientRejectsUnscopedUploadGrant(t *testing.T) {
	const requestID = "00000000-0000-0000-0000-000000000014"
	const uploadID = "00000000-0000-0000-0000-000000000011"
	const projectID = "00000000-0000-0000-0000-000000000013"
	key := "projects/" + projectID + "/datasets/uploads/" + uploadID
	for name, mutate := range map[string]func(*DatasetUploadSession){
		"caller-selected path": func(s *DatasetUploadSession) { s.ObjectKey = "projects/" + projectID + "/other/" + uploadID },
		"cross-key url":        func(s *DatasetUploadSession) { s.UploadURL = "https://storage.example.test/other-key" },
		"credential header":    func(s *DatasetUploadSession) { s.RequiredHeaders.Set("Authorization", "secret") },
		"expired grant":        func(s *DatasetUploadSession) { s.ExpiresAt = time.Now().Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				session := DatasetUploadSession{UploadID: uploadID, ObjectKey: key,
					UploadURL: "https://storage.example.test/dispatch-test/" + key + "?signature=private",
					Method:    http.MethodPut, ExpiresAt: time.Now().Add(time.Minute)}
				mutate(&session)
				w.WriteHeader(201)
				_ = json.NewEncoder(w).Encode(session)
			}))
			defer server.Close()
			c, err := New(server.URL, "private-token", true, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.CreateDatasetUpload(context.Background(), requestID, "data-v1", 3, strings.Repeat("a", 64)); err == nil {
				t.Fatal("invalid upload grant accepted")
			}
		})
	}
}
