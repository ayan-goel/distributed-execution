package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/client"
)

func TestDatasetUploadCLICompletesAndResumesWithoutAnotherPut(t *testing.T) {
	const requestID = "00000000-0000-0000-0000-000000000014"
	const uploadID = "00000000-0000-0000-0000-000000000011"
	const projectID = "00000000-0000-0000-0000-000000000013"
	const datasetID = "00000000-0000-0000-0000-000000000012"
	const version = "immutable-version"
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "input.txt"), []byte("dataset input"), 0600); err != nil {
		t.Fatal(err)
	}
	archive, manifest, err := buildDatasetArchive(root)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(archive)
	digest := hex.EncodeToString(sum[:])
	key := "projects/" + projectID + "/datasets/uploads/" + uploadID
	var puts, declarations, completions atomic.Int32
	object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
		body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<20+1))
		if !bytes.Equal(body, archive) || r.Header.Get("Authorization") != "" {
			t.Error("CLI sent wrong archive or project token to storage")
		}
		w.Header().Set("X-Amz-Version-Id", version)
		w.WriteHeader(200)
	}))
	defer object.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer project-token" {
			t.Error("CLI metadata call lost token")
		}
		switch r.URL.Path {
		case "/v1/datasets/uploads":
			declarations.Add(1)
			var got struct {
				RequestID string `json:"requestId"`
				Name      string `json:"name"`
				SizeBytes int64  `json:"sizeBytes"`
				SHA256    string `json:"sha256"`
			}
			if json.NewDecoder(r.Body).Decode(&got) != nil || got.RequestID != requestID || got.Name != "data-v1" ||
				got.SizeBytes != int64(len(archive)) || got.SHA256 != digest {
				t.Error("CLI declaration disagrees with archive")
			}
			w.WriteHeader(201)
			_ = json.NewEncoder(w).Encode(client.DatasetUploadSession{UploadID: uploadID, ObjectKey: key,
				UploadURL: object.URL + "/dispatch-test/" + key + "?signature=private",
				Method:    http.MethodPut, ExpiresAt: time.Now().Add(time.Minute), Replayed: declarations.Load() > 1})
		case "/v1/datasets/uploads/" + uploadID + "/complete":
			completions.Add(1)
			var got struct {
				Version  string                 `json:"version"`
				Manifest client.DatasetManifest `json:"manifest"`
			}
			if json.NewDecoder(r.Body).Decode(&got) != nil || got.Version != version || !reflect.DeepEqual(got.Manifest, manifest) {
				t.Error("CLI completion did not preserve uploaded version and manifest")
			}
			w.WriteHeader(201)
			_ = json.NewEncoder(w).Encode(client.DatasetRegistration{DatasetID: datasetID, Name: "data-v1", UploadID: uploadID,
				ObjectKey: key, ObjectVersion: version, SizeBytes: int64(len(archive)), SHA256: digest, Manifest: manifest})
		default:
			t.Error("unexpected dataset API route", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	getenv := func(name string) string {
		switch name {
		case "DISPATCH_URL":
			return api.URL
		case "DISPATCH_TOKEN":
			return "project-token"
		case "DISPATCH_DEV_INSECURE":
			return "1"
		}
		return ""
	}
	args := []string{"dataset", "upload", root, "--name", "data-v1", "--request-id", requestID}
	var out, errout bytes.Buffer
	if code := Run(context.Background(), args, getenv, &out, &errout); code != 0 ||
		!strings.Contains(out.String(), datasetID) || !strings.Contains(errout.String(), requestID) ||
		!strings.Contains(errout.String(), version) || puts.Load() != 1 || completions.Load() != 1 {
		t.Fatal("CLI dataset upload failed", code, out.String(), errout.String(), puts.Load(), completions.Load())
	}
	out.Reset()
	errout.Reset()
	args = append(args, "--resume-version", version)
	if code := Run(context.Background(), args, getenv, &out, &errout); code != 0 || puts.Load() != 1 ||
		declarations.Load() != 2 || completions.Load() != 2 {
		t.Fatal("completion recovery reuploaded bytes", code, out.String(), errout.String(), puts.Load())
	}
}
