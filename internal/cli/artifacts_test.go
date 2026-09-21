package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/client"
)

func TestArtifactDownloadCommandVerifiesBeforeReportingSuccess(t *testing.T) {
	const job = "00000000-0000-0000-0000-000000000001"
	const attempt = "00000000-0000-0000-0000-000000000002"
	const artifact = "00000000-0000-0000-0000-000000000003"
	const body = "accepted output"
	for _, mode := range []string{"json", "text", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "" {
					t.Error("CLI leaked project token to storage")
				}
				w.Header().Set("X-Amz-Version-Id", "accepted-version")
				if mode == "corrupt" {
					_, _ = fmt.Fprint(w, strings.Repeat("x", len(body)))
				} else {
					_, _ = fmt.Fprint(w, body)
				}
			}))
			defer storage.Close()
			checksum := sha256.Sum256([]byte(body))
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer project-private" || r.URL.Path != "/v1/jobs/"+job+"/artifacts" {
					t.Error("incorrect artifact API request")
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"jobId": job, "state": "SUCCEEDED", "acceptedAttemptId": attempt, "artifacts": []any{map[string]any{"name": "result", "artifactId": artifact, "object": map[string]any{"key": "objects/result", "version": "accepted-version", "sizeBytes": len(body), "sha256": hex.EncodeToString(checksum[:])}, "downloadUrl": storage.URL + "/objects/result?versionId=accepted-version&signature=private-capability", "method": "GET", "requiredHeaders": map[string][]string{}, "expiresAt": time.Now().Add(time.Minute)}}})
			}))
			defer api.Close()
			env := func(key string) string {
				return map[string]string{"DISPATCH_URL": api.URL, "DISPATCH_TOKEN": "project-private", "DISPATCH_DEV_INSECURE": "1"}[key]
			}
			dest := filepath.Join(t.TempDir(), "output\nname")
			args := []string{"artifacts", "download", job, "result", "--output", dest}
			if mode != "text" {
				args = append(args, "--json")
			}
			var out, errs bytes.Buffer
			code := Run(context.Background(), args, env, &out, &errs)
			if strings.Contains(out.String()+errs.String(), "private-capability") || strings.Contains(out.String()+errs.String(), "project-private") {
				t.Fatal("CLI disclosed transfer credentials")
			}
			if mode == "corrupt" {
				if code != 2 || out.Len() != 0 || errs.Len() == 0 {
					t.Fatal("failed download reported success", code, out.String())
				}
				if _, err := os.Stat(dest); !os.IsNotExist(err) {
					t.Fatal("failed download published destination")
				}
				return
			}
			if code != 0 || errs.Len() != 0 {
				t.Fatal("download command failed", code, errs.String())
			}
			content, err := os.ReadFile(dest)
			if err != nil || string(content) != body {
				t.Fatal("CLI saved incorrect bytes", err)
			}
			if mode == "json" {
				var receipt client.DownloadReceipt
				if err := json.Unmarshal(out.Bytes(), &receipt); err != nil || receipt.JobID != job || receipt.AttemptID != attempt || receipt.ArtifactID != artifact || receipt.Path != dest || receipt.SHA256 != hex.EncodeToString(checksum[:]) {
					t.Fatal("receipt lost verified identity", err)
				}
			} else if !strings.Contains(out.String(), `output\nname`) || strings.Count(out.String(), "\n") != 1 {
				t.Fatal("text receipt did not quote destination")
			}
		})
	}
}

func TestArtifactDownloadUsageRejectsMissingDestinationAndExtraArguments(t *testing.T) {
	for _, args := range [][]string{
		{"artifacts"}, {"artifacts", "download"}, {"artifacts", "download", "job"}, {"artifacts", "download", "job", "result"},
		{"artifacts", "list", "job", "result", "--output", "x"},
		{"artifacts", "download", "job", "result", "--output"},
		{"artifacts", "download", "job", "result", "--output", "x", "extra"},
		{"artifacts", "download", "job", "result", "--output", "x", "--unknown"},
	} {
		var out, errs bytes.Buffer
		if code := Run(context.Background(), args, func(string) string { t.Fatal("invalid usage consulted credentials"); return "" }, &out, &errs); code != 2 || out.Len() != 0 || errs.Len() == 0 {
			t.Fatal("invalid artifact usage accepted", args, code)
		}
	}
}
