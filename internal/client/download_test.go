package client

import (
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
	"sync/atomic"
	"testing"
	"time"
)

const downloadJob = "00000000-0000-0000-0000-000000000001"
const downloadAttempt = "00000000-0000-0000-0000-000000000002"
const downloadID = "00000000-0000-0000-0000-000000000003"
const downloadBody = "verified output bytes"

func downloadFixture(t *testing.T, handler http.HandlerFunc, mutate func(map[string]any)) (*Client, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("project credentials reached storage")
		}
		if r.URL.Query().Get("versionId") != "accepted-version" {
			t.Error("version query lost")
		}
		if handler != nil {
			handler(w, r)
			return
		}
		w.Header().Set("X-Amz-Version-Id", "accepted-version")
		w.Header().Set("Content-Length", fmt.Sprint(len(downloadBody)))
		_, _ = fmt.Fprint(w, downloadBody)
	}))
	t.Cleanup(object.Close)
	checksum := sha256.Sum256([]byte(downloadBody))
	metadata := map[string]any{"jobId": downloadJob, "state": "SUCCEEDED", "acceptedAttemptId": downloadAttempt, "artifacts": []any{map[string]any{"name": "result", "artifactId": downloadID, "object": map[string]any{"key": "objects/result", "version": "accepted-version", "sizeBytes": len(downloadBody), "sha256": hex.EncodeToString(checksum[:])}, "downloadUrl": object.URL + "/bucket/objects/result?versionId=accepted-version&signature=private-capability", "method": "GET", "requiredHeaders": map[string][]string{}, "expiresAt": time.Now().Add(time.Minute)}}}
	if mutate != nil {
		mutate(metadata)
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer project-private" || r.URL.Path != "/v1/jobs/"+downloadJob+"/artifacts" {
			t.Error("metadata request lost scope")
		}
		_ = json.NewEncoder(w).Encode(metadata)
	}))
	t.Cleanup(api.Close)
	c, err := New(api.URL, "project-private", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c, calls
}

func artifactMap(m map[string]any) map[string]any { return m["artifacts"].([]any)[0].(map[string]any) }

func TestArtifactDownloadPublishesVerifiedPrivateFile(t *testing.T) {
	c, calls := downloadFixture(t, nil, nil)
	dir := t.TempDir()
	dest := filepath.Join(dir, "result.bin")
	receipt, err := c.DownloadArtifact(context.Background(), downloadJob, "result", dest)
	if err != nil || receipt.ArtifactID != downloadID || receipt.AttemptID != downloadAttempt || receipt.SizeBytes != int64(len(downloadBody)) || receipt.Path != dest || calls.Load() != 1 {
		t.Fatal("download failed", receipt, err)
	}
	content, err := os.ReadFile(dest)
	if err != nil || string(content) != downloadBody {
		t.Fatal("wrong published bytes", err)
	}
	info, err := os.Stat(dest)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("output permissions not private", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatal("temporary files retained", err)
	}
	encoded, _ := json.Marshal(receipt)
	if strings.Contains(string(encoded), "private-capability") || strings.Contains(string(encoded), "downloadUrl") {
		t.Fatal("receipt leaked a capability")
	}
}

func TestArtifactDownloadRejectsInvalidMetadataBeforeTransfer(t *testing.T) {
	for name, change := range map[string]func(map[string]any){
		"foreign job":              func(m map[string]any) { m["jobId"] = downloadID },
		"failed result":            func(m map[string]any) { m["state"] = "FAILED" },
		"missing accepted attempt": func(m map[string]any) { m["acceptedAttemptId"] = nil },
		"duplicate name":           func(m map[string]any) { m["artifacts"] = []any{artifactMap(m), artifactMap(m)} },
		"invalid artifact ID":      func(m map[string]any) { artifactMap(m)["artifactId"] = "bad" },
		"traversal name":           func(m map[string]any) { artifactMap(m)["name"] = "../result" },
		"large object":             func(m map[string]any) { artifactMap(m)["object"].(map[string]any)["sizeBytes"] = int64(1) << 62 },
		"negative size":            func(m map[string]any) { artifactMap(m)["object"].(map[string]any)["sizeBytes"] = -1 },
		"invalid hash":             func(m map[string]any) { artifactMap(m)["object"].(map[string]any)["sha256"] = "bad" },
		"null version":             func(m map[string]any) { artifactMap(m)["object"].(map[string]any)["version"] = "null" },
		"wrong version URL": func(m map[string]any) {
			a := artifactMap(m)
			a["downloadUrl"] = strings.Replace(a["downloadUrl"].(string), "versionId=accepted-version", "versionId=other", 1)
		},
		"duplicate version URL": func(m map[string]any) {
			a := artifactMap(m)
			a["downloadUrl"] = a["downloadUrl"].(string) + "&versionId=other"
		},
		"wrong key URL": func(m map[string]any) {
			a := artifactMap(m)
			a["downloadUrl"] = strings.Replace(a["downloadUrl"].(string), "/objects/result", "/other", 1)
		},
		"remote HTTP": func(m map[string]any) {
			artifactMap(m)["downloadUrl"] = "http://remote.example.org/objects/result?versionId=accepted-version"
		},
		"userinfo": func(m map[string]any) {
			artifactMap(m)["downloadUrl"] = "https://private:secret@storage.example.org/objects/result?versionId=accepted-version"
		},
		"fragment":     func(m map[string]any) { a := artifactMap(m); a["downloadUrl"] = a["downloadUrl"].(string) + "#private" },
		"wrong method": func(m map[string]any) { artifactMap(m)["method"] = "POST" },
		"authorization header": func(m map[string]any) {
			artifactMap(m)["requiredHeaders"] = map[string][]string{"Authorization": {"Bearer private"}}
		},
		"range header": func(m map[string]any) {
			artifactMap(m)["requiredHeaders"] = map[string][]string{"Range": {"bytes=0-1"}}
		},
		"host header": func(m map[string]any) {
			artifactMap(m)["requiredHeaders"] = map[string][]string{"Host": {"other.example.org"}}
		},
		"expired": func(m map[string]any) { artifactMap(m)["expiresAt"] = time.Now().Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			c, calls := downloadFixture(t, nil, change)
			dir := t.TempDir()
			if _, err := c.DownloadArtifact(context.Background(), downloadJob, "result", filepath.Join(dir, "result")); err == nil || strings.Contains(err.Error(), "private-capability") {
				t.Fatal("unsafe metadata accepted/leaked", err)
			}
			entries, _ := os.ReadDir(dir)
			if calls.Load() != 0 || len(entries) != 0 {
				t.Fatal("rejected metadata caused transfer or files")
			}
		})
	}
}

func TestArtifactDownloadRejectsCorruptionAndPartialResponses(t *testing.T) {
	for _, mode := range []string{"checksum", "version", "short", "long", "status", "encoding"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := downloadFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Amz-Version-Id", "accepted-version")
				body := downloadBody
				switch mode {
				case "checksum":
					body = "X" + body[1:]
				case "version":
					w.Header().Set("X-Amz-Version-Id", "newer-version")
				case "short":
					w.Header().Set("Content-Length", fmt.Sprint(len(body)))
					body = body[:len(body)-1]
				case "long":
					body += "X"
				case "status":
					w.WriteHeader(403)
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
				}
				_, _ = fmt.Fprint(w, body)
			}, nil)
			dir := t.TempDir()
			if _, err := c.DownloadArtifact(context.Background(), downloadJob, "result", filepath.Join(dir, "result")); err == nil {
				t.Fatal("invalid bytes published")
			}
			entries, _ := os.ReadDir(dir)
			if len(entries) != 0 {
				t.Fatal("failed download left local files")
			}
		})
	}
}

func TestArtifactDownloadNeverOverwritesExistingOrRacingDestination(t *testing.T) {
	for _, mode := range []string{"existing", "symlink", "racing"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			dest := filepath.Join(dir, "result")
			target := filepath.Join(dir, "target")
			if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			if mode == "existing" {
				if err := os.WriteFile(dest, []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "symlink" {
				if err := os.Symlink(target, dest); err != nil {
					t.Fatal(err)
				}
			}
			c, _ := downloadFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if mode == "racing" {
					if err := os.WriteFile(dest, []byte("keep"), 0600); err != nil {
						t.Error(err)
					}
				}
				w.Header().Set("X-Amz-Version-Id", "accepted-version")
				_, _ = fmt.Fprint(w, downloadBody)
			}, nil)
			if _, err := c.DownloadArtifact(context.Background(), downloadJob, "result", dest); err == nil {
				t.Fatal("overwrote destination")
			}
			content, err := os.ReadFile(dest)
			if err != nil || string(content) != "keep" {
				t.Fatal("existing bytes changed", err)
			}
			content, err = os.ReadFile(target)
			if err != nil || string(content) != "keep" {
				t.Fatal("symlink target changed", err)
			}
			entries, _ := os.ReadDir(dir)
			if len(entries) != 2 {
				t.Fatal("temporary files retained")
			}
		})
	}
}

func TestArtifactDownloadDoesNotFollowRedirects(t *testing.T) {
	var reached atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Store(true) }))
	defer target.Close()
	c, _ := downloadFixture(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}, nil)
	if _, err := c.DownloadArtifact(context.Background(), downloadJob, "result", filepath.Join(t.TempDir(), "result")); err == nil || reached.Load() {
		t.Fatal("redirect followed", err)
	}
}

func TestArtifactDownloadCancellationRemovesTemporaryData(t *testing.T) {
	started := make(chan struct{})
	c, _ := downloadFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Amz-Version-Id", "accepted-version")
		w.Header().Set("Content-Length", fmt.Sprint(len(downloadBody)))
		_, _ = fmt.Fprint(w, downloadBody[:1])
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	done := make(chan error, 1)
	go func() {
		_, err := c.DownloadArtifact(ctx, downloadJob, "result", filepath.Join(dir, "result"))
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled download succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled download stuck")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal("cancelled download left data")
	}
}

func TestArtifactDownloadAcceptsEmptyAndChunkedVerifiedBodies(t *testing.T) {
	for _, empty := range []bool{true, false} {
		t.Run(map[bool]string{true: "empty", false: "chunked"}[empty], func(t *testing.T) {
			body := downloadBody
			if empty {
				body = ""
			}
			hash := sha256.Sum256([]byte(body))
			c, _ := downloadFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Amz-Version-Id", "accepted-version")
				w.(http.Flusher).Flush()
				_, _ = fmt.Fprint(w, body)
			}, func(m map[string]any) {
				o := artifactMap(m)["object"].(map[string]any)
				o["sizeBytes"] = len(body)
				o["sha256"] = hex.EncodeToString(hash[:])
			})
			dest := filepath.Join(t.TempDir(), "result")
			if _, err := c.DownloadArtifact(context.Background(), downloadJob, "result", dest); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(dest)
			if err != nil || string(got) != body {
				t.Fatal("valid stream rejected", err)
			}
		})
	}
}

func TestArtifactDownloadBoundsChunkedBodiesAndKeepsOriginalDirectory(t *testing.T) {
	t.Run("excess chunked bytes", func(t *testing.T) {
		c, _ := downloadFixture(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Amz-Version-Id", "accepted-version")
			w.(http.Flusher).Flush()
			_, _ = fmt.Fprint(w, downloadBody+"extra bytes")
		}, nil)
		dir := t.TempDir()
		if _, err := c.DownloadArtifact(context.Background(), downloadJob, "result", filepath.Join(dir, "result")); err == nil {
			t.Fatal("oversized chunked stream accepted")
		}
		entries, _ := os.ReadDir(dir)
		if len(entries) != 0 {
			t.Fatal("failed stream left files")
		}
	})
	t.Run("parent renamed during transfer", func(t *testing.T) {
		parent := t.TempDir()
		original := filepath.Join(parent, "chosen")
		moved := filepath.Join(parent, "moved")
		if err := os.Mkdir(original, 0700); err != nil {
			t.Fatal(err)
		}
		c, _ := downloadFixture(t, func(w http.ResponseWriter, r *http.Request) {
			if err := os.Rename(original, moved); err != nil {
				t.Error(err)
			}
			if err := os.Mkdir(original, 0700); err != nil {
				t.Error(err)
			}
			w.Header().Set("X-Amz-Version-Id", "accepted-version")
			_, _ = fmt.Fprint(w, downloadBody)
		}, nil)
		if _, err := c.DownloadArtifact(context.Background(), downloadJob, "result", filepath.Join(original, "result")); err != nil {
			t.Fatal(err)
		}
		content, err := os.ReadFile(filepath.Join(moved, "result"))
		if err != nil || string(content) != downloadBody {
			t.Fatal("lost opened-directory identity", err)
		}
		entries, _ := os.ReadDir(original)
		if len(entries) != 0 {
			t.Fatal("published into replacement directory")
		}
	})
}
