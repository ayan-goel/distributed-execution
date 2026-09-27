package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const cliLogVector = "4453504c4f47303131323365343536372d653839622d313264332d613435362d34323636313431373430303001000000000000000000000117979cfe362a00000000000500ff1b5b41000000000000000317979cfe362a002a000000010a"
const cliJob = "00000000-0000-0000-0000-000000000001"
const cliAttempt = "123e4567-e89b-12d3-a456-426614174000"

func TestEscapedLogBytesCannotControlTerminal(t *testing.T) {
	got := escapeLog([]byte{'A', '\n', '\r', '\t', 0, 0xff, 0x1b, '[', 'A', '\\'})
	if got != `A\x0a\x0d\x09\x00\xff\x1b[A\x5c` {
		t.Fatal(got)
	}
}

func TestLogsCommandFetchesVerifiedBinarySegments(t *testing.T) {
	raw, _ := hex.DecodeString(cliLogVector)
	sum := sha256.Sum256(raw)
	object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.URL.Query().Get("versionId") != "v1" {
			t.Error("storage request leaked authority")
		}
		w.Header().Set("X-Amz-Version-Id", "v1")
		_, _ = w.Write(raw)
	}))
	defer object.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer project-private" {
			t.Error("missing project token")
		}
		switch r.URL.Path {
		case "/v1/jobs/" + cliJob + "/attempts":
			_ = json.NewEncoder(w).Encode(map[string]any{"jobId": cliJob, "attempts": []any{map[string]any{"id": cliAttempt, "number": 1, "state": "RUNNING", "workerId": "00000000-0000-0000-0000-000000000003", "createdAt": "2026-09-27T00:00:00Z"}}})
		case "/v1/attempts/" + cliAttempt + "/logs":
			if r.URL.Query().Get("stream") != "stdout" {
				t.Error("wrong stream")
			}
			segment := map[string]any{"artifactId": cliJob, "stream": "stdout", "firstSequence": 1, "lastSequence": 3, "gaps": []any{map[string]any{"firstSequence": 2, "lastSequence": 2}},
				"object":      map[string]any{"key": "projects/test/log", "version": "v1", "sizeBytes": len(raw), "sha256": hex.EncodeToString(sum[:])},
				"downloadUrl": object.URL + "/bucket/projects/test/log?versionId=v1", "method": "GET", "requiredHeaders": map[string][]string{}, "expiresAt": time.Now().Add(time.Minute)}
			_ = json.NewEncoder(w).Encode(map[string]any{"attemptId": cliAttempt, "stream": "stdout", "segments": []any{segment}, "nextCursor": "opaque", "hasMore": false, "completion": nil})
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	getenv := func(key string) string {
		switch key {
		case "DISPATCH_URL":
			return api.URL
		case "DISPATCH_TOKEN":
			return "project-private"
		case "DISPATCH_DEV_INSECURE":
			return "1"
		}
		return ""
	}
	var out, errors bytes.Buffer
	if code := Run(context.Background(), []string{"logs", cliJob, "--stream", "stdout"}, getenv, &out, &errors); code != 0 {
		t.Fatal(code, errors.String())
	}
	rendered := out.String()
	if !strings.Contains(rendered, "attempt "+cliAttempt) || !strings.Contains(rendered, `[stdout #1] \x00\xff\x1b[A`) || !strings.Contains(rendered, `[stdout #3] \x0a`) || strings.ContainsRune(rendered, '\x1b') {
		t.Fatal(rendered)
	}
}

func TestLogsFollowStopsAfterTerminalCompletion(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/jobs/" + cliJob + "/attempts":
			_ = json.NewEncoder(w).Encode(map[string]any{"jobId": cliJob, "attempts": []any{map[string]any{"id": cliAttempt, "number": 1, "state": "FAILED", "workerId": "00000000-0000-0000-0000-000000000003", "createdAt": "2026-09-27T00:00:00Z"}}})
		case "/v1/jobs/" + cliJob:
			_ = json.NewEncoder(w).Encode(map[string]any{"id": cliJob, "state": "FAILED"})
		case "/v1/attempts/" + cliAttempt + "/logs":
			_ = json.NewEncoder(w).Encode(map[string]any{"attemptId": cliAttempt, "stream": r.URL.Query().Get("stream"), "segments": []any{}, "nextCursor": "", "hasMore": false, "completion": map[string]any{"logsComplete": false, "gaps": []any{map[string]any{"stream": "stdout", "first": 1, "last": 3}}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	getenv := func(key string) string {
		switch key {
		case "DISPATCH_URL":
			return api.URL
		case "DISPATCH_TOKEN":
			return "project-private"
		case "DISPATCH_DEV_INSECURE":
			return "1"
		}
		return ""
	}
	var out, errors bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if code := Run(ctx, []string{"logs", cliJob, "--follow"}, getenv, &out, &errors); code != 0 || !strings.Contains(out.String(), "[logs incomplete stdout:1-3]") {
		t.Fatal(code, out.String(), errors.String())
	}
}
