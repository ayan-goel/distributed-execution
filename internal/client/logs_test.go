package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const logVector = "4453504c4f47303131323365343536372d653839622d313264332d613435362d34323636313431373430303001000000000000000000000117979cfe362a00000000000500ff1b5b41000000000000000317979cfe362a002a000000010a"
const logAttempt = "123e4567-e89b-12d3-a456-426614174000"
const logArtifact = "00000000-0000-0000-0000-000000000001"

func TestLogClientVerifiesExactBinaryObjectAndCatalogGap(t *testing.T) {
	raw, _ := hex.DecodeString(logVector)
	sum := sha256.Sum256(raw)
	var storageCalls atomic.Int32
	var tamper atomic.Bool
	object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		storageCalls.Add(1)
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.URL.Query().Get("versionId") != "v1" {
			t.Error("storage request leaked credentials or changed version")
		}
		w.Header().Set("X-Amz-Version-Id", "v1")
		if tamper.Load() {
			changed := append([]byte(nil), raw...)
			changed[len(changed)-1] ^= 1
			_, _ = w.Write(changed)
		} else {
			_, _ = w.Write(raw)
		}
	}))
	defer object.Close()
	segment := map[string]any{"artifactId": logArtifact, "stream": "stdout", "firstSequence": 1, "lastSequence": 3,
		"gaps":        []any{map[string]any{"firstSequence": 2, "lastSequence": 2}},
		"object":      map[string]any{"key": "projects/test/log", "version": "v1", "sizeBytes": len(raw), "sha256": hex.EncodeToString(sum[:])},
		"downloadUrl": object.URL + "/bucket/projects/test/log?versionId=v1&signature=private", "method": "GET",
		"requiredHeaders": map[string][]string{}, "expiresAt": time.Now().Add(time.Minute)}
	page := map[string]any{"attemptId": logAttempt, "stream": "stdout", "segments": []any{segment}, "nextCursor": "opaque", "hasMore": false, "completion": nil}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer project-private" || r.URL.Path != "/v1/attempts/"+logAttempt+"/logs" || r.URL.Query().Get("stream") != "stdout" {
			t.Error("log metadata request lost project scope")
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer api.Close()
	c, err := New(api.URL, "project-private", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	listed, err := c.ListLogs(context.Background(), logAttempt, "stdout", "")
	if err != nil || len(listed.Segments) != 1 {
		t.Fatal(listed, err)
	}
	decoded, err := c.FetchLogSegment(context.Background(), logAttempt, listed.Segments[0])
	if err != nil || len(decoded.Records) != 2 || decoded.Records[0].Payload[1] != 0xff || storageCalls.Load() != 1 {
		t.Fatal(decoded, err)
	}
	tamper.Store(true)
	if _, err := c.FetchLogSegment(context.Background(), logAttempt, listed.Segments[0]); err == nil {
		t.Fatal("tampered log object accepted")
	}
	tamper.Store(false)
	segment["gaps"] = []any{}
	if _, err := c.FetchLogSegment(context.Background(), logAttempt, listed.Segments[0]); err != nil {
		t.Fatal("saved metadata changed", err)
	}
	changed, err := c.ListLogs(context.Background(), logAttempt, "stdout", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.FetchLogSegment(context.Background(), logAttempt, changed.Segments[0]); err == nil {
		t.Fatal("object gap mismatch accepted")
	}
}

func TestLogClientRejectsUnsafeGrantBeforeStorage(t *testing.T) {
	raw, _ := hex.DecodeString(logVector)
	sum := sha256.Sum256(raw)
	segment := LogSegment{ArtifactID: logArtifact, Stream: "stdout", FirstSequence: 1, LastSequence: 3,
		Object:      downloadObject{Key: "projects/test/log", Version: "v1", SizeBytes: int64(len(raw)), SHA256: hex.EncodeToString(sum[:])},
		DownloadURL: "https://storage.example.org/projects/test/log?versionId=v1", Method: "GET", ExpiresAt: time.Now().Add(time.Minute)}
	c, _ := New("https://dispatch.example.org", "private", false, transportFunc(func(*http.Request) (*http.Response, error) { t.Fatal("metadata transport used"); return nil, nil }))
	segment.RequiredHeaders = http.Header{"Authorization": []string{"Bearer private"}}
	if _, err := c.FetchLogSegment(context.Background(), logAttempt, segment); err == nil {
		t.Fatal("project token header accepted")
	}
	segment.RequiredHeaders = http.Header{}
	segment.DownloadURL = strings.Replace(segment.DownloadURL, "versionId=v1", "versionId=v2", 1)
	if _, err := c.FetchLogSegment(context.Background(), logAttempt, segment); err == nil {
		t.Fatal("changed object version accepted")
	}
}
