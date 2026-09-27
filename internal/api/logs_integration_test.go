//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

type logSigner func(context.Context, objectstore.Object, time.Duration) (objectstore.Grant, error)

func (f logSigner) PresignDownload(ctx context.Context, object objectstore.Object, ttl time.Duration) (objectstore.Grant, error) {
	return f(ctx, object, ttl)
}

func TestHTTPLogCursorScopesExactVersionGrants(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	provision := store.WorkerProvision{Name: "log-worker", CertificateSHA256: sha256.Sum256([]byte("log test certificate")), Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4, Projects: []string{"research"}, Labels: map[string]string{"os": "linux", "architecture": "arm64"}}
	id, err := store.ProvisionWorker(ctx, pool, provision)
	if err != nil {
		t.Fatal(err)
	}
	session := uuid.NewString()
	registration := store.Registration{RequestID: uuid.NewString(), SessionID: session, ProtocolVersion: 1, Resources: provision.Resources, Slots: provision.Slots, Labels: provision.Labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}}
	if _, err := store.RegisterSession(ctx, pool, id, registration); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordHeartbeat(ctx, pool, id, store.HeartbeatReport{RequestID: uuid.NewString(), SessionID: session, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}); err != nil {
		t.Fatal(err)
	}
	job, err := spec.DecodeJob(bytes.NewReader(jobBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	job.Spec.Image = "registry.example.org/eval@sha256:" + strings.Repeat("a", 64)
	job.Spec.Placement.Labels["architecture"] = "arm64"
	_, hash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SubmitJob(ctx, pool, uuid.NewString(), hash, job); err != nil {
		t.Fatal(err)
	}
	acquired, err := store.AcquireWork(ctx, pool, id, store.AcquisitionRequest{SessionID: session, RequestID: uuid.NewString()}, store.AcquisitionPolicy{})
	if err != nil || acquired.Assignment == nil {
		t.Fatal(acquired, err)
	}
	a := acquired.Assignment.Authority
	if phase, err := store.ReportPhase(ctx, pool, id, store.PhaseReport{Authority: a, EventID: uuid.NewString(), Phase: "STARTING"}); err != nil || phase.Decision != "ACCEPTED" {
		t.Fatal(phase, err)
	}
	content := []byte("abc")
	sum := sha256.Sum256(content)
	declared, err := store.CreateUpload(ctx, pool, id, store.UploadRequest{Authority: a, RequestID: uuid.NewString(), Kind: "LOG", LogicalName: "stdout", SizeBytes: int64(len(content)), SHA256: hex.EncodeToString(sum[:]), PartCount: 1})
	if err != nil || declared.Upload == nil {
		t.Fatal(declared, err)
	}
	verified, err := store.FinalizeUpload(ctx, pool, id, store.FinalizeUploadRequest{Authority: a, RequestID: uuid.NewString(), UploadID: declared.Upload.UploadID, Object: store.ArtifactObject{Key: declared.Upload.ObjectKey, Version: "log-version", SizeBytes: int64(len(content)), SHA256: hex.EncodeToString(sum[:])}}, func(context.Context, store.ArtifactObject) error { return nil })
	if err != nil || verified.Artifact == nil {
		t.Fatal(verified, err)
	}
	segment := store.LogSegmentRequest{Authority: a, RequestID: uuid.NewString(), ArtifactID: verified.Artifact.ArtifactID, Stream: "STDOUT", FirstSequence: 1, LastSequence: 1}
	if result, err := store.RegisterLogSegment(ctx, pool, id, segment); err != nil || result.Decision != "ACCEPTED" {
		t.Fatal(result, err)
	}
	reader, tokenID, err := store.IssueToken(ctx, pool, "research", store.RoleRead)
	if err != nil {
		t.Fatal(err)
	}
	foreign, _, err := store.IssueToken(ctx, pool, "other", store.RoleRead)
	if err != nil {
		t.Fatal(err)
	}
	var signed int
	h := New(pool, nil, logSigner(func(_ context.Context, object objectstore.Object, ttl time.Duration) (objectstore.Grant, error) {
		if object.Key != declared.Upload.ObjectKey || object.Version != "log-version" || object.Size != 3 || ttl != time.Minute {
			t.Fatal("signed wrong object version", object)
		}
		signed++
		return objectstore.Grant{URL: "https://storage.example.test/log", Method: http.MethodGet, ExpiresAt: time.Now().Add(ttl)}, nil
	}))
	path := "/v1/attempts/" + a.AttemptID + "/logs?stream=stdout&limit=1"
	historyPath := "/v1/jobs/" + a.JobID + "/attempts"
	if w := call(h, http.MethodGet, historyPath, foreign, "", nil); w.Code != 404 {
		t.Fatal("foreign attempt history was visible", w.Code)
	}
	var history store.AttemptHistory
	if w := call(h, http.MethodGet, historyPath, reader, "", nil); w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &history) != nil || history.JobID != a.JobID || len(history.Attempts) != 1 || history.Attempts[0].ID != a.AttemptID || history.Attempts[0].State != "STARTING" {
		t.Fatal("authorized attempt history", w.Code, w.Body.String())
	}
	if w := call(h, http.MethodGet, path, foreign, "", nil); w.Code != 404 || signed != 0 {
		t.Fatal("foreign project received log grant", w.Code, signed)
	}
	if w := call(h, http.MethodGet, path+"&cursor=bad", reader, "", nil); w.Code != 400 || signed != 0 {
		t.Fatal("invalid cursor received log grant", w.Code, signed)
	}
	w := call(h, http.MethodGet, path, reader, "", nil)
	var page LogList
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || signed != 1 || page.HasMore || page.Completion != nil || len(page.Segments) != 1 || page.Segments[0].Stream != "stdout" || page.Segments[0].ArtifactID != segment.ArtifactID || page.Segments[0].Object.Version != "log-version" || page.Segments[0].DownloadURL == "" || page.NextCursor == "" {
		t.Fatal("authorized log page", w.Code, w.Body.String(), signed)
	}
	w = call(h, http.MethodGet, path+"&cursor="+page.NextCursor, reader, "", nil)
	var next LogList
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &next) != nil || signed != 1 || len(next.Segments) != 0 || next.NextCursor != page.NextCursor {
		t.Fatal("reconnect cursor replayed a segment", w.Code, w.Body.String(), signed)
	}
	if err := store.RevokeToken(ctx, pool, "research", tokenID); err != nil {
		t.Fatal(err)
	}
	if w := call(h, http.MethodGet, path, reader, "", nil); w.Code != 401 || signed != 1 {
		t.Fatal("revoked token received log grant", w.Code, signed)
	}
}
