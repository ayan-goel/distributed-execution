//go:build integration

package workerapi

import (
	"context"
	"crypto/sha256"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInputAssignmentSignsTheSameVersionAfterAnUncertainReply(t *testing.T) {
	pool := workerTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var projectID string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM projects WHERE name='research'").Scan(&projectID); err != nil {
		t.Fatal(err)
	}
	upload, err := store.CreateDatasetUpload(ctx, pool, store.DatasetUploadRequest{ProjectID: projectID,
		RequestID: uuid.NewString(), Name: "registered", SizeBytes: 4096, SHA256: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	recorded, err := store.RegisterDataset(ctx, pool, store.DatasetRegistrationRequest{ProjectID: projectID,
		UploadID: upload.ID, Version: "version-1", Manifest: store.DatasetManifest{Format: "tar.v1",
			Files: []store.DatasetFile{{Path: "data.txt", SizeBytes: 4, SHA256: strings.Repeat("b", 64)}}}},
		func(context.Context, objectstore.Object) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := store.ResolveDatasetNames(ctx, pool, projectID, []string{"registered"})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	job, err := spec.DecodeJob(file)
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	job.Spec.Image = "registry.example.org/eval@sha256:" + strings.Repeat("c", 64)
	job.Spec.Placement.Labels["architecture"] = "arm64"
	job.Spec.Inputs = []spec.Input{{Dataset: "registered", MountPath: "/inputs/data"}}
	_, requestHash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	queued, err := store.SubmitJobResolved(ctx, pool, uuid.NewString(), requestHash, job, bindings)
	if err != nil {
		t.Fatal(err)
	}
	provision := store.WorkerProvision{Name: "input-rpc-host", CertificateSHA256: sha256.Sum256([]byte("input fixture certificate")),
		Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 2,
		Labels: map[string]string{"os": "linux", "architecture": "arm64"}, Projects: []string{"research"}}
	identity, err := store.ProvisionWorker(ctx, pool, provision)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := uuid.NewString()
	_, err = store.RegisterSession(ctx, pool, identity, store.Registration{RequestID: uuid.NewString(), SessionID: sessionID,
		ProtocolVersion: 1, Resources: provision.Resources, Slots: provision.Slots, Labels: provision.Labels,
		Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.RecordHeartbeat(ctx, pool, identity, store.HeartbeatReport{RequestID: uuid.NewString(), SessionID: sessionID,
		Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true})
	if err != nil {
		t.Fatal(err)
	}
	request := &pb.AcquireWorkRequest{Session: &pb.WorkerSession{WorkerId: identity.WorkerID, SessionId: sessionID}, RequestId: uuid.NewString()}
	authorized := context.WithValue(ctx, identityKey{}, identity)
	if _, err := NewService(pool, store.AcquisitionPolicy{}, nil).AcquireWork(authorized, request); status.Code(err) != codes.Unavailable {
		t.Fatal("missing signer did not fail after durable assignment", err)
	}
	objects, err := objectstore.New(objectstore.Config{Endpoint: "https://storage.example.org", Region: "test", Bucket: "results", AccessKey: "test-access", SecretKey: "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(pool, store.AcquisitionPolicy{}, objects)
	first, err := service.AcquireWork(authorized, request)
	if err != nil || first.GetAssignment() == nil || first.GetAssignment().GetAuthority().GetJobId() != queued.ID {
		t.Fatal("replay did not return committed assignment", err)
	}
	check := func(input *pb.InputManifest) {
		t.Helper()
		if input == nil || input.DatasetId != recorded.ID || input.DatasetName != "registered" || input.MountPath != "/inputs/data" ||
			input.GetArchive().GetVersionId() != "version-1" || input.GetArchive().GetSha256() != strings.Repeat("a", 64) ||
			!strings.Contains(string(input.FileManifestJson), `"path":"data.txt"`) || input.ExpiresUnixMs <= time.Now().UnixMilli() {
			t.Fatal("input binding changed across RPC")
		}
		parsed, err := url.Parse(input.DownloadUrl)
		if err != nil || parsed.Query().Get("versionId") != "version-1" {
			t.Fatal("download grant did not pin the registered version", err)
		}
	}
	check(first.GetAssignment().GetInputs()[0])
	page, err := service.ListAssignments(authorized, &pb.ListAssignmentsRequest{Session: request.Session, PageSize: 1})
	if err != nil || len(page.GetAssignments()) != 1 || len(page.Assignments[0].GetInputs()) != 1 {
		t.Fatal("recovery page lost input assignment", err)
	}
	check(page.Assignments[0].Inputs[0])
}
