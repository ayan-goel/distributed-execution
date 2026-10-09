//go:build integration

package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSweepProgressPagesExposeOnlyAcceptedMetrics(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	job, _ := admittedExample(t)
	job.Spec.Placement.Labels["architecture"] = "arm64"
	job.Spec.Outputs = []spec.Output{{Name: "metrics", Path: "/outputs/metrics.json", MaxBytes: MaxCompletionMetricsBytes, Required: true}}
	job.Spec.Retry.MaxAttempts = 2
	job.Spec.Retry.On = []string{"TRANSFER_FAILED"}
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: spec.Metadata{Name: "progress-grid", Project: "research"},
		Spec: spec.SweepSpec{JobTemplate: job, Matrix: map[string][]string{"SEED": {"1", "2", "3", "4", "5"}}, MaxConcurrent: 2}}
	created, err := SubmitSweepResolved(ctx, pool, uuid.NewString(), strings.Repeat("f", 64), sweep, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, reason := range []string{"", "TRANSFER_FAILED"} {
		acquired, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
		if err != nil || acquired.Assignment == nil {
			t.Fatal(acquired, err)
		}
		completeSweepMetric(t, pool, id, acquired.Assignment.Authority, reason)
	}
	if _, err := RequestCancellation(ctx, pool, created.ProjectID, created.ChildIDs[2]); err != nil {
		t.Fatal(err)
	}
	after, seen := -1, 0
	for {
		page, err := GetSweep(ctx, pool, created.ProjectID, created.ID, after, 2)
		if err != nil || page.Sweep.State != "ACTIVE" || page.Sweep.Progress.Total != 5 || page.Sweep.Progress.Succeeded != 1 || page.Sweep.Progress.RetryWait != 1 || page.Sweep.Progress.Cancelled != 1 || page.Sweep.Progress.Queued != 2 {
			t.Fatal("incorrect sweep progress", page, err)
		}
		for _, child := range page.Children {
			if child.Index != seen || child.ID != created.ChildIDs[seen] || child.Parameters["SEED"] != fmt.Sprint(seen+1) {
				t.Fatal("unstable child page", child, seen)
			}
			if seen == 0 {
				if child.AcceptedAttemptID == nil || child.Metrics["score"].String() != "9007199254740993" {
					t.Fatal("accepted metric was missing or rounded", child)
				}
			} else if child.AcceptedAttemptID != nil || len(child.Metrics) != 0 {
				t.Fatal("unaccepted attempt exposed canonical metrics", child)
			}
			seen++
		}
		if !page.HasMore {
			break
		}
		if page.NextIndex <= after || len(page.Children) != 2 {
			t.Fatal("page did not advance", page)
		}
		after = page.NextIndex
	}
	if seen != 5 {
		t.Fatal("page omitted children", seen)
	}
	if _, err := GetSweep(ctx, pool, uuid.NewString(), created.ID, -1, 2); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign project disclosed sweep", err)
	}
}

func TestSweepProgressPagesRespectByteAndRowBounds(t *testing.T) {
	pool, _, _ := readyAcquisitionWorker(t)
	ctx := context.Background()
	job, _ := admittedExample(t)
	matrix := map[string][]string{"SEED": make([]string, 100)}
	for i := range matrix["SEED"] {
		matrix["SEED"][i] = fmt.Sprint(i)
	}
	for i := range 8 {
		matrix[fmt.Sprintf("PARAM_%d", i)] = []string{strings.Repeat("x", 8192)}
	}
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: spec.Metadata{Name: "wide-grid", Project: "research"},
		Spec: spec.SweepSpec{JobTemplate: job, Matrix: matrix, MaxConcurrent: 2}}
	created, err := SubmitSweepResolved(ctx, pool, uuid.NewString(), strings.Repeat("f", 64), sweep, nil)
	if err != nil {
		t.Fatal(err)
	}
	after, count, pages := -1, 0, 0
	for {
		page, err := GetSweep(ctx, pool, created.ProjectID, created.ID, after, MaxSweepPageSize)
		if err != nil || len(page.Children) == 0 || len(page.Children) > MaxSweepPageSize {
			t.Fatal("wide page rejected", page, err)
		}
		encoded, err := json.Marshal(page.Children)
		if err != nil || len(encoded) > MaxSweepPageBytes {
			t.Fatal("child page exceeded byte limit", len(encoded), err)
		}
		for _, child := range page.Children {
			if child.Index != count || child.ID != created.ChildIDs[count] {
				t.Fatal("byte boundary skipped or repeated child", child.Index, count)
			}
			count++
		}
		pages++
		if !page.HasMore {
			break
		}
		after = page.NextIndex
	}
	if count != 100 || pages < 2 {
		t.Fatal("byte pagination did not cover the sweep", count, pages)
	}
	if empty, err := GetSweep(ctx, pool, created.ProjectID, created.ID, 99, 2); err != nil || empty.HasMore || len(empty.Children) != 0 {
		t.Fatal("end cursor did not return empty page", empty, err)
	}
	for _, bounds := range [][2]int{{-2, 1}, {1000, 1}, {-1, 0}, {-1, MaxSweepPageSize + 1}} {
		if _, err := GetSweep(ctx, pool, created.ProjectID, created.ID, bounds[0], bounds[1]); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid pagination bound accepted", bounds, err)
		}
	}
}

func completeSweepMetric(t *testing.T, pool *pgxpool.Pool, id WorkerIdentity, a AttemptAuthority, reason string) {
	t.Helper()
	ctx := context.Background()
	finalizeUploadFixture(t, pool, id, a)
	body := []byte(`{"score":9007199254740993}`)
	hash := sha256.Sum256(body)
	upload := uploadRequest()
	upload.Authority, upload.LogicalName, upload.SizeBytes, upload.SHA256 = a, "metrics", int64(len(body)), hex.EncodeToString(hash[:])
	pending, err := CreateUpload(ctx, pool, id, upload)
	if err != nil || pending.Upload == nil {
		t.Fatal(pending, err)
	}
	artifact, err := FinalizeUpload(ctx, pool, id, FinalizeUploadRequest{Authority: a, RequestID: uuid.NewString(), UploadID: pending.Upload.UploadID,
		Object: ArtifactObject{Key: pending.Upload.ObjectKey, Version: "metric-version", SizeBytes: upload.SizeBytes, SHA256: upload.SHA256}}, verifiedFixtureObject)
	if err != nil || artifact.Artifact == nil {
		t.Fatal(artifact, err)
	}
	r := completionRequest()
	r.Authority, r.Reason, r.MetricsJSON = a, reason, body
	r.Outputs = []CompletionOutput{{Name: "metrics", ArtifactID: artifact.Artifact.ArtifactID}}
	signCompletion(t, &r)
	if result, err := CompleteAttempt(ctx, pool, id, r); err != nil || result.Decision != "ACCEPTED" {
		t.Fatal("metric completion rejected", result, err)
	}
}
