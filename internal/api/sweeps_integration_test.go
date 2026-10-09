//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"dispatch.local/dispatch/internal/admission"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func TestHTTPSweepSubmissionIsAtomicScopedAndReplayable(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	issue := func(project string, role store.Role) string {
		t.Helper()
		token, _, err := store.IssueToken(ctx, pool, project, role)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	token, reader, foreign := issue("research", store.RoleSubmit), issue("research", store.RoleRead), issue("other", store.RoleSubmit)
	var projectID string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM projects WHERE name='research'").Scan(&projectID); err != nil {
		t.Fatal(err)
	}
	upload, err := store.CreateDatasetUpload(ctx, pool, store.DatasetUploadRequest{ProjectID: projectID,
		RequestID: uuid.NewString(), Name: "sweep-data", SizeBytes: 4096, SHA256: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	dataset, err := store.RegisterDataset(ctx, pool, store.DatasetRegistrationRequest{ProjectID: projectID,
		UploadID: upload.ID, Version: "sweep-version", Manifest: store.DatasetManifest{Format: "tar.v1",
			Files: []store.DatasetFile{{Path: "data.txt", SizeBytes: 4, SHA256: strings.Repeat("b", 64)}}}},
		func(context.Context, objectstore.Object) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	job, err := spec.DecodeJob(bytes.NewReader(jobBody(t)))
	if err != nil {
		t.Fatal(err)
	}
	job.Spec.Inputs = []spec.Input{{Dataset: "sweep-data", MountPath: "/inputs/data"}}
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: spec.Metadata{Name: "grid", Project: "research"},
		Spec: spec.SweepSpec{JobTemplate: job, Matrix: map[string][]string{
			"METHOD": {"random", "contact", "knn"}, "RATE": {"0.0001", "0.0003", "0.001"}, "SEED": {"1", "2", "3"}}, MaxConcurrent: 6}}
	body, _ := json.Marshal(sweep)
	var down atomic.Bool
	h := New(pool, resolverFunc(func(context.Context, string) (string, error) {
		if down.Load() {
			return "", admission.ErrImageUnavailable
		}
		return "registry.example.org/eval@sha256:" + strings.Repeat("a", 64), nil
	}), nil)
	for _, tc := range []struct {
		token string
		code  int
	}{{"", 401}, {reader, 403}, {foreign, 403}} {
		if got := call(h, "POST", "/v1/sweeps", tc.token, "same", body); got.Code != tc.code {
			t.Fatal("sweep permission check", got.Code, tc.code, got.Body.String())
		}
	}
	responses := make(chan *httptest.ResponseRecorder, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); responses <- call(h, "POST", "/v1/sweeps", token, "same", body) }()
	}
	wg.Wait()
	close(responses)
	var first store.SweepRecord
	created := 0
	for response := range responses {
		if response.Code != 200 && response.Code != 201 {
			t.Fatal("sweep submission rejected", response.Code, response.Body.String())
		}
		var result store.SweepRecord
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if first.ID == "" {
			first = result
		}
		if result.ID != first.ID || len(result.ChildIDs) != 27 || !reflect.DeepEqual(result.ChildIDs, first.ChildIDs) {
			t.Fatal("sweep replay changed identity or children", result)
		}
		if response.Header().Get("Location") != "/v1/sweeps/"+result.ID {
			t.Fatal("sweep location missing", response.Header())
		}
		if response.Code == 201 {
			created++
		}
	}
	if created != 1 {
		t.Fatal("concurrent submissions did not create exactly one sweep", created)
	}
	var resolved spec.Sweep
	if err := json.Unmarshal(first.Spec, &resolved); err != nil || resolved.Spec.JobTemplate.Spec.Image != "registry.example.org/eval@sha256:"+strings.Repeat("a", 64) {
		t.Fatal("template image was not pinned", resolved, err)
	}
	var children, bindings int
	if err := pool.QueryRow(ctx, `SELECT count(*),count(i.dataset_id) FROM jobs j LEFT JOIN job_inputs i ON i.job_id=j.id AND i.dataset_id=$2 WHERE j.sweep_id=$1`, first.ID, dataset.ID).Scan(&children, &bindings); err != nil || children != 27 || bindings != 27 {
		t.Fatal("children or frozen inputs missing", children, bindings, err)
	}
	down.Store(true)
	if replay := call(h, "POST", "/v1/sweeps", token, "same", body); replay.Code != 200 {
		t.Fatal("sweep replay depended on registry", replay.Code, replay.Body.String())
	}
	sweep.Spec.MaxConcurrent++
	changed, _ := json.Marshal(sweep)
	if conflict := call(h, "POST", "/v1/sweeps", token, "same", changed); conflict.Code != 409 {
		t.Fatal("changed sweep replayed", conflict.Code, conflict.Body.String())
	}
	if unavailable := call(h, "POST", "/v1/sweeps", token, "new", body); unavailable.Code != 503 {
		t.Fatal("new sweep bypassed unavailable resolver", unavailable.Code)
	}
	down.Store(false)
	sweep.Spec.JobTemplate.Spec.Inputs[0].Dataset = "missing"
	missing, _ := json.Marshal(sweep)
	if response := call(h, "POST", "/v1/sweeps", token, "missing", missing); response.Code != 404 {
		t.Fatal("missing dataset admitted", response.Code, response.Body.String())
	}
	for _, tc := range []struct {
		key  string
		body []byte
		code int
	}{{"", body, 400}, {"malformed", []byte(`{"kind":"Sweep"}`), 422}, {"oversize", bytes.Repeat([]byte(" "), spec.MaxDocumentBytes+1), 413}} {
		if response := call(h, "POST", "/v1/sweeps", token, tc.key, tc.body); response.Code != tc.code {
			t.Fatal("invalid sweep status", response.Code, tc.code, response.Body.String())
		}
	}
	var sweeps, jobs int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM sweeps),(SELECT count(*) FROM jobs)").Scan(&sweeps, &jobs); err != nil || sweeps != 1 || jobs != 27 {
		t.Fatal("failed admission left partial sweep", sweeps, jobs, err)
	}
}
