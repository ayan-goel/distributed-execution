//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/admission"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func TestHTTPSubmissionPinsProjectDatasetAndReplaysWithoutResolution(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	var projectID, otherID string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM projects WHERE name='research'").Scan(&projectID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT id::text FROM projects WHERE name='other'").Scan(&otherID); err != nil {
		t.Fatal(err)
	}
	register := func(project, name string) store.DatasetRecord {
		t.Helper()
		upload, err := store.CreateDatasetUpload(ctx, pool, store.DatasetUploadRequest{
			ProjectID: project, RequestID: uuid.NewString(), Name: name,
			SizeBytes: 4096, SHA256: strings.Repeat("a", 64),
		})
		if err != nil {
			t.Fatal(err)
		}
		registered, err := store.RegisterDataset(ctx, pool, store.DatasetRegistrationRequest{
			ProjectID: project, UploadID: upload.ID, Version: "version-1",
			Manifest: store.DatasetManifest{Format: "tar.v1", Files: []store.DatasetFile{{
				Path: "data.txt", SizeBytes: 4, SHA256: strings.Repeat("b", 64),
			}}},
		}, func(context.Context, objectstore.Object) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		return registered
	}
	registered := register(projectID, "public-data")
	_ = register(otherID, "private-data")
	token, _, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	resolverDown := false
	h := New(pool, resolverFunc(func(context.Context, string) (string, error) {
		if resolverDown {
			return "", admission.ErrImageUnavailable
		}
		return "registry.example.org/eval@sha256:" + strings.Repeat("a", 64), nil
	}), nil)
	makeBody := func(name string) []byte {
		t.Helper()
		job, err := spec.DecodeJob(bytes.NewReader(jobBody(t)))
		if err != nil {
			t.Fatal(err)
		}
		job.Spec.Inputs = []spec.Input{{Dataset: name, MountPath: "/inputs/data"}}
		body, _, err := job.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	body := makeBody("public-data")
	response := call(h, "POST", "/v1/jobs", token, "dataset-job", body)
	if response.Code != 201 {
		t.Fatal(response.Code, response.Body.String())
	}
	var job store.JobRecord
	if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	bindings, err := store.LoadJobInputs(ctx, pool, projectID, job.ID,
		[]spec.Input{{Dataset: "public-data", MountPath: "/inputs/data"}})
	if err != nil || len(bindings) != 1 || bindings[0].Dataset.ID != registered.ID ||
		bindings[0].Dataset.Object.Version != "version-1" {
		t.Fatal("submission did not pin dataset", bindings, err)
	}
	resolverDown = true
	if replay := call(h, "POST", "/v1/jobs", token, "dataset-job", body); replay.Code != 200 {
		t.Fatal("committed replay required resolver", replay.Code, replay.Body.String())
	}
	resolverDown = false
	for _, name := range []string{"private-data", "missing-data"} {
		if denied := call(h, "POST", "/v1/jobs", token, name, makeBody(name)); denied.Code != 404 {
			t.Fatal("unowned dataset admitted", name, denied.Code, denied.Body.String())
		}
	}
}
