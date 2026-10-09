//go:build integration

package store

import (
	"context"
	"os"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func rollbackJobInputs(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	rollbackRetryLineage(t, ctx, tx)
	sweepDown, err := os.ReadFile("../../migrations/0015_sweeps.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(sweepDown)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0015_sweeps.up.sql'"); err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../migrations/0014_job_inputs.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0014_job_inputs.up.sql'"); err != nil {
		t.Fatal(err)
	}
}

func TestJobInputBindingsStayImmutableAndProjectScoped(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	project := func(name string) string {
		t.Helper()
		var id string
		if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
			VALUES($1,4000,8192,4) RETURNING id::text`, name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	research := project("bound-research")
	other := project("bound-other")
	register := func(projectID, name string) string {
		t.Helper()
		upload, err := CreateDatasetUpload(ctx, pool, DatasetUploadRequest{ProjectID: projectID,
			RequestID: uuid.NewString(), Name: name, SizeBytes: 3, SHA256: strings.Repeat("a", 64)})
		if err != nil {
			t.Fatal(err)
		}
		record, err := RegisterDataset(ctx, pool, DatasetRegistrationRequest{ProjectID: projectID,
			UploadID: upload.ID, Version: "version-1", Manifest: DatasetManifest{Format: "tar.v1",
				Files: []DatasetFile{{Path: "data.txt", SizeBytes: 3, SHA256: strings.Repeat("b", 64)}}}},
			func(context.Context, objectstore.Object) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		return record.ID
	}
	owned := register(research, "owned")
	foreign := register(other, "foreign")
	job, hash := admittedExample(t)
	job.Metadata.Project = "bound-research"
	record, err := SubmitJob(ctx, pool, uuid.NewString(), hash, job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO job_inputs(job_id,project_id,position,dataset_id,mount_path)
		VALUES($1,$2,0,$3,'/inputs/data')`, record.ID, research, owned); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO job_inputs(job_id,project_id,position,dataset_id,mount_path)
		VALUES($1,$2,1,$3,'/inputs/foreign')`, record.ID, research, foreign); err == nil {
		t.Fatal("cross-project dataset binding committed")
	}
	if _, err := pool.Exec(ctx, `UPDATE job_inputs SET mount_path='/inputs/other' WHERE job_id=$1`, record.ID); err == nil {
		t.Fatal("job input binding changed after admission")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM job_inputs WHERE job_id=$1`, record.ID); err == nil {
		t.Fatal("job input binding was deleted")
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_inputs WHERE job_id=$1", record.ID).Scan(&count); err != nil || count != 1 {
		t.Fatal("binding changed", count, err)
	}
}

func TestSubmissionCommitsResolvedInputsWithTheJob(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var projectID string
	if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
		VALUES('resolved-inputs',4000,8192,4) RETURNING id::text`).Scan(&projectID); err != nil {
		t.Fatal(err)
	}
	upload, err := CreateDatasetUpload(ctx, pool, DatasetUploadRequest{ProjectID: projectID,
		RequestID: uuid.NewString(), Name: "one", SizeBytes: 3, SHA256: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	registered, err := RegisterDataset(ctx, pool, DatasetRegistrationRequest{ProjectID: projectID,
		UploadID: upload.ID, Version: "version-1", Manifest: DatasetManifest{Format: "tar.v1",
			Files: []DatasetFile{{Path: "data.txt", SizeBytes: 3, SHA256: strings.Repeat("b", 64)}}}},
		func(context.Context, objectstore.Object) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := ResolveDatasetNames(ctx, pool, projectID, []string{"one"})
	if err != nil {
		t.Fatal(err)
	}
	job, _ := admittedExample(t)
	job.Metadata.Project = "resolved-inputs"
	job.Spec.Inputs = []spec.Input{{Dataset: "one", MountPath: "/inputs/one"}}
	key, hash := uuid.NewString(), strings.Repeat("c", 64)
	result, err := SubmitJobResolved(ctx, pool, key, hash, job, bindings)
	if err != nil {
		t.Fatal(err)
	}
	var datasetID, mount string
	if err := pool.QueryRow(ctx, "SELECT dataset_id::text,mount_path FROM job_inputs WHERE job_id=$1 AND position=0", result.ID).Scan(&datasetID, &mount); err != nil || datasetID != registered.ID || mount != "/inputs/one" {
		t.Fatal("submission did not bind the registered version", datasetID, mount, err)
	}
	loaded, err := LoadJobInputs(ctx, pool, projectID, result.ID, job.Spec.Inputs)
	if err != nil || len(loaded) != 1 || loaded[0].Dataset.ID != registered.ID ||
		loaded[0].Dataset.Object.Version != "version-1" || loaded[0].Dataset.Manifest.Files[0].Path != "data.txt" ||
		loaded[0].MountPath != "/inputs/one" {
		t.Fatal("durable input binding could not be replayed", loaded, err)
	}
	if _, err := LoadJobInputs(ctx, pool, uuid.NewString(), result.ID, job.Spec.Inputs); err == nil {
		t.Fatal("foreign project read a job input")
	}
	if _, err := LoadJobInputs(ctx, pool, projectID, result.ID,
		[]spec.Input{{Dataset: "one", MountPath: "/inputs/changed"}}); err == nil {
		t.Fatal("stored mount disagreed with job specification")
	}
	replay, err := SubmitJobResolved(ctx, pool, key, hash, job, bindings)
	if err != nil || !replay.Replayed || replay.ID != result.ID {
		t.Fatal("submission replay changed binding", replay, err)
	}
	bad := bindings[0]
	bad.ID = uuid.NewString()
	if _, err := SubmitJobResolved(ctx, pool, uuid.NewString(), hash, job, []DatasetBinding{bad}); err == nil {
		t.Fatal("unregistered input admitted")
	}
	var jobs, inputs int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&jobs); err != nil || jobs != 1 {
		t.Fatal("failed binding leaked a job", jobs, err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_inputs").Scan(&inputs); err != nil || inputs != 1 {
		t.Fatal("failed binding leaked an input", inputs, err)
	}
}
