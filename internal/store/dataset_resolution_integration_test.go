//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/objectstore"
	"github.com/google/uuid"
)

func TestResolveDatasetNamesPinsProjectOwnedRegistrations(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var research, other string
	if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
		VALUES('resolution-research',1000,1024,1) RETURNING id::text`).Scan(&research); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
		VALUES('resolution-other',1000,1024,1) RETURNING id::text`).Scan(&other); err != nil {
		t.Fatal(err)
	}
	register := func(project, name string) DatasetRecord {
		t.Helper()
		declared, err := CreateDatasetUpload(ctx, pool, DatasetUploadRequest{ProjectID: project,
			RequestID: uuid.NewString(), Name: name, SizeBytes: 3, SHA256: strings.Repeat("a", 64)})
		if err != nil {
			t.Fatal(err)
		}
		result, err := RegisterDataset(ctx, pool, DatasetRegistrationRequest{ProjectID: project,
			UploadID: declared.ID, Version: "version-1", Manifest: DatasetManifest{Format: "tar.v1",
				Files: []DatasetFile{{Path: "data.txt", SizeBytes: 3, SHA256: strings.Repeat("b", 64)}}}},
			func(context.Context, objectstore.Object) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := register(research, "first")
	second := register(research, "second")
	_ = register(other, "private")
	bindings, err := ResolveDatasetNames(ctx, pool, research, []string{"second", "first", "second"})
	if err != nil || len(bindings) != 3 || bindings[0].ID != second.ID || bindings[1].ID != first.ID ||
		bindings[2].ID != second.ID || bindings[0].Object.Version != "version-1" ||
		bindings[1].Object.Key != first.Object.Key || bindings[1].Manifest.Files[0].Path != "data.txt" {
		t.Fatal("resolution did not pin ordered immutable registrations", bindings, err)
	}
	if _, err := ResolveDatasetNames(ctx, pool, research, []string{"private"}); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-project dataset name resolved", err)
	}
	if _, err := ResolveDatasetNames(ctx, pool, research, []string{"first", "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatal("partially registered input set resolved", err)
	}
	if _, err := ResolveDatasetNames(ctx, pool, research, []string{"../private"}); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid dataset name reached lookup", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE projects SET enabled=false WHERE id=$1", research); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveDatasetNames(ctx, pool, research, []string{"first"}); !errors.Is(err, ErrDisabled) {
		t.Fatal("disabled project resolved datasets", err)
	}
}
