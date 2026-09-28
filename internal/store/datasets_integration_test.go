//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
	"github.com/google/uuid"
)

func TestDatasetRecordsPinOneImmutableProjectOwnedVersion(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var project, foreign string
	for _, name := range []string{"dataset-owner", "dataset-foreign"} {
		var id string
		if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
			VALUES($1,1000,1024,1) RETURNING id::text`, name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if project == "" {
			project = id
		} else {
			foreign = id
		}
	}
	upload := uuid.NewString()
	request := uuid.NewString()
	hash := strings.Repeat("a", 64)
	var key string
	if err := pool.QueryRow(ctx, `INSERT INTO dataset_uploads(id,project_id,request_id,request_hash,name,size_bytes,sha256)
		VALUES($1,$2,$3,$4,'antibodies-v1',3,$4) RETURNING object_key`, upload, project, request, hash).
		Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key != "projects/"+project+"/datasets/uploads/"+upload {
		t.Fatal("dataset upload escaped its project key", key)
	}
	manifest := `{"files":[{"path":"input.txt","size":3}]}`
	if _, err := pool.Exec(ctx, `INSERT INTO datasets(project_id,name,upload_id,object_version,manifest)
		VALUES($1,'antibodies-v1',$2,'version-1',$3)`, project, upload, manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE datasets SET object_version='version-2' WHERE project_id=$1`, project); err == nil {
		t.Fatal("registered dataset version was mutable")
	}
	if _, err := pool.Exec(ctx, `UPDATE dataset_uploads SET sha256=$2 WHERE id=$1`, upload, strings.Repeat("b", 64)); err == nil {
		t.Fatal("dataset upload declaration was mutable")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO datasets(project_id,name,upload_id,object_version,manifest)
		VALUES($1,'antibodies-v1',$2,'version-1',$3)`, foreign, upload, manifest); err == nil {
		t.Fatal("another project claimed the dataset upload")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO datasets(project_id,name,upload_id,object_version,manifest)
		VALUES($1,'renamed',$2,'version-1',$3)`, project, upload, manifest); err == nil {
		t.Fatal("one upload registered under a different name")
	}
	duplicate := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO dataset_uploads(id,project_id,request_id,request_hash,name,size_bytes,sha256)
		VALUES($1,$2,$3,$4,'antibodies-v1',3,$4)`, duplicate, project, uuid.NewString(), hash); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO datasets(project_id,name,upload_id,object_version,manifest)
		VALUES($1,'antibodies-v1',$2,'version-1',$3)`, project, duplicate, manifest); err == nil {
		t.Fatal("dataset name was registered twice")
	}
	invalid := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO dataset_uploads(id,project_id,request_id,request_hash,name,size_bytes,sha256)
		VALUES($1,$2,$3,$4,'invalid-version',3,$4)`, invalid, project, uuid.NewString(), hash); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO datasets(project_id,name,upload_id,object_version,manifest)
		VALUES($1,'invalid-version',$2,'null',$3)`, project, invalid, manifest); err == nil {
		t.Fatal("dataset accepted an unversioned object")
	}
}

func TestDatasetRegistrationPinsOnlyVerifiedExactVersion(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var project string
	if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
		VALUES('dataset-register',1000,1024,1) RETURNING id::text`).Scan(&project); err != nil {
		t.Fatal(err)
	}
	declaration, err := CreateDatasetUpload(ctx, pool, DatasetUploadRequest{
		ProjectID: project, RequestID: uuid.NewString(), Name: "antibodies-v1",
		SizeBytes: 10240, SHA256: strings.Repeat("a", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := DatasetRegistrationRequest{
		ProjectID: project, UploadID: declaration.ID, Version: "version-1",
		Manifest: DatasetManifest{Format: "tar.v1", Files: []DatasetFile{{Path: "data/input.txt", SizeBytes: 3, SHA256: strings.Repeat("b", 64)}}},
	}
	var verified int
	check := func(_ context.Context, object objectstore.Object) error {
		verified++
		if object.Key != declaration.ObjectKey || object.Version != request.Version || object.Size != declaration.SizeBytes || object.SHA256 != declaration.SHA256 {
			t.Fatal("verifier received a changed object", object)
		}
		return nil
	}
	if _, err := RegisterDataset(ctx, pool, request, func(context.Context, objectstore.Object) error { return objectstore.ErrIntegrity }); !errors.Is(err, objectstore.ErrIntegrity) {
		t.Fatal("corrupt object was registered", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM datasets WHERE upload_id=$1", declaration.ID).Scan(&count); err != nil || count != 0 {
		t.Fatal("corrupt registration left a dataset", count, err)
	}
	first, err := RegisterDataset(ctx, pool, request, check)
	if err != nil || first.ID == "" || first.Name != "antibodies-v1" || first.Object.Version != "version-1" || verified != 1 || first.Replayed {
		t.Fatal("verified registration failed", first, verified, err)
	}
	replay, err := RegisterDataset(ctx, pool, request, check)
	if err != nil || replay.ID != first.ID || !replay.Replayed || verified != 1 {
		t.Fatal("same registration did not replay", replay, verified, err)
	}
	changed := request
	changed.Version = "version-2"
	if _, err := RegisterDataset(ctx, pool, changed, check); !errors.Is(err, ErrConflict) || verified != 1 {
		t.Fatal("changed version reused a registered name", err, verified)
	}
	changed = request
	changed.Manifest.Files = []DatasetFile{{Path: "data/other.txt", SizeBytes: 3, SHA256: strings.Repeat("b", 64)}}
	if _, err := RegisterDataset(ctx, pool, changed, check); !errors.Is(err, ErrConflict) || verified != 1 {
		t.Fatal("changed manifest reused a registered name", err, verified)
	}
	changed = request
	changed.Manifest.Files = append([]DatasetFile(nil), request.Manifest.Files...)
	changed.Manifest.Files[0].Path = "../escape"
	if _, err := RegisterDataset(ctx, pool, changed, check); !errors.Is(err, ErrInvalid) {
		t.Fatal("unsafe tar path was accepted", err)
	}
	second, err := CreateDatasetUpload(ctx, pool, DatasetUploadRequest{
		ProjectID: project, RequestID: uuid.NewString(), Name: "later-v1",
		SizeBytes: 10240, SHA256: strings.Repeat("c", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	changeDuringVerify := request
	changeDuringVerify.UploadID = second.ID
	verifyAndDisable := func(context.Context, objectstore.Object) error {
		deadline, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, err := pool.Exec(deadline, "UPDATE projects SET enabled=false WHERE id=$1", project)
		return err
	}
	if _, err := RegisterDataset(ctx, pool, changeDuringVerify, verifyAndDisable); !errors.Is(err, ErrDisabled) {
		t.Fatal("project disabled during verification still registered data", err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM datasets WHERE upload_id=$1", second.ID).Scan(&count); err != nil || count != 0 {
		t.Fatal("disabled project left a dataset row", count, err)
	}
}

func TestDatasetUploadDeclarationReplaysOneScopedKey(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var project string
	if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
		VALUES('dataset-upload',1000,1024,1) RETURNING id::text`).Scan(&project); err != nil {
		t.Fatal(err)
	}
	request := DatasetUploadRequest{
		ProjectID: project, RequestID: uuid.NewString(), Name: "antibodies-v1",
		SizeBytes: 3, SHA256: strings.Repeat("a", 64),
	}
	var wg sync.WaitGroup
	results := make(chan DatasetUploadRecord, 32)
	errorsSeen := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			record, err := CreateDatasetUpload(ctx, pool, request)
			results <- record
			errorsSeen <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	id := ""
	replays := 0
	for record := range results {
		if record.ID == "" || record.ProjectID != project || record.Name != request.Name || record.SHA256 != request.SHA256 || record.SizeBytes != 3 || record.ObjectKey != "projects/"+project+"/datasets/uploads/"+record.ID {
			t.Fatal("invalid dataset upload scope", record)
		}
		if id == "" {
			id = record.ID
		} else if record.ID != id {
			t.Fatal("same request allocated multiple object keys", id, record.ID)
		}
		if record.Replayed {
			replays++
		}
	}
	if replays != 31 {
		t.Fatal("concurrent allocation did not have one winner", replays)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM dataset_uploads WHERE project_id=$1 AND request_id=$2`, project, request.RequestID).Scan(&count); err != nil || count != 1 {
		t.Fatal("request inserted multiple declarations", count, err)
	}
	changed := request
	changed.SHA256 = strings.Repeat("b", 64)
	if _, err := CreateDatasetUpload(ctx, pool, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("changed replay was accepted", err)
	}
	changed = request
	changed.Name = "../escape"
	if _, err := CreateDatasetUpload(ctx, pool, changed); !errors.Is(err, ErrInvalid) {
		t.Fatal("unsafe name was accepted", err)
	}
	changed = request
	changed.SizeBytes = 64<<20 + 1
	if _, err := CreateDatasetUpload(ctx, pool, changed); !errors.Is(err, ErrInvalid) {
		t.Fatal("single-part dataset bound was bypassed", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE projects SET enabled=false WHERE id=$1", project); err != nil {
		t.Fatal(err)
	}
	newRequest := request
	newRequest.RequestID = uuid.NewString()
	if _, err := CreateDatasetUpload(ctx, pool, newRequest); !errors.Is(err, ErrDisabled) {
		t.Fatal("disabled project reserved an upload", err)
	}
}
