//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

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
