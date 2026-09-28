//go:build integration

package store

import (
	"context"
	"strings"
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
