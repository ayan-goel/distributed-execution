//go:build integration

package store

import (
	"context"
	"strings"
	"testing"
)

func TestSweepSchemaKeepsChildrenUniqueAndProjectScoped(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	project := func(name string) string {
		t.Helper()
		var id string
		if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
			VALUES($1,1000,1024,1) RETURNING id::text`, name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	owner, other := project("sweep-owner"), project("sweep-other")
	hash := strings.Repeat("a", 64)
	var sweep string
	if err := pool.QueryRow(ctx, `INSERT INTO sweeps(project_id,name,spec,spec_hash,child_count,max_concurrent,fail_fast,cancel_running_on_failure)
		VALUES($1,'grid','{}',$2,27,6,false,false) RETURNING id::text`, owner, hash).Scan(&sweep); err != nil {
		t.Fatal(err)
	}
	insert := func(projectID string, sweepID any, index any) error {
		_, err := pool.Exec(ctx, `INSERT INTO jobs(project_id,spec,spec_hash,cpu_millis,memory_mib,scratch_mib,sweep_id,sweep_index)
			VALUES($1,'{}',$2,100,128,64,$3,$4)`, projectID, hash, sweepID, index)
		return err
	}
	if err := insert(owner, sweep, 0); err != nil {
		t.Fatal("valid child rejected", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET sweep_index=1 WHERE sweep_id=$1", sweep); err == nil {
		t.Fatal("child index changed after admission")
	}
	if _, err := pool.Exec(ctx, "UPDATE sweeps SET max_concurrent=1 WHERE id=$1", sweep); err == nil {
		t.Fatal("sweep policy changed after admission")
	}
	for _, invalid := range []struct {
		project string
		sweep   any
		index   any
	}{
		{owner, sweep, 0}, // a child index cannot be reused
		{other, sweep, 1}, // a project cannot link to another project's sweep
		{owner, sweep, nil},
		{owner, nil, 1},
		{owner, sweep, 1000},
	} {
		if err := insert(invalid.project, invalid.sweep, invalid.index); err == nil {
			t.Fatal("invalid sweep child accepted", invalid)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO sweeps(project_id,name,spec,spec_hash,child_count,max_concurrent,fail_fast,cancel_running_on_failure)
		VALUES($1,'oversized','{}',$2,1001,6,false,false)`, owner, hash); err == nil {
		t.Fatal("oversized sweep admitted")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO sweeps(project_id,name,spec,spec_hash,child_count,max_concurrent,fail_fast,cancel_running_on_failure)
		VALUES($1,'bad-policy','{}',$2,27,6,false,true)`, owner, hash); err == nil {
		t.Fatal("running cancellation admitted without fail-fast")
	}
}
