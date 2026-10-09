//go:build integration

package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestJobRetryParentsStayProjectScopedAndImmutable(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var owner, foreign string
	if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
		VALUES('retry-owner',1000,1024,2) RETURNING id::text`).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
		VALUES('retry-foreign',1000,1024,2) RETURNING id::text`).Scan(&foreign); err != nil {
		t.Fatal(err)
	}
	insert := func(id, project string, parent any) error {
		_, err := pool.Exec(ctx, `INSERT INTO jobs(id,project_id,parent_job_id,spec,spec_hash,cpu_millis,memory_mib,scratch_mib)
			VALUES($1,$2,$3,'{}',repeat('a',64),100,128,64)`, id, project, parent)
		return err
	}
	root, other, retry, next := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, job := range []struct {
		id, project string
		parent      any
	}{{root, owner, nil}, {other, foreign, nil}, {retry, owner, root}, {next, owner, retry}} {
		if err := insert(job.id, job.project, job.parent); err != nil {
			t.Fatal("valid retry chain rejected", err)
		}
	}
	requireLineageSQLState(t, insert(uuid.NewString(), owner, other), "23503")
	self := uuid.NewString()
	requireLineageSQLState(t, insert(self, owner, self), "23514")
	for _, parent := range []any{nil, next} {
		_, err := pool.Exec(ctx, "UPDATE jobs SET parent_job_id=$2 WHERE id=$1", retry, parent)
		requireLineageSQLState(t, err, "23514")
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET parent_job_id=parent_job_id WHERE id=$1", retry); err != nil {
		t.Fatal("unchanged provenance rejected", err)
	}
	var saved string
	if err := pool.QueryRow(ctx, "SELECT parent_job_id::text FROM jobs WHERE id=$1", retry).Scan(&saved); err != nil || saved != root {
		t.Fatal("original retry provenance changed", saved, err)
	}
}

func requireLineageSQLState(t *testing.T, err error, code string) {
	t.Helper()
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != code {
		t.Fatalf("wanted SQLSTATE %s, got %v", code, err)
	}
}

func TestSweepRetryParentsStayProjectScopedAndImmutable(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var owner, foreign string
	if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
		VALUES('sweep-retry-owner',1000,1024,2) RETURNING id::text`).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
		VALUES('sweep-retry-foreign',1000,1024,2) RETURNING id::text`).Scan(&foreign); err != nil {
		t.Fatal(err)
	}
	insert := func(id, project string, parent any) error {
		_, err := pool.Exec(ctx, `INSERT INTO sweeps(id,project_id,parent_sweep_id,name,spec,spec_hash,child_count,max_concurrent,fail_fast,cancel_running_on_failure)
			VALUES($1,$2,$3,'grid','{}',repeat('a',64),1,1,false,false)`, id, project, parent)
		return err
	}
	root, other, retry := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, sweep := range []struct {
		id, project string
		parent      any
	}{{root, owner, nil}, {other, foreign, nil}, {retry, owner, root}} {
		if err := insert(sweep.id, sweep.project, sweep.parent); err != nil {
			t.Fatal("valid sweep lineage rejected", err)
		}
	}
	requireLineageSQLState(t, insert(uuid.NewString(), owner, other), "23503")
	self := uuid.NewString()
	requireLineageSQLState(t, insert(self, owner, self), "23514")
	_, err := pool.Exec(ctx, "UPDATE sweeps SET parent_sweep_id=NULL WHERE id=$1", retry)
	requireLineageSQLState(t, err, "23514")
	var saved string
	if err := pool.QueryRow(ctx, "SELECT parent_sweep_id::text FROM sweeps WHERE id=$1", retry).Scan(&saved); err != nil || saved != root {
		t.Fatal("original sweep provenance changed", saved, err)
	}
}

func rollbackRetryLineage(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	rollbackQueueBlockers(t, ctx, tx)
	downCursor, err := os.ReadFile("../../migrations/0017_scheduler_cursor.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(downCursor)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0017_scheduler_cursor.up.sql'"); err != nil {
		t.Fatal(err)
	}
	down, err := os.ReadFile("../../migrations/0016_retry_lineage.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, string(down)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE name='0016_retry_lineage.up.sql'"); err != nil {
		t.Fatal(err)
	}
}

func TestRetryLineageMigrationPreservesActiveWorkAndExistingJobParents(t *testing.T) {
	pool, id, request := uploadFixture(t)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	rollbackRetryLineage(t, ctx, tx)
	var child string
	if err := tx.QueryRow(ctx, `INSERT INTO jobs(project_id,parent_job_id,spec,spec_hash,cpu_millis,memory_mib,scratch_mib)
		SELECT project_id,id,spec,spec_hash,cpu_millis,memory_mib,scratch_mib FROM jobs WHERE id=$1 RETURNING id::text`, request.Authority.JobID).Scan(&child); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var lease, phase time.Time
	if err := pool.QueryRow(ctx, "SELECT lease_expires_at,phase_deadline FROM attempts WHERE id=$1", request.Authority.AttemptID).Scan(&lease, &phase); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	result, err := CreateUpload(ctx, pool, id, request)
	if err != nil || result.Upload == nil || result.Upload.Authority != request.Authority || result.State != "FINALIZING" || !result.LeaseExpiresAt.Equal(lease) || !result.PhaseDeadline.Equal(phase) {
		t.Fatal("retry lineage migration changed active ownership or deadlines", result, err)
	}
	var parent string
	if err := pool.QueryRow(ctx, "SELECT parent_job_id::text FROM jobs WHERE id=$1", child).Scan(&parent); err != nil || parent != request.Authority.JobID {
		t.Fatal("upgrade erased existing job provenance", parent, err)
	}
	_, err = pool.Exec(ctx, "UPDATE jobs SET parent_job_id=NULL WHERE id=$1", child)
	requireLineageSQLState(t, err, "23514")
}

func TestRetryLineageMigrationRejectsInvalidLegacyParentsAtomically(t *testing.T) {
	for _, kind := range []string{"foreign", "self"} {
		t.Run(kind, func(t *testing.T) {
			pool := testPool(t)
			ctx := context.Background()
			if err := Migrate(ctx, pool); err != nil {
				t.Fatal(err)
			}
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer rollback(tx)
			rollbackRetryLineage(t, ctx, tx)
			var owner, foreign string
			for _, project := range []struct {
				name string
				id   *string
			}{{"legacy-owner", &owner}, {"legacy-foreign", &foreign}} {
				if err := tx.QueryRow(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
					VALUES($1,1000,1024,2) RETURNING id::text`, project.name).Scan(project.id); err != nil {
					t.Fatal(err)
				}
			}
			root, bad := uuid.NewString(), uuid.NewString()
			if _, err := tx.Exec(ctx, `INSERT INTO jobs(id,project_id,spec,spec_hash,cpu_millis,memory_mib,scratch_mib)
				VALUES($1,$2,'{}',repeat('a',64),100,128,64)`, root, owner); err != nil {
				t.Fatal(err)
			}
			parent, code := root, "23503"
			if kind == "self" {
				parent, code = bad, "23514"
			}
			if _, err := tx.Exec(ctx, `INSERT INTO jobs(id,project_id,parent_job_id,spec,spec_hash,cpu_millis,memory_mib,scratch_mib)
				VALUES($1,$2,$3,'{}',repeat('a',64),100,128,64)`, bad, foreign, parent); err != nil {
				t.Fatal("previous schema no longer reproduces unsafe lineage", err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			requireLineageSQLState(t, Migrate(ctx, pool), code)
			var count int
			var column bool
			var saved string
			err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM schema_migrations),
				EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='sweeps' AND column_name='parent_sweep_id'),
				(SELECT parent_job_id::text FROM jobs WHERE id=$1)`, bad).Scan(&count, &column, &saved)
			if err != nil || count != 15 || column || saved != parent {
				t.Fatal("failed upgrade changed schema history or legacy lineage", count, column, saved, err)
			}
		})
	}
}
