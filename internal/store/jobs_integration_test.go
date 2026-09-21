//go:build integration

package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"dispatch.local/dispatch/internal/spec"
)

func admittedExample(t *testing.T) (spec.Job, string) {
	t.Helper()
	f, err := os.Open("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	j, err := spec.DecodeJob(f)
	if err != nil {
		t.Fatal(err)
	}
	j.Spec.Inputs = nil
	_, requestHash, err := j.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	j.Spec.Image = "registry.example.org/eval@sha256:" + strings.Repeat("a", 64)
	return j, requestHash
}

func TestConcurrentSubmissionIsOneDurableJob(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES ('research',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	j, hash := admittedExample(t)
	var wg sync.WaitGroup
	ids := make(chan string, 100)
	failures := make(chan error, 100)
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := SubmitJob(ctx, pool, "same-key", hash, j)
			if err != nil {
				failures <- err
				return
			}
			ids <- result.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if first != id {
			t.Fatal("duplicate job IDs")
		}
	}
	if first == "" {
		t.Fatal("no accepted job")
	}
	for _, table := range []string{"jobs", "idempotency_keys", "job_events"} {
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s has %d rows: %v", table, count, err)
		}
	}
	var persisted []byte
	var storedHash string
	if err := pool.QueryRow(ctx, "SELECT spec,spec_hash FROM jobs WHERE id=$1", first).Scan(&persisted, &storedHash); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(persisted), "@sha256:") {
		t.Fatal("unpinned image persisted")
	}
	canonical, _, _ := j.Canonical()
	digest := sha256.Sum256(canonical)
	if storedHash != hex.EncodeToString(digest[:]) {
		t.Fatal("execution hash does not match resolved spec")
	}
	if storedHash == hash {
		t.Fatal("request identity confused with resolved execution identity")
	}
	j.Spec.Env["SEED"] = "2"
	if _, err := SubmitJob(ctx, pool, "same-key", strings.Repeat("b", 64), j); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed payload: %v", err)
	}
	// Recovery reads the durable submission without any scheduler process memory.
	got, err := LookupSubmission(ctx, pool, "research", "same-key", hash)
	if err != nil || got.ID != first {
		t.Fatalf("lost submission response could not be recovered: %v", err)
	}
}

func TestSubmissionRollbackAndAdmissionLimits(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES ('research',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	j, hash := admittedExample(t)
	for _, mutate := range []func(*spec.Job){
		func(j *spec.Job) { j.Spec.Image = "example.org/image:latest" },
		func(j *spec.Job) { j.Spec.Resources.CPUMillis = 8000 },
		func(j *spec.Job) { j.Metadata.Project = "missing" },
	} {
		bad := j
		mutate(&bad)
		if _, err := SubmitJob(ctx, pool, "invalid", hash, bad); err == nil {
			t.Fatal("invalid admission accepted")
		}
	}
	// Fail after the idempotency key and job insert but before the event commit.
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected event failure'; END $$; CREATE TRIGGER reject_event BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION reject_event()`); err != nil {
		t.Fatal(err)
	}
	if _, err := SubmitJob(ctx, pool, "rollback", hash, j); err == nil {
		t.Fatal("injected failure accepted")
	}
	for _, table := range []string{"jobs", "idempotency_keys", "job_events"} {
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("rollback leaked %s rows: %d %v", table, count, err)
		}
	}
}
