//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"dispatch.local/dispatch/internal/spec"
)

func TestSweepSubmissionCommitsStableChildrenAndReplaysOneIdentity(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota)
		VALUES('research',4000,8192,4)`); err != nil {
		t.Fatal(err)
	}
	job, _ := admittedExample(t)
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep",
		Metadata: spec.Metadata{Name: "grid", Project: "research"},
		Spec: spec.SweepSpec{JobTemplate: job, Matrix: map[string][]string{
			"METHOD": {"random", "contact", "knn"},
			"RATE":   {"0.0001", "0.0003", "0.001"},
			"SEED":   {"1", "2", "3"},
		}, MaxConcurrent: 6}}
	hash := strings.Repeat("f", 64)
	results := make(chan SweepRecord, 8)
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			record, err := SubmitSweepResolved(ctx, pool, "same-sweep", hash, sweep, nil)
			results <- record
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first SweepRecord
	for result := range results {
		if first.ID == "" {
			first = result
		}
		if result.ID != first.ID || len(result.ChildIDs) != 27 {
			t.Fatal("duplicate or incomplete sweep", result)
		}
		for i, id := range result.ChildIDs {
			if id != first.ChildIDs[i] {
				t.Fatal("replay changed child order", i)
			}
		}
	}
	var count, events, unique int
	if err := pool.QueryRow(ctx, `SELECT count(*),count(DISTINCT sweep_index) FROM jobs WHERE sweep_id=$1`, first.ID).Scan(&count, &unique); err != nil || count != 27 || unique != 27 {
		t.Fatal("child transaction was partial", count, unique, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM job_events e JOIN jobs j ON j.id=e.job_id WHERE j.sweep_id=$1`, first.ID).Scan(&events); err != nil || events != 27 {
		t.Fatal("child events missing", events, err)
	}
	for i, id := range first.ChildIDs {
		var child spec.Job
		var body []byte
		if err := pool.QueryRow(ctx, "SELECT spec FROM jobs WHERE id=$1 AND sweep_index=$2", id, i).Scan(&body); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body, &child); err != nil {
			t.Fatal(err)
		}
		if child.Spec.Env["METHOD"] != sweep.Spec.Matrix["METHOD"][i/9] ||
			child.Spec.Env["RATE"] != sweep.Spec.Matrix["RATE"][(i/3)%3] ||
			child.Spec.Env["SEED"] != sweep.Spec.Matrix["SEED"][i%3] {
			t.Fatal("unstable matrix position", i, child.Spec.Env)
		}
	}
	if _, err := LookupSweepSubmission(ctx, pool, "research", "same-sweep", strings.Repeat("e", 64)); !errors.Is(err, ErrConflict) {
		t.Fatal("changed request replayed", err)
	}
	tooLarge := sweep
	tooLarge.Spec.JobTemplate.Spec.Resources.CPUMillis = 5000
	if _, err := SubmitSweepResolved(ctx, pool, "over-quota", hash, tooLarge, nil); !errors.Is(err, ErrQuota) {
		t.Fatal("over-quota sweep admitted", err)
	}
	var leaked int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys WHERE endpoint='/v1/sweeps' AND key='over-quota'`).Scan(&leaked); err != nil || leaked != 0 {
		t.Fatal("failed sweep left a request claim", leaked, err)
	}
}
