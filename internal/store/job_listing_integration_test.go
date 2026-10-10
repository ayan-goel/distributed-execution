//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
)

func TestJobListingFiltersProjectsAndTraversesTiedKeys(t *testing.T) {
	pool, _, _ := readyAcquisitionWorker(t)
	ctx := context.Background()
	var ids []string
	var project string
	for range 4 {
		job := queueAcquisitionJob(t, pool, func(j *spec.Job) {
			j.Metadata.Labels = map[string]string{"cohort": "alpha", "tier": "train", "empty": ""}
		})
		ids, project = append(ids, job.ID), job.ProjectID
	}
	other := queueAcquisitionJob(t, pool, func(j *spec.Job) { j.Metadata.Labels = map[string]string{"cohort": "alpha", "tier": "test"} })
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES('other',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	foreign := queueAcquisitionJob(t, pool, func(j *spec.Job) {
		j.Metadata.Project = "other"
		j.Metadata.Labels = map[string]string{"cohort": "alpha", "tier": "train"}
	})
	if _, err := RequestCancellation(ctx, pool, project, other.ID); err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, "UPDATE jobs SET created_at=$1", stamp); err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	slices.Reverse(ids)
	filter := JobListFilter{State: "QUEUED", Labels: map[string]string{"cohort": "alpha", "tier": "train", "empty": ""}}
	first, err := ListJobs(ctx, pool, project, filter, nil, 2)
	if err != nil || len(first.Jobs) != 2 || !first.HasMore || first.Jobs[0].ID != ids[0] || first.Jobs[1].ID != ids[1] {
		t.Fatal("incorrect filtered first page/tie ordering", first, err)
	}
	// Retention may remove a returned anchor. Position values, not a parent row
	// lookup, must keep traversal valid without exposing another project's jobs.
	if _, err := pool.Exec(ctx, "DELETE FROM job_events WHERE job_id=$1", ids[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM jobs WHERE id=$1", ids[1]); err != nil {
		t.Fatal(err)
	}
	newer := queueAcquisitionJob(t, pool, func(j *spec.Job) { j.Metadata.Labels = filter.Labels })
	second, err := ListJobs(ctx, pool, project, filter, first.Next, 100)
	if err != nil || len(second.Jobs) != 2 || second.HasMore || second.Jobs[0].ID != ids[2] || second.Jobs[1].ID != ids[3] {
		t.Fatal("continuation skipped/duplicated tied keys after deletion/insertion", second, err)
	}
	fresh, err := ListJobs(ctx, pool, project, filter, nil, 1)
	if err != nil || len(fresh.Jobs) != 1 || fresh.Jobs[0].ID != newer.ID {
		t.Fatal("fresh listing missed new submission", fresh, err)
	}
	cancelled, err := ListJobs(ctx, pool, project, JobListFilter{State: "CANCELLED"}, nil, 100)
	if err != nil || len(cancelled.Jobs) != 1 || cancelled.Jobs[0].ID != other.ID {
		t.Fatal("state filter mismatch", cancelled, err)
	}
	page, err := ListJobs(ctx, pool, foreign.ProjectID, JobListFilter{}, nil, 100)
	if err != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != foreign.ID || page.Jobs[0].Labels["tier"] != "train" {
		t.Fatal("project isolation or summary labels changed", page, err)
	}
	empty, err := ListJobs(ctx, pool, project, JobListFilter{Labels: map[string]string{"absent": ""}}, nil, 100)
	if err != nil || empty.Jobs == nil || len(empty.Jobs) != 0 || empty.HasMore || empty.Next != nil {
		t.Fatal("missing key incorrectly matched empty value", empty, err)
	}
}

func TestJobListingBoundsBytesWithoutSkippingWideMetadata(t *testing.T) {
	pool, _, _ := readyAcquisitionWorker(t)
	ctx := context.Background()
	labels := make(map[string]string, 128)
	for i := range 128 {
		labels[fmt.Sprintf("key%d", i)] = strings.Repeat("<", 8192)
	}
	var project string
	for range 4 {
		job := queueAcquisitionJob(t, pool, func(j *spec.Job) { j.Metadata.Labels = labels })
		project = job.ProjectID
	}
	seen := map[string]bool{}
	var after *JobListPosition
	for round := range 4 {
		page, err := ListJobs(ctx, pool, project, JobListFilter{}, after, 100)
		if err != nil || len(page.Jobs) != 1 || page.HasMore != (round < 3) {
			t.Fatal("byte-bound page did not preserve progress", len(page.Jobs), page.HasMore, err)
		}
		body, err := json.Marshal(page)
		if err != nil || len(body) > MaxJobPageBytes {
			t.Fatal("page exceeded encoded byte bound", len(body), err)
		}
		for _, job := range page.Jobs {
			if seen[job.ID] || len(job.Labels) != 128 {
				t.Fatal("wide job skipped/duplicated or truncated")
			}
			seen[job.ID] = true
		}
		after = page.Next
	}
	if len(seen) != 4 {
		t.Fatal("wide metadata traversal incomplete", len(seen))
	}
}

func TestJobListingRejectsInvalidBoundariesBeforeDatabaseAccess(t *testing.T) {
	ctx := context.Background()
	project := "00000000-0000-0000-0000-000000000001"
	for _, id := range []string{"bad", "00000000-0000-0000-0000-000000000000"} {
		if _, err := ListJobs(ctx, nil, id, JobListFilter{}, nil, 1); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid project reached database", id, err)
		}
	}
	for _, limit := range []int{0, -1, 101} {
		if _, err := ListJobs(ctx, nil, project, JobListFilter{}, nil, limit); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid limit reached database", limit, err)
		}
	}
	tooMany := make(map[string]string, 129)
	for i := range 129 {
		tooMany[fmt.Sprintf("key%d", i)] = ""
	}
	for _, filter := range []JobListFilter{{State: "running"}, {Labels: tooMany}, {Labels: map[string]string{"bad key": "v"}}, {Labels: map[string]string{"key": "nul\x00"}}, {Labels: map[string]string{"key": "\xff"}}, {Labels: map[string]string{"key": strings.Repeat("x", 8193)}}} {
		if _, err := ListJobs(ctx, nil, project, filter, nil, 1); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid filter reached database", filter.State, err)
		}
	}
	for _, position := range []*JobListPosition{{ID: project}, {ID: "bad", CreatedAt: time.Now()}, {ID: project, CreatedAt: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}} {
		if _, err := ListJobs(ctx, nil, project, JobListFilter{}, position, 1); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid position reached database", err)
		}
	}
}
