//go:build integration

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

func TestSweepRetryPreservesNondefaultPolicyAndPriority(t *testing.T) {
	pool, _, _, original := failedSweepFixture(t)
	ctx := context.Background()
	var sweep spec.Sweep
	if err := json.Unmarshal(original.Spec, &sweep); err != nil {
		t.Fatal(err)
	}
	inputs, err := LoadJobInputs(ctx, pool, original.ProjectID, original.ChildIDs[0], sweep.Spec.JobTemplate.Spec.Inputs)
	if err != nil {
		t.Fatal(err)
	}
	sweep.Spec.MaxConcurrent, sweep.Spec.FailFast, sweep.Spec.CancelRunningOnFailure = 7, true, true
	sweep.Spec.JobTemplate.Spec.Priority = 3
	source, err := SubmitSweepResolved(ctx, pool, uuid.NewString(), strings.Repeat("e", 64), sweep, []DatasetBinding{inputs[0].Dataset})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range source.ChildIDs {
		if _, err := RequestCancellation(ctx, pool, source.ProjectID, id); err != nil {
			t.Fatal(err)
		}
	}
	retried, err := RetrySweep(ctx, pool, source.ProjectID, source.ID, "policy-retry")
	if err != nil || len(retried.Children) != 4 || retried.MaxConcurrent != 7 {
		t.Fatal("retry changed source concurrency policy", retried, err)
	}
	page, err := GetSweep(ctx, pool, source.ProjectID, retried.ID, -1, 100)
	if err != nil || !page.Sweep.FailFast || !page.Sweep.CancelRunningOnFailure || page.Sweep.MaxConcurrent != 7 {
		t.Fatal("retry changed inherited failure policy", page.Sweep, err)
	}
	var priorities int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE sweep_id=$1 AND priority=3 AND spec->'spec'->>'priority'='3'", retried.ID).Scan(&priorities); err != nil || priorities != 4 {
		t.Fatal("retry discarded effective source priorities", priorities, err)
	}
}

func TestSweepRetryCopiesThe1000ChildBoundaryInOneTransaction(t *testing.T) {
	pool, _, _, original := failedSweepFixture(t)
	ctx := context.Background()
	var sweep spec.Sweep
	if err := json.Unmarshal(original.Spec, &sweep); err != nil {
		t.Fatal(err)
	}
	inputs, err := LoadJobInputs(ctx, pool, original.ProjectID, original.ChildIDs[0], sweep.Spec.JobTemplate.Spec.Inputs)
	if err != nil {
		t.Fatal(err)
	}
	seeds := make([]string, spec.MaxSweepJobs)
	for index := range seeds {
		seeds[index] = fmt.Sprint(index)
	}
	sweep.Spec.Matrix = map[string][]string{"SEED": seeds}
	source, err := SubmitSweepResolved(ctx, pool, uuid.NewString(), strings.Repeat("e", 64), sweep, []DatasetBinding{inputs[0].Dataset})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range source.ChildIDs {
		if _, err := RequestCancellation(ctx, pool, source.ProjectID, id); err != nil {
			t.Fatal(err)
		}
	}
	retried, err := RetrySweep(ctx, pool, source.ProjectID, source.ID, "bounded-retry")
	if err != nil || len(retried.Children) != spec.MaxSweepJobs {
		t.Fatal("bounded retry failed to create the complete subset", len(retried.Children), err)
	}
	for index, child := range retried.Children {
		if child.Index != index || child.ParentJobID != source.ChildIDs[index] || child.ID == child.ParentJobID {
			t.Fatal("large retry lost dense order or parent identity", child)
		}
	}
	var jobs, inputsCount, events int
	err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM jobs WHERE sweep_id=$1),
		(SELECT count(*) FROM job_inputs i JOIN jobs j ON j.id=i.job_id WHERE j.sweep_id=$1),
		(SELECT count(*) FROM job_events e JOIN jobs j ON j.id=e.job_id WHERE j.sweep_id=$1)`, retried.ID).Scan(&jobs, &inputsCount, &events)
	if err != nil || jobs != spec.MaxSweepJobs || inputsCount != jobs || events != jobs {
		t.Fatal("large retry committed partial admission", jobs, inputsCount, events, err)
	}
}
