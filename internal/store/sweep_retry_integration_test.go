//go:build integration

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func failedSweepFixture(t *testing.T) (*pgxpool.Pool, WorkerIdentity, Registration, SweepRecord) {
	t.Helper()
	pool, worker, session := readyAcquisitionWorker(t)
	ctx := context.Background()
	var project string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM projects WHERE name='research'").Scan(&project); err != nil {
		t.Fatal(err)
	}
	upload, err := CreateDatasetUpload(ctx, pool, DatasetUploadRequest{ProjectID: project, RequestID: uuid.NewString(), Name: "retry-input", SizeBytes: 4096, SHA256: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	dataset, err := RegisterDataset(ctx, pool, DatasetRegistrationRequest{ProjectID: project, UploadID: upload.ID, Version: "frozen-version", Manifest: DatasetManifest{Format: "tar.v1", Files: []DatasetFile{{Path: "input.txt", SizeBytes: 3, SHA256: strings.Repeat("b", 64)}}}}, func(context.Context, objectstore.Object) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	job, _ := admittedExample(t)
	job.Spec.Placement.Labels["architecture"] = "arm64"
	job.Spec.Inputs = []spec.Input{{Dataset: "retry-input", MountPath: "/inputs/data"}}
	job.Spec.Outputs = []spec.Output{{Name: "metrics", Path: "/outputs/metrics.json", Required: true, MaxBytes: MaxCompletionMetricsBytes}}
	job.Spec.Retry.MaxAttempts = 1
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: job.Metadata, Spec: spec.SweepSpec{
		JobTemplate: job, Matrix: map[string][]string{"METHOD": {"a", "b"}, "SEED": {"1", "2"}}, MaxConcurrent: 2,
	}}
	created, err := SubmitSweepResolved(ctx, pool, uuid.NewString(), strings.Repeat("f", 64), sweep, []DatasetBinding{{ID: dataset.ID, Name: dataset.Name}})
	if err != nil {
		t.Fatal(err)
	}
	for index, reason := range []string{"", "OUTPUT_INVALID"} {
		acquired, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: session.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
		if err != nil || acquired.Assignment == nil || acquired.Assignment.Authority.JobID != created.ChildIDs[index] {
			t.Fatal("source sweep did not acquire the expected child", acquired, err)
		}
		completeSweepMetric(t, pool, worker, acquired.Assignment.Authority, reason, []byte(`{"score":9007199254740993}`))
	}
	if _, err := RequestCancellation(ctx, pool, created.ProjectID, created.ChildIDs[2]); err != nil {
		t.Fatal(err)
	}
	last, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: session.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || last.Assignment == nil || last.Assignment.Authority.JobID != created.ChildIDs[3] {
		t.Fatal("source sweep did not acquire its final successful child", last, err)
	}
	completeSweepMetric(t, pool, worker, last.Assignment.Authority, "", []byte(`{"score":9007199254740993}`))
	return pool, worker, session, created
}

func TestSweepRetryCreatesOnlyFailedCancelledChildrenWithFrozenInputs(t *testing.T) {
	pool, _, _, source := failedSweepFixture(t)
	ctx := context.Background()
	var before, after []byte
	snapshot := `SELECT jsonb_agg(to_jsonb(j) ORDER BY sweep_index) FROM jobs j WHERE sweep_id=$1`
	if err := pool.QueryRow(ctx, snapshot, source.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	retried, err := RetrySweep(ctx, pool, source.ProjectID, source.ID, "retry-failed")
	if err != nil || retried.ID == source.ID || retried.ParentSweepID != source.ID || retried.ProjectID != source.ProjectID || len(retried.Children) != 2 || retried.Replayed {
		t.Fatal("retry did not create a linked subset", retried, err)
	}
	for index, child := range retried.Children {
		if child.Index != index || child.ParentJobID != source.ChildIDs[index+1] || child.ID == child.ParentJobID {
			t.Fatal("retry lost source ordering or fresh identity", child)
		}
		var matches bool
		err := pool.QueryRow(ctx, `SELECT n.state='QUEUED' AND n.attempt_counter=0 AND n.current_attempt_id IS NULL AND n.accepted_attempt_id IS NULL
			AND NOT n.cancel_requested AND n.spec=p.spec AND n.spec_hash=p.spec_hash
			AND n.cpu_millis=p.cpu_millis AND n.memory_mib=p.memory_mib AND n.scratch_mib=p.scratch_mib
			AND i.dataset_id=pi.dataset_id AND i.mount_path=pi.mount_path AND i.position=pi.position
			FROM jobs n JOIN jobs p ON p.id=n.parent_job_id JOIN job_inputs i ON i.job_id=n.id JOIN job_inputs pi ON pi.job_id=p.id WHERE n.id=$1`, child.ID).Scan(&matches)
		if err != nil || !matches {
			t.Fatal("retry changed frozen spec/input or inherited terminal execution state", matches, err)
		}
	}
	page, err := GetSweep(ctx, pool, retried.ProjectID, retried.ID, -1, 100)
	if err != nil || page.Sweep.State != "QUEUED" || page.Sweep.Progress.Total != 2 || page.Sweep.Progress.Queued != 2 || len(page.Children) != 2 || page.Children[0].Parameters["SEED"] != "2" || page.Children[1].Parameters["SEED"] != "1" || page.Children[0].Parameters["METHOD"] != "a" || page.Children[1].Parameters["METHOD"] != "b" {
		t.Fatal("retry subset did not support ordinary sweep progress", page, err)
	}
	var body []byte
	if err := pool.QueryRow(ctx, "SELECT spec FROM sweeps WHERE id=$1", retried.ID).Scan(&body); err != nil {
		t.Fatal(err)
	}
	var stored struct {
		Retry struct {
			ParentSweepID string   `json:"parentSweepId"`
			ParentJobIDs  []string `json:"parentJobIds"`
		} `json:"retry"`
	}
	if json.Unmarshal(body, &stored) != nil || stored.Retry.ParentSweepID != source.ID || !reflect.DeepEqual(stored.Retry.ParentJobIDs, source.ChildIDs[1:3]) || retried.SpecHash == source.SpecHash {
		t.Fatal("retry declaration did not bind its nonrectangular selection", stored)
	}
	if err := pool.QueryRow(ctx, snapshot, source.ID).Scan(&after); err != nil || !bytes.Equal(before, after) {
		t.Fatal("retry changed original successful/failed/cancelled jobs", err)
	}
}

func TestSweepRetryConcurrentReplayKeepsOneIdentityAndWorksAfterQuotaChanges(t *testing.T) {
	pool, _, _, source := failedSweepFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan SweepRetryRecord, 8)
	failures := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := RetrySweep(ctx, pool, source.ProjectID, source.ID, "same-retry")
			results <- r
			failures <- err
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first SweepRetryRecord
	created := 0
	for result := range results {
		if !result.Replayed {
			created++
		}
		result.Replayed = false
		if first.ID == "" {
			first = result
		}
		if !reflect.DeepEqual(first, result) {
			t.Fatal("concurrent retry changed membership, hash, or creation time", first, result)
		}
	}
	if created != 1 {
		t.Fatal("concurrent callers did not create exactly one retry", created)
	}
	if _, err := pool.Exec(ctx, "UPDATE projects SET enabled=false,cpu_quota=1,memory_quota_mib=1 WHERE id=$1", source.ProjectID); err != nil {
		t.Fatal(err)
	}
	replay, err := RetrySweep(ctx, pool, source.ProjectID, source.ID, "same-retry")
	if err != nil || !replay.Replayed || replay.ID != first.ID || !reflect.DeepEqual(replay.Children, first.Children) {
		t.Fatal("durable replay depended on changed admission policy", replay, err)
	}
	var sweeps, jobs, keys, inputs, events int
	err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM sweeps),(SELECT count(*) FROM jobs),
		(SELECT count(*) FROM idempotency_keys WHERE endpoint LIKE '%/retry'),
		(SELECT count(*) FROM job_inputs i JOIN jobs j ON j.id=i.job_id WHERE j.sweep_id=$1),
		(SELECT count(*) FROM job_events e JOIN jobs j ON j.id=e.job_id WHERE j.sweep_id=$1)`, first.ID).Scan(&sweeps, &jobs, &keys, &inputs, &events)
	if err != nil || sweeps != 2 || jobs != 6 || keys != 1 || inputs != 2 || events != 2 {
		t.Fatal("replay duplicated rows or omitted frozen inputs/events", sweeps, jobs, keys, inputs, events, err)
	}
}

func TestSweepRetryRequiresTerminalSourceAndPreservesImmediateParentChains(t *testing.T) {
	pool, worker, session, source := failedSweepFixture(t)
	ctx := context.Background()
	first, err := RetrySweep(ctx, pool, source.ProjectID, source.ID, "first-retry")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RetrySweep(ctx, pool, first.ProjectID, first.ID, "still-queued"); !errors.Is(err, ErrConflict) {
		t.Fatal("unfinished source admitted a retry", err)
	}
	if _, err := RequestCancellation(ctx, pool, first.ProjectID, first.Children[0].ID); err != nil {
		t.Fatal(err)
	}
	acquired, err := AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: session.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || acquired.Assignment == nil || acquired.Assignment.Authority.JobID != first.Children[1].ID {
		t.Fatal("retry did not enter ordinary acquisition", acquired, err)
	}
	if _, err := RetrySweep(ctx, pool, first.ProjectID, first.ID, "still-active"); !errors.Is(err, ErrConflict) {
		t.Fatal("active source admitted a partial retry", err)
	}
	completeSweepMetric(t, pool, worker, acquired.Assignment.Authority, "", []byte(`{"score":9007199254740993}`))
	second, err := RetrySweep(ctx, pool, first.ProjectID, first.ID, "second-retry")
	if err != nil || second.ParentSweepID != first.ID || len(second.Children) != 1 || second.Children[0].ParentJobID != first.Children[0].ID || second.Children[0].Index != 0 || second.SpecHash == first.SpecHash {
		t.Fatal("retry chain lost its immediate parent or repeated a success", second, err)
	}
	acquired, err = AcquireWork(ctx, pool, worker, AcquisitionRequest{SessionID: session.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
	if err != nil || acquired.Assignment == nil || acquired.Assignment.Authority.JobID != second.Children[0].ID || acquired.Assignment.Authority.Generation != 1 {
		t.Fatal("new retry job inherited old attempt numbering", acquired, err)
	}
	completeSweepMetric(t, pool, worker, acquired.Assignment.Authority, "", []byte(`{"score":9007199254740993}`))
	if _, err := RetrySweep(ctx, pool, second.ProjectID, second.ID, "all-succeeded"); !errors.Is(err, ErrConflict) {
		t.Fatal("successful source repeated completed work", err)
	}
	var keys int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM idempotency_keys WHERE endpoint LIKE '%/retry'").Scan(&keys); err != nil || keys != 2 {
		t.Fatal("rejected source state leaked an idempotency claim", keys, err)
	}
}

func TestSweepRetryRejectsForeignInvalidAndOverQuotaRequests(t *testing.T) {
	pool, _, _, source := failedSweepFixture(t)
	ctx := context.Background()
	for _, request := range []struct {
		project, sweep, key string
		want                error
	}{
		{uuid.NewString(), source.ID, "foreign", ErrNotFound},
		{source.ProjectID, uuid.NewString(), "missing", ErrNotFound},
		{source.ProjectID, source.ID, "", ErrInvalid},
		{source.ProjectID, source.ID, strings.Repeat("x", 129), ErrInvalid},
		{source.ProjectID, "bad", "invalid", ErrInvalid},
	} {
		if _, err := RetrySweep(ctx, pool, request.project, request.sweep, request.key); !errors.Is(err, request.want) {
			t.Fatal("invalid/foreign retry returned the wrong error", request, err)
		}
	}
	for _, policy := range []struct {
		sql  string
		want error
	}{
		{"UPDATE projects SET enabled=false", ErrDisabled},
		{"UPDATE projects SET enabled=true,cpu_quota=1", ErrQuota},
		{"UPDATE projects SET cpu_quota=4000,memory_quota_mib=1", ErrQuota},
	} {
		if _, err := pool.Exec(ctx, policy.sql); err != nil {
			t.Fatal(err)
		}
		if _, err := RetrySweep(ctx, pool, source.ProjectID, source.ID, "rejected"); !errors.Is(err, policy.want) {
			t.Fatal("retry bypassed admission policy", policy.want, err)
		}
	}
	var sweeps, jobs, keys int
	err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM sweeps),(SELECT count(*) FROM jobs),
		(SELECT count(*) FROM idempotency_keys WHERE endpoint LIKE '%/retry')`).Scan(&sweeps, &jobs, &keys)
	if err != nil || sweeps != 1 || jobs != 4 || keys != 0 {
		t.Fatal("rejected retries left partial work", sweeps, jobs, keys, err)
	}
}

func TestSweepRetryRollsBackEveryWriteStageAndReusesTheKey(t *testing.T) {
	for _, table := range []string{"sweeps", "jobs", "job_inputs", "job_events"} {
		t.Run(table, func(t *testing.T) {
			pool, _, _, source := failedSweepFixture(t)
			ctx := context.Background()
			snapshot := func() []int64 {
				t.Helper()
				var counts []int64
				if err := pool.QueryRow(ctx, `SELECT ARRAY[(SELECT count(*) FROM sweeps),(SELECT count(*) FROM jobs),
					(SELECT count(*) FROM job_inputs),(SELECT count(*) FROM job_events),(SELECT count(*) FROM idempotency_keys),
					(SELECT count(*) FROM attempts),(SELECT count(*) FROM attempt_completions)]`).Scan(&counts); err != nil {
					t.Fatal(err)
				}
				return counts
			}
			before := snapshot()
			if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_retry_write() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'fixture rejects retry write' USING ERRCODE='XX000'; END $$`); err != nil {
				t.Fatal(err)
			}
			// Only static fixture table names enter this SQL; injection occurs after
			// the previous stages so partial admission cannot hide behind validation.
			if _, err := pool.Exec(ctx, "CREATE TRIGGER reject_retry_write BEFORE INSERT ON "+table+" FOR EACH ROW EXECUTE FUNCTION reject_retry_write()"); err != nil {
				t.Fatal(err)
			}
			_, err := RetrySweep(ctx, pool, source.ProjectID, source.ID, "retry-after-rollback")
			var pgerr *pgconn.PgError
			if !errors.As(err, &pgerr) || pgerr.Code != "XX000" {
				t.Fatal("retry never reached the injected write stage", table, err)
			}
			if after := snapshot(); !reflect.DeepEqual(before, after) {
				t.Fatal("failed retry leaked rows or changed history", before, after)
			}
			if _, err := pool.Exec(ctx, "DROP TRIGGER reject_retry_write ON "+table); err != nil {
				t.Fatal(err)
			}
			retried, err := RetrySweep(ctx, pool, source.ProjectID, source.ID, "retry-after-rollback")
			if err != nil || retried.Replayed || len(retried.Children) != 2 {
				t.Fatal("rollback poisoned the request recovery key", retried, err)
			}
			var linkedEvents int
			err = pool.QueryRow(ctx, `SELECT count(*) FROM job_events e JOIN jobs j ON j.id=e.job_id
				WHERE j.sweep_id=$1 AND e.sequence=1 AND e.type='SUBMITTED' AND e.payload->>'parentJobId'=j.parent_job_id::text
				AND e.payload->>'parentSweepId'=$2 AND e.payload->>'specHash'=j.spec_hash`, retried.ID, source.ID).Scan(&linkedEvents)
			if err != nil || linkedEvents != 2 {
				t.Fatal("retry submission events lost lineage or frozen spec identity", linkedEvents, err)
			}
		})
	}
}
