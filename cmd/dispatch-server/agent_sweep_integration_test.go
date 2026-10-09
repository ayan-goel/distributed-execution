//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Observe committed acquisition state without changing scheduling decisions.
// Store concurrency tests cover the transaction invariant; this records its live use.
type sweepAcquisitionProbe struct {
	pb.WorkerServiceServer
	pool *pgxpool.Pool
	id   string
	peak atomic.Int32
}

func (s *sweepAcquisitionProbe) AcquireWork(ctx context.Context, request *pb.AcquireWorkRequest) (*pb.AcquireWorkResponse, error) {
	response, err := s.WorkerServiceServer.AcquireWork(ctx, request)
	if err != nil {
		return response, err
	}
	var active int32
	err = s.pool.QueryRow(ctx, `SELECT count(*) FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE j.sweep_id=$1 AND a.state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING')`, s.id).Scan(&active)
	if err != nil {
		return nil, err
	}
	for previous := s.peak.Load(); active > previous; previous = s.peak.Load() {
		if s.peak.CompareAndSwap(previous, active) {
			break
		}
	}
	return response, nil
}

func TestWorkerDaemonsExecute27ChildSweep(t *testing.T) {
	f := newSweepDaemonFixture(t)
	ctx, pool, root := f.ctx, f.pool, f.root
	runCLI := func(args ...string) []byte { return f.runCLI(t, args...) }
	job := f.job
	// Keep work overlapping so two admitted attempts are observable with three workers.
	job.Spec.Command = []string{"sh", "-c", `sleep 2; printf '{"seed":%s,"rate":%s,"score":9007199254740993}\n' "$SEED" "$RATE" > /outputs/evaluation.json`}
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: job.Metadata, Spec: spec.SweepSpec{
		JobTemplate: job, MaxConcurrent: 2,
		Matrix: map[string][]string{"ALGORITHM": {"a", "b", "c"}, "RATE": {"0.1", "0.01", "0.001"}, "SEED": {"1", "2", "3"}},
	}}
	submitted, replay := f.submit(t, sweep), f.submit(t, sweep)
	if submitted.ID != replay.ID || len(submitted.ChildIDs) != 27 || !reflect.DeepEqual(submitted.ChildIDs, replay.ChildIDs) {
		t.Fatal("sweep replay changed child membership")
	}
	probe := &sweepAcquisitionProbe{WorkerServiceServer: workerapi.NewService(pool, store.AcquisitionPolicy{AllowSoftScratch: true}, f.objects), pool: pool, id: submitted.ID}
	f.startWorkers(t, probe)
	for {
		var page client.SweepPage
		if err := json.Unmarshal(runCLI("sweep", "get", submitted.ID, "--limit", "7", "--json"), &page); err != nil {
			t.Fatal(err)
		}
		if page.Sweep.Progress.Failed > 0 || page.Sweep.Progress.Cancelled > 0 {
			t.Fatal("sweep child did not succeed", page.Sweep.Progress)
		}
		if page.Sweep.State == "SUCCEEDED" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("live sweep timed out", page.Sweep.Progress)
		case <-time.After(200 * time.Millisecond):
		}
	}
	if probe.peak.Load() != 2 {
		t.Fatal("live sweep did not exercise its concurrency cap", probe.peak.Load())
	}
	var attempts, workers, held int
	err := pool.QueryRow(ctx, `SELECT count(*),count(DISTINCT a.worker_id),count(*) FILTER(WHERE r.state<>'released') FROM attempts a JOIN jobs j ON j.id=a.job_id JOIN reservations r ON r.attempt_id=a.id WHERE j.sweep_id=$1`, submitted.ID).Scan(&attempts, &workers, &held)
	if err != nil || attempts != 27 || workers < 2 || held != 0 {
		t.Fatal("incorrect execution or terminal release", attempts, workers, held, err)
	}
	var results client.SweepResults
	if err := json.Unmarshal(runCLI("sweep", "export", submitted.ID), &results); err != nil {
		t.Fatal(err)
	}
	if results.SweepID != submitted.ID || len(results.Children) != 27 {
		t.Fatal("incomplete live export")
	}
	for index, child := range results.Children {
		parameters := map[string]string{"ALGORITHM": []string{"a", "b", "c"}[index/9], "RATE": []string{"0.1", "0.01", "0.001"}[index/3%3], "SEED": strconv.Itoa(index%3 + 1)}
		if child.Index != index || child.ID != submitted.ChildIDs[index] || child.State != "SUCCEEDED" || child.AcceptedAttemptID == nil || !reflect.DeepEqual(child.Parameters, parameters) || len(child.Metrics) != 3 || child.Metrics["seed"].String() != parameters["SEED"] || child.Metrics["rate"].String() != parameters["RATE"] || child.Metrics["score"].String() != "9007199254740993" {
			t.Fatal("live export lost stable parameters or accepted metric precision", child)
		}
	}
	rows, err := csv.NewReader(bytes.NewReader(runCLI("sweep", "export", submitted.ID, "--format", "csv"))).ReadAll()
	if err != nil || len(rows) != 28 {
		t.Fatal("incomplete live CSV", len(rows), err)
	}
	if !reflect.DeepEqual(rows[0], []string{"index", "jobId", "state", "acceptedAttemptId", "parameters", "metric.rate", "metric.score", "metric.seed"}) {
		t.Fatal("incorrect metric columns", rows[0])
	}
	for index, row := range rows[1:] {
		child := results.Children[index]
		parameters, _ := json.Marshal(child.Parameters)
		if !reflect.DeepEqual(row, []string{strconv.Itoa(index), child.ID, child.State, *child.AcceptedAttemptID, string(parameters), child.Metrics["rate"].String(), "9007199254740993", child.Metrics["seed"].String()}) {
			t.Fatal("CSV disagrees with accepted JSON results", row)
		}
	}
	var children []client.SweepChild
	cursor := ""
	for {
		var page client.SweepPage
		args := []string{"sweep", "get", submitted.ID, "--limit", "7", "--json"}
		if cursor != "" {
			args = append(args, "--cursor", cursor)
		}
		if err := json.Unmarshal(runCLI(args...), &page); err != nil {
			t.Fatal(err)
		}
		children = append(children, page.Children...)
		if !page.HasMore {
			break
		}
		cursor = page.NextCursor
	}
	if !reflect.DeepEqual(children, results.Children) {
		t.Fatal("live pagination disagrees with export")
	}
	destination := filepath.Join(root, "downloaded-metrics.json")
	runCLI("artifacts", "download", submitted.ChildIDs[0], "metrics", "--output", destination, "--json")
	download, err := os.ReadFile(destination)
	if err != nil || string(download) != "{\"seed\":1,\"rate\":0.1,\"score\":9007199254740993}\n" {
		t.Fatal("metric artifact download changed source bytes", string(download), err)
	}
	t.Logf("completed 27 children across %d local worker identities; observed peak=%d, JSON/CSV/pagination/artifact checks passed", workers, probe.peak.Load())
}
