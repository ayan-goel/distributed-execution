//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type sweepFailureGate struct {
	pb.WorkerServiceServer
	pool                         *pgxpool.Pool
	sweepID, failedID, siblingID string
	cancelRunning                bool
	running, committed           chan struct{}
	runningOnce                  sync.Once
	calls                        atomic.Int32
	mu                           sync.Mutex
	original                     *pb.CompleteAttemptRequest
	evidenceErr                  error
}

func (s *sweepFailureGate) ReportPhase(ctx context.Context, request *pb.ReportPhaseRequest) (*pb.MutationResponse, error) {
	reply, err := s.WorkerServiceServer.ReportPhase(ctx, request)
	if err == nil && reply.GetDecision() == pb.Decision_ACCEPTED && request.GetPhase() == pb.AttemptState_RUNNING && request.GetAuthority().GetJobId() == s.siblingID {
		s.runningOnce.Do(func() { close(s.running) })
	}
	return reply, err
}

func (s *sweepFailureGate) CompleteAttempt(ctx context.Context, request *pb.CompleteAttemptRequest) (*pb.CompleteAttemptResponse, error) {
	id := request.GetAuthority().GetJobId()
	if id == s.siblingID {
		// Hold terminal acknowledgement until the post-failure snapshot proves
		// capacity is still reserved; renewal and physical stop remain independent.
		select {
		case <-s.committed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if id != s.failedID {
		return s.WorkerServiceServer.CompleteAttempt(ctx, request)
	}
	// Force a live running sibling, rather than letting startup timing decide
	// which fail-fast policy this test actually exercises.
	select {
	case <-s.running:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	reply, err := s.WorkerServiceServer.CompleteAttempt(ctx, request)
	if err != nil {
		return reply, err
	}
	s.mu.Lock()
	first := s.original == nil
	if first {
		s.original = proto.Clone(request).(*pb.CompleteAttemptRequest)
		s.evidenceErr = s.snapshot(ctx)
		close(s.committed)
	} else if !proto.Equal(s.original, request) {
		s.evidenceErr = fmt.Errorf("failure replay changed the sealed completion request")
	}
	if reply.GetDecision() != pb.Decision_ACCEPTED || reply.GetState() != pb.AttemptState_FAILED {
		s.evidenceErr = fmt.Errorf("failure completion/replay was not accepted: %s/%s", reply.GetDecision(), reply.GetState())
	}
	// Publish progress only after replay comparison, so the polling test cannot
	// finish before the handler records a changed request or rejected replay.
	s.calls.Add(1)
	s.mu.Unlock()
	if first {
		// The real failure and sibling cancellations are already committed.
		// Losing only the reply tests durable replay without repeating events.
		return nil, status.Error(codes.Unavailable, "fixture lost failure completion reply")
	}
	return reply, nil
}

func (s *sweepFailureGate) snapshot(ctx context.Context) error {
	var jobState, attemptState, reservation, container string
	var failed, cancelled, attempts int
	err := s.pool.QueryRow(ctx, `SELECT j.state,a.state,r.state,a.container_id,
		(SELECT count(*) FROM jobs WHERE sweep_id=$2 AND state='FAILED'),
		(SELECT count(*) FROM jobs WHERE sweep_id=$2 AND state='CANCELLED'),
		(SELECT count(*) FROM attempts x JOIN jobs y ON y.id=x.job_id WHERE y.sweep_id=$2)
		FROM jobs j JOIN attempts a ON a.id=j.current_attempt_id JOIN reservations r ON r.attempt_id=a.id WHERE j.id=$1`, s.siblingID, s.sweepID).Scan(&jobState, &attemptState, &reservation, &container, &failed, &cancelled, &attempts)
	if err != nil {
		return err
	}
	want := "ACTIVE"
	if s.cancelRunning {
		want = "CANCELLING"
	}
	if jobState != want || attemptState != "RUNNING" || reservation != "active" || failed != 1 || cancelled != 25 || attempts != 2 {
		return fmt.Errorf("failure snapshot: sibling=%s/%s reservation=%s failed=%d cancelled=%d attempts=%d", jobState, attemptState, reservation, failed, cancelled, attempts)
	}
	if !s.cancelRunning {
		output, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.Running}}", container).CombinedOutput()
		if err != nil || strings.TrimSpace(string(output)) != "true" {
			return fmt.Errorf("continuing sibling was not physically running: %v: %s", err, output)
		}
	}
	return nil
}

func TestWorkerDaemonsApplySweepFailFast(t *testing.T) {
	for _, cancelRunning := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_running_%t", cancelRunning), func(t *testing.T) {
			f := newSweepDaemonFixture(t)
			job := f.job
			job.Spec.Command = []string{"sh", "-c", `printf '{"seed":%s,"score":9007199254740993}\n' "$SEED" > /outputs/evaluation.json; if [ "$ALGORITHM" = a ] && [ "$RATE" = 0.1 ] && [ "$SEED" = 1 ]; then exit 7; fi; sleep 20`}
			sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: job.Metadata, Spec: spec.SweepSpec{
				JobTemplate: job, MaxConcurrent: 2, FailFast: true, CancelRunningOnFailure: cancelRunning,
				Matrix: map[string][]string{"ALGORITHM": {"a", "b", "c"}, "RATE": {"0.1", "0.01", "0.001"}, "SEED": {"1", "2", "3"}},
			}}
			submitted := f.submit(t, sweep)
			gate := &sweepFailureGate{WorkerServiceServer: workerapi.NewService(f.pool, store.AcquisitionPolicy{AllowSoftScratch: true}, f.objects), pool: f.pool, sweepID: submitted.ID, failedID: submitted.ChildIDs[0], siblingID: submitted.ChildIDs[1], cancelRunning: cancelRunning, running: make(chan struct{}), committed: make(chan struct{})}
			f.startWorkers(t, gate)
			var page client.SweepPage
			for {
				if err := json.Unmarshal(f.runCLI(t, "sweep", "get", submitted.ID, "--json"), &page); err != nil {
					t.Fatal(err)
				}
				if page.Sweep.State == "FAILED" && gate.calls.Load() >= 2 {
					break
				}
				select {
				case <-f.ctx.Done():
					t.Fatal("fail-fast sweep did not finish", page.Sweep.Progress)
				case <-time.After(100 * time.Millisecond):
				}
			}
			gate.mu.Lock()
			evidenceErr := gate.evidenceErr
			diagnosticMetrics := string(gate.original.GetMetricsJson())
			diagnosticOutputs := gate.original.GetOutputs()
			gate.mu.Unlock()
			if evidenceErr != nil {
				t.Fatal(evidenceErr)
			}
			if diagnosticMetrics != "{\"seed\":1,\"score\":9007199254740993}\n" || len(diagnosticOutputs) != 1 || diagnosticOutputs[0].GetName() != "metrics" || diagnosticOutputs[0].GetArtifactId() == "" {
				t.Fatal("failed attempt did not supply diagnostic metric evidence")
			}
			wantCancelled, wantSucceeded := 25, 1
			if cancelRunning {
				wantCancelled, wantSucceeded = 26, 0
			}
			if page.Sweep.Progress != (client.SweepProgress{Total: 27, Failed: 1, Cancelled: wantCancelled, Succeeded: wantSucceeded}) {
				t.Fatal("incorrect terminal sweep counts", page.Sweep.Progress)
			}
			var attempts, completions, held, events int
			err := f.pool.QueryRow(f.ctx, `SELECT count(*),count(c.attempt_id),count(*) FILTER(WHERE r.state<>'released'),
				(SELECT count(*) FROM job_events e JOIN jobs x ON x.id=e.job_id WHERE x.sweep_id=$1 AND e.type='SWEEP_FAIL_FAST')
				FROM attempts a JOIN jobs j ON j.id=a.job_id JOIN reservations r ON r.attempt_id=a.id LEFT JOIN attempt_completions c ON c.attempt_id=a.id WHERE j.sweep_id=$1`, submitted.ID).Scan(&attempts, &completions, &held, &events)
			if err != nil || attempts != 2 || completions != 2 || held != 0 || events != wantCancelled {
				t.Fatal("failure replay duplicated work/events or leaked reservations", attempts, completions, held, events, err)
			}
			var uniqueEvents int
			if err := f.pool.QueryRow(f.ctx, `SELECT count(DISTINCT e.job_id) FROM job_events e JOIN jobs j ON j.id=e.job_id WHERE j.sweep_id=$1 AND e.type='SWEEP_FAIL_FAST' AND e.payload->>'failedJobId'=$2 AND e.payload->>'sweepId'=$1::text`, submitted.ID, gate.failedID).Scan(&uniqueEvents); err != nil || uniqueEvents != wantCancelled {
				t.Fatal("fail-fast events lost sibling or failure identity", uniqueEvents, err)
			}
			var reason string
			var exit *int
			if err := f.pool.QueryRow(f.ctx, "SELECT reason,exit_code FROM attempts WHERE job_id=$1", gate.failedID).Scan(&reason, &exit); err != nil || reason != "APPLICATION_EXIT" || exit == nil || *exit != 7 {
				t.Fatal("failure lost its execution evidence", reason, exit, err)
			}
			if cancelRunning {
				if err := f.pool.QueryRow(f.ctx, "SELECT reason FROM attempts WHERE job_id=$1", gate.siblingID).Scan(&reason); err != nil || reason != "USER_CANCELLED" {
					t.Fatal("running sibling did not acknowledge cancellation", reason, err)
				}
			}
			var results client.SweepResults
			if err := json.Unmarshal(f.runCLI(t, "sweep", "export", submitted.ID), &results); err != nil {
				t.Fatal(err)
			}
			if len(results.Children) != 27 {
				t.Fatal("failure export omitted children")
			}
			for index, child := range results.Children {
				want := "CANCELLED"
				if index == 0 {
					want = "FAILED"
				} else if index == 1 && !cancelRunning {
					want = "SUCCEEDED"
				}
				if child.ID != submitted.ChildIDs[index] || child.Index != index || child.State != want {
					t.Fatal("incorrect failure export identity/outcome", child)
				}
				if want == "SUCCEEDED" {
					if child.AcceptedAttemptID == nil || len(child.Metrics) != 2 || child.Metrics["seed"].String() != "2" || child.Metrics["score"].String() != "9007199254740993" {
						t.Fatal("continuing sibling lost accepted metrics", child)
					}
				} else if child.AcceptedAttemptID != nil || len(child.Metrics) != 0 {
					t.Fatal("failed/cancelled metrics became canonical", child)
				}
			}
			rows, err := csv.NewReader(bytes.NewReader(f.runCLI(t, "sweep", "export", submitted.ID, "--format", "csv"))).ReadAll()
			if err != nil || len(rows) != 28 {
				t.Fatal("incomplete failure CSV", len(rows), err)
			}
			header := []string{"index", "jobId", "state", "acceptedAttemptId", "parameters"}
			if !cancelRunning {
				header = append(header, "metric.score", "metric.seed")
			}
			if !reflect.DeepEqual(rows[0], header) {
				t.Fatal("incorrect failure CSV columns", rows[0])
			}
			for index, row := range rows[1:] {
				child := results.Children[index]
				if row[1] != child.ID || row[2] != child.State {
					t.Fatal("failure CSV disagrees with JSON", row)
				}
				if child.State != "SUCCEEDED" && (row[3] != "" || len(row) > 5 && (row[5] != "" || row[6] != "")) {
					t.Fatal("failure CSV leaked canonical metrics", row)
				}
				if child.State == "SUCCEEDED" && (row[3] != *child.AcceptedAttemptID || row[5] != "9007199254740993" || row[6] != "2") {
					t.Fatal("failure CSV lost continuing sibling metrics", row)
				}
			}
			// Terminal database state can precede physical cleanup. Observe absence
			// before fixture cleanup can remove containers and hide a worker leak.
			for _, id := range []string{gate.failedID, gate.siblingID} {
				for {
					output, err := exec.CommandContext(f.ctx, "docker", "ps", "-aq", "--filter", "label=dev.dispatch.job="+id).CombinedOutput()
					if err != nil {
						t.Fatal("cannot inspect terminal container cleanup", err, string(output))
					}
					if strings.TrimSpace(string(output)) == "" {
						break
					}
					select {
					case <-f.ctx.Done():
						t.Fatal("terminal container was not cleaned", id)
					case <-time.After(100 * time.Millisecond):
					}
				}
			}
			t.Logf("27-child fail-fast sweep: failed=1 succeeded=%d cancelled=%d; failure reply replayed, reservations released, containers absent", wantSucceeded, wantCancelled)
		})
	}
}
