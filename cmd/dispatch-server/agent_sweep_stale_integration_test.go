//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type delayedSweepCompletion struct {
	pb.WorkerServiceServer
	jobID       string
	captured    chan *pb.CompleteAttemptRequest
	mu          sync.Mutex
	original    *pb.CompleteAttemptRequest
	allowOld    bool
	evidenceErr error
}

func (s *delayedSweepCompletion) CompleteAttempt(ctx context.Context, request *pb.CompleteAttemptRequest) (*pb.CompleteAttemptResponse, error) {
	if request.GetAuthority().GetJobId() != s.jobID {
		return s.WorkerServiceServer.CompleteAttempt(ctx, request)
	}
	s.mu.Lock()
	if s.original == nil {
		s.original = proto.Clone(request).(*pb.CompleteAttemptRequest)
		s.captured <- s.original
	}
	old := request.GetAuthority().GetAttemptId() == s.original.GetAuthority().GetAttemptId()
	if old && !proto.Equal(s.original, request) {
		s.evidenceErr = fmt.Errorf("delayed worker completion changed its sealed evidence")
	}
	blocked := old && !s.allowOld
	s.mu.Unlock()
	if blocked {
		// Drop the real completed request before acceptance. A replacement may
		// publish normally; only the original attempt's completion is delayed.
		return nil, status.Error(codes.Unavailable, "fixture delayed original completion")
	}
	return s.WorkerServiceServer.CompleteAttempt(ctx, request)
}

func TestWorkerDaemonsRejectDelayedSweepResultAfterReplacement(t *testing.T) {
	f := newSweepDaemonFixture(t)
	job := f.job
	// Distinct attempt IDs make stale and accepted output bytes distinguishable,
	// even though the immutable job command is identical on both workers.
	job.Spec.Command = []string{"sh", "-c", `printf '%s\n' "$DISPATCH_ATTEMPT_ID" > /outputs/result.txt; printf '{"seed":%s,"score":9007199254740993}\n' "$SEED" > /outputs/evaluation.json`}
	job.Spec.Outputs = append(job.Spec.Outputs, spec.Output{Name: "result", Path: "/outputs/result.txt", Required: true, MaxBytes: 128})
	job.Spec.Retry.MaxAttempts = 2
	job.Spec.Retry.On = []string{"WORKER_LOST"}
	job.Spec.Retry.InitialBackoffSeconds, job.Spec.Retry.MaxBackoffSeconds = 1, 1
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: job.Metadata, Spec: spec.SweepSpec{
		JobTemplate: job, MaxConcurrent: 2, Matrix: map[string][]string{"SEED": {"1", "2", "3"}},
	}}
	submitted := f.submit(t, sweep)
	gate := &delayedSweepCompletion{WorkerServiceServer: workerapi.NewService(f.pool, store.AcquisitionPolicy{AllowSoftScratch: true}, f.objects), jobID: submitted.ChildIDs[0], captured: make(chan *pb.CompleteAttemptRequest, 1)}
	daemons := f.startWorkers(t, gate)
	var delayed *pb.CompleteAttemptRequest
	select {
	case delayed = <-gate.captured:
	case <-f.ctx.Done():
		t.Fatal("worker never produced a real completion to delay")
	}
	old := delayed.GetAuthority()
	if delayed.ExitCode == nil || delayed.GetExitCode() != 0 || !delayed.GetStopped() || delayed.GetReason() != pb.FailureReason_FAILURE_REASON_UNSPECIFIED || len(delayed.GetOutputs()) != 2 || string(delayed.GetMetricsJson()) != "{\"seed\":1,\"score\":9007199254740993}\n" {
		t.Fatal("delayed request did not represent actual successful execution")
	}
	var daemon sweepTestDaemon
	for _, candidate := range daemons {
		if candidate.workerID == old.GetWorkerId() {
			daemon = candidate
			break
		}
	}
	if daemon.stop == nil {
		t.Fatal("delayed request did not belong to a fixture worker")
	}
	daemon.stop()
	var phase, container string
	var expiry, now time.Time
	err := f.pool.QueryRow(f.ctx, "SELECT state,container_id,lease_expires_at,clock_timestamp() FROM attempts WHERE id=$1", old.GetAttemptId()).Scan(&phase, &container, &expiry, &now)
	if err != nil || phase != "FINALIZING" || expiry.Sub(now) < 20*time.Second {
		t.Fatal("old completion was not held before acceptance with its natural lease", phase, expiry, now, err)
	}
	output, err := exec.CommandContext(f.ctx, "docker", "inspect", "--format", "{{.State.Running}}", container).CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "false" {
		t.Fatal("delayed completion's container had not actually finished", err, string(output))
	}
	// Verified old output must exist before loss. Otherwise rejection could be
	// explained by fabricated or missing artifacts instead of stale authority.
	var oldResultID string
	var oldObject objectstore.Object
	for _, reference := range delayed.GetOutputs() {
		var owner, name string
		var object objectstore.Object
		err := f.pool.QueryRow(f.ctx, `SELECT u.attempt_id::text,u.logical_name,u.object_key,a.object_version,u.size_bytes,u.sha256
			FROM artifacts a JOIN artifact_uploads u ON u.upload_id=a.upload_id WHERE a.id=$1`, reference.GetArtifactId()).Scan(&owner, &name, &object.Key, &object.Version, &object.Size, &object.SHA256)
		if err != nil || owner != old.GetAttemptId() || name != reference.GetName() || f.objects.Verify(f.ctx, object) != nil {
			t.Fatal("delayed request lacked verified attempt-owned output", name, owner, err)
		}
		if name == "result" {
			oldResultID, oldObject = reference.GetArtifactId(), object
		}
	}
	if oldResultID == "" || oldObject.Size != int64(len(old.GetAttemptId())+1) || oldObject.SHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte(old.GetAttemptId()+"\n"))) {
		t.Fatal("old output did not contain its actual attempt identity")
	}
	reaperCtx, stopReaper := context.WithCancel(f.ctx)
	reaperDone := make(chan struct{})
	var reaperLogs lockedBuffer
	go func() {
		defer close(reaperDone)
		runLeaseReaper(reaperCtx, f.pool, slog.New(slog.NewJSONHandler(&reaperLogs, nil)))
	}()
	t.Cleanup(func() {
		stopReaper()
		<-reaperDone
		if t.Failed() {
			t.Log("lease reaper:", reaperLogs.String())
		}
	})
	for {
		var page client.SweepPage
		if err := json.Unmarshal(f.runCLI(t, "sweep", "get", submitted.ID, "--json"), &page); err != nil {
			t.Fatal(err)
		}
		if page.Sweep.Progress.Failed > 0 || page.Sweep.Progress.Cancelled > 0 {
			t.Fatal("delayed-result sweep did not recover", page.Sweep.Progress)
		}
		if page.Sweep.State == "SUCCEEDED" {
			break
		}
		select {
		case <-f.ctx.Done():
			t.Fatal("replacement never completed", page.Sweep.Progress)
		case <-time.After(100 * time.Millisecond):
		}
	}
	var replacement, replacementWorker, oldState, oldReservation, replacementReservation string
	var generation int64
	var manifestBefore []byte
	var lostAt time.Time
	err = f.pool.QueryRow(f.ctx, `SELECT j.accepted_attempt_id::text,j.accepted_manifest,n.worker_id::text,n.generation,a.state,a.finished_at,r.state,nr.state
		FROM jobs j JOIN attempts n ON n.id=j.accepted_attempt_id JOIN attempts a ON a.id=$2 JOIN reservations r ON r.attempt_id=a.id JOIN reservations nr ON nr.attempt_id=n.id WHERE j.id=$1`, old.GetJobId(), old.GetAttemptId()).Scan(&replacement, &manifestBefore, &replacementWorker, &generation, &oldState, &lostAt, &oldReservation, &replacementReservation)
	if err != nil || replacement == old.GetAttemptId() || replacementWorker == old.GetWorkerId() || generation != int64(old.GetGeneration())+1 || oldState != "LOST" || lostAt.Before(expiry) || oldReservation != "quarantined" || replacementReservation != "released" {
		t.Fatal("replacement did not supersede the naturally lost attempt", replacement, oldState, oldReservation, replacementReservation, err)
	}
	gate.mu.Lock()
	gate.allowOld = true
	evidenceErr := gate.evidenceErr
	gate.mu.Unlock()
	if evidenceErr != nil {
		t.Fatal(evidenceErr)
	}
	// Replay through the real listener with the killed worker's enrolled leaf.
	// This preserves authentication and the original session/generation fence.
	connection, err := grpc.NewClient(daemon.endpoint, grpc.WithTransportCredentials(credentials.NewTLS(daemon.tls)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	rpcCtx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	reply, err := pb.NewWorkerServiceClient(connection).CompleteAttempt(rpcCtx, delayed)
	if err != nil || reply.GetDecision() != pb.Decision_ALREADY_TERMINAL || reply.GetState() != pb.AttemptState_LOST || len(reply.GetAcceptedManifestJson()) != 0 {
		t.Fatal("old completed result was not rejected as lost", reply, err)
	}
	var manifestAfter []byte
	var acceptedID string
	var attempts, completions, oldCompletions int
	err = f.pool.QueryRow(f.ctx, `SELECT j.accepted_attempt_id::text,j.accepted_manifest,
		(SELECT count(*) FROM attempts a JOIN jobs x ON x.id=a.job_id WHERE x.sweep_id=$2),
		(SELECT count(*) FROM attempt_completions c JOIN jobs x ON x.id=c.job_id WHERE x.sweep_id=$2),
		(SELECT count(*) FROM attempt_completions WHERE attempt_id=$3)
		FROM jobs j WHERE j.id=$1`, old.GetJobId(), submitted.ID, old.GetAttemptId()).Scan(&acceptedID, &manifestAfter, &attempts, &completions, &oldCompletions)
	if err != nil || acceptedID != replacement || !bytes.Equal(manifestBefore, manifestAfter) || attempts != 4 || completions != 3 || oldCompletions != 0 {
		t.Fatal("delayed completion changed accepted history or manifest", acceptedID, attempts, completions, oldCompletions, err)
	}
	destination := filepath.Join(f.root, "accepted-result.txt")
	var receipt client.DownloadReceipt
	if err := json.Unmarshal(f.runCLI(t, "artifacts", "download", old.GetJobId(), "result", "--output", destination, "--json"), &receipt); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(destination)
	if err != nil || string(data) != replacement+"\n" || receipt.AttemptID != replacement || receipt.ArtifactID == oldResultID || receipt.SHA256 == oldObject.SHA256 || receipt.SizeBytes != int64(len(data)) || receipt.SHA256 != fmt.Sprintf("%x", sha256.Sum256(data)) {
		t.Fatal("accepted download did not retain distinct replacement bytes", receipt, string(data), err)
	}
	if err := f.objects.Verify(f.ctx, oldObject); err != nil {
		t.Fatal("rejected old output lost its immutable diagnostic version", err)
	}
	var results client.SweepResults
	if err := json.Unmarshal(f.runCLI(t, "sweep", "export", submitted.ID), &results); err != nil {
		t.Fatal(err)
	}
	if len(results.Children) != 3 || results.Children[0].ID != old.GetJobId() || results.Children[0].AcceptedAttemptID == nil || *results.Children[0].AcceptedAttemptID != replacement || results.Children[0].Metrics["score"].String() != "9007199254740993" {
		t.Fatal("accepted export changed after rejecting delayed result")
	}
	t.Log("real old completion rejected as ALREADY_TERMINAL/LOST after replacement; accepted manifest, history, export and downloaded attempt identity preserved")
}
