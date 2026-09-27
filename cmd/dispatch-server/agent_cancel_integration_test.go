//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/api"
	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

type cancelOnCompletionService struct {
	pb.WorkerServiceServer
	pool         *pgxpool.Pool
	projectID    string
	target       atomic.Value
	fired        atomic.Bool
	stopSeen     atomic.Bool
	uploadTarget atomic.Value
	uploadFired  atomic.Bool
}

func (s *cancelOnCompletionService) CreateUpload(ctx context.Context, r *pb.CreateUploadRequest) (*pb.CreateUploadResponse, error) {
	if target, ok := s.uploadTarget.Load().(string); ok && r.GetAuthority().GetJobId() == target && s.uploadFired.CompareAndSwap(false, true) {
		// Commit cancellation at the upload gate before any normal completion is
		// journaled, then return the server's scoped stop rejection.
		if _, err := store.RequestCancellation(ctx, s.pool, s.projectID, target); err != nil {
			return nil, err
		}
		return nil, status.Error(codes.FailedPrecondition, "UPLOAD_STOP_REQUESTED")
	}
	return s.WorkerServiceServer.CreateUpload(ctx, r)
}

func (s *cancelOnCompletionService) CompleteAttempt(ctx context.Context, r *pb.CompleteAttemptRequest) (*pb.CompleteAttemptResponse, error) {
	if target, ok := s.target.Load().(string); ok && r.GetAuthority().GetJobId() == target && s.fired.CompareAndSwap(false, true) {
		// Force cancellation to commit after the worker sealed its normal result
		// but before the server considers that result for acceptance.
		if _, err := store.RequestCancellation(ctx, s.pool, s.projectID, target); err != nil {
			return nil, err
		}
		reply, err := s.WorkerServiceServer.CompleteAttempt(ctx, r)
		if err == nil && reply.GetDecision() == pb.Decision_STOP_REQUESTED {
			s.stopSeen.Store(true)
		}
		return reply, err
	}
	return s.WorkerServiceServer.CompleteAttempt(ctx, r)
}

func TestWorkerAcknowledgesLiveCancellation(t *testing.T) {
	configureServerTestDatabase(t)
	pki := testWorkerPKI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	docker := func(args ...string) string {
		t.Helper()
		output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("Docker fixture failed: %v: %s", err, output)
		}
		return strings.TrimSpace(string(output))
	}
	architecture := docker("version", "--format", "{{.Server.Arch}}")
	socket := strings.TrimPrefix(docker("context", "inspect", "--format", "{{.Endpoints.docker.Host}}"), "unix://")
	image := "rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89"
	if exec.CommandContext(ctx, "docker", "image", "inspect", image).Run() != nil {
		docker("pull", image)
	}
	pool, err := openPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	resources := spec.Resources{CPUMillis: 500, MemoryMiB: 128, ScratchMiB: 64}
	labels := map[string]string{"os": "linux", "architecture": architecture}
	worker, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{
		Name: "cancel-worker", CertificateSHA256: sha256.Sum256(pki.client.Certificate[0]),
		Resources: resources, Slots: 1, Labels: labels, Projects: []string{"research"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		output, _ := exec.Command("docker", "ps", "-aq", "--filter", "label=dev.dispatch.worker="+worker.WorkerID).Output()
		for _, id := range strings.Fields(string(output)) {
			_ = exec.Command("docker", "rm", "-f", id).Run()
		}
	})
	file, err := os.Open("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	job, err := spec.DecodeJob(file)
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	job.Spec.Inputs = nil
	job.Spec.Outputs = nil
	job.Spec.Image = image
	job.Spec.Command = []string{"sh", "-c", "exec sleep 60"}
	job.Spec.Args = nil
	job.Spec.Resources = resources
	job.Spec.Placement.Labels["architecture"] = architecture
	_, hash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	queued, err := store.SubmitJob(ctx, pool, uuid.NewString(), hash, job)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.LoadX509KeyPair(pki.serverCert, pki.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	service := &cancelOnCompletionService{WorkerServiceServer: workerapi.NewService(pool, store.AcquisitionPolicy{AllowSoftScratch: true}, nil), pool: pool, projectID: queued.ProjectID}
	server, err := workerapi.NewServer(pool, certificate, pki.roots, service)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-served })
	root := t.TempDir()
	for _, name := range []string{"state", "work"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	config, err := json.Marshal(map[string]any{
		"worker_id": worker.WorkerID, "server_url": "https://" + listener.Addr().String(),
		"ca_cert": pki.ca, "client_cert": pki.clientCert, "client_key": pki.clientKey,
		"journal_dir": filepath.Join(root, "state"), "workspace_root": filepath.Join(root, "work"),
		"docker_socket": socket, "cpu_millis": 500, "memory_mib": 128, "scratch_mib": 64,
		"execution_slots": 1, "labels": labels,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "worker.json")
	if err := os.WriteFile(path, config, 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := filepath.Abs("../../.local/cargo-target/debug/dispatch-worker")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, binary, "run", "--config", path, "--dev-soft-scratch")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostic lockedBuffer
	command.Stderr = &diagnostic
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	decoder := json.NewDecoder(stdout)
	attempt := ""
	deadline := time.Now().Add(18 * time.Second)
	for time.Now().Before(deadline) {
		if containers := docker("ps", "-q", "--filter", "label=dev.dispatch.worker="+worker.WorkerID); containers != "" {
			_ = pool.QueryRow(ctx, "SELECT id::text FROM attempts WHERE worker_id=$1 AND state='RUNNING'", worker.WorkerID).Scan(&attempt)
			if attempt != "" {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if attempt == "" {
		t.Fatal("worker never entered a real running attempt")
	}
	token, _, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(api.New(pool, nil, nil))
	defer httpServer.Close()
	cliEnv := func(key string) string {
		return map[string]string{"DISPATCH_URL": httpServer.URL, "DISPATCH_TOKEN": token, "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	var cancelOutput, cancelDiagnostic bytes.Buffer
	if code := cli.Run(ctx, []string{"cancel", queued.ID, "--json"}, cliEnv, &cancelOutput, &cancelDiagnostic); code != 0 {
		t.Fatal("CLI cancellation failed", code, cancelDiagnostic.String())
	}
	var requested store.JobRecord
	err = json.Unmarshal(cancelOutput.Bytes(), &requested)
	if err != nil || requested.State != "CANCELLING" {
		t.Fatal(requested, err)
	}
	for {
		var event struct {
			Event     string `json:"event"`
			AttemptID string `json:"attempt_id"`
		}
		if err := decoder.Decode(&event); err != nil {
			waitErr := command.Wait()
			t.Fatal("worker stopped before cancellation acknowledgement", err, waitErr, diagnostic.String())
		}
		if event.Event == "attempt_terminal" {
			if event.AttemptID != attempt {
				t.Fatal("wrong attempt terminalized", event.AttemptID, attempt)
			}
			break
		}
	}
	var jobState, attemptState, reservation, reason string
	var completions int
	if err := pool.QueryRow(ctx, `SELECT j.state,a.state,r.state,a.reason,
		(SELECT count(*) FROM attempt_completions WHERE attempt_id=a.id)
		FROM jobs j JOIN attempts a ON a.id=$2 JOIN reservations r ON r.attempt_id=a.id
		WHERE j.id=$1`, queued.ID, attempt).
		Scan(&jobState, &attemptState, &reservation, &reason, &completions); err != nil || jobState != "CANCELLED" || attemptState != "CANCELLED" || reservation != "released" || reason != "USER_CANCELLED" || completions != 1 {
		t.Fatal("cancellation was not durably acknowledged", jobState, attemptState, reservation, reason, completions, err)
	}
	if containers := docker("ps", "-aq", "--filter", "label=dev.dispatch.worker="+worker.WorkerID); containers != "" {
		t.Fatal("cancelled container survived cleanup", containers)
	}
	if _, err := os.Stat(filepath.Join(root, "work", attempt)); !os.IsNotExist(err) {
		t.Fatal("cancelled workspace survived cleanup", err)
	}
	job.Spec.Command = []string{"sh", "-c", "printf resumed; sleep 1"}
	_, nextHash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	nextJob, err := store.SubmitJob(ctx, pool, uuid.NewString(), nextHash, job)
	if err != nil {
		t.Fatal(err)
	}
	service.target.Store(nextJob.ID)
	for {
		var event struct {
			Event     string `json:"event"`
			AttemptID string `json:"attempt_id"`
		}
		if err := decoder.Decode(&event); err != nil {
			waitErr := command.Wait()
			t.Fatal("worker stopped before accepting another job", err, waitErr, diagnostic.String())
		}
		if event.Event == "attempt_terminal" {
			if event.AttemptID == attempt {
				t.Fatal("worker replayed cancelled attempt")
			}
			break
		}
	}
	var raced, racedReason, racedReservation, racedAttempt string
	if err := pool.QueryRow(ctx, `SELECT j.state,a.reason,r.state,a.id::text FROM jobs j JOIN attempts a ON a.job_id=j.id JOIN reservations r ON r.attempt_id=a.id WHERE j.id=$1`, nextJob.ID).Scan(&raced, &racedReason, &racedReservation, &racedAttempt); err != nil || raced != "CANCELLED" || racedReason != "USER_CANCELLED" || racedReservation != "released" || !service.stopSeen.Load() {
		t.Fatal("finalization cancellation was not acknowledged", raced, racedReason, racedReservation, service.stopSeen.Load(), err, diagnostic.String())
	}
	if _, err := os.Stat(filepath.Join(root, "work", racedAttempt)); !os.IsNotExist(err) {
		t.Fatal("sealed-result cancellation left a workspace", err)
	}
	job.Spec.Command = []string{"sh", "-c", "printf abc > /outputs/result; sleep 1"}
	job.Spec.Outputs = []spec.Output{{Name: "result", Path: "/outputs/result", Required: true, MaxBytes: 3}}
	_, thirdHash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	thirdJob, err := store.SubmitJob(ctx, pool, uuid.NewString(), thirdHash, job)
	if err != nil {
		t.Fatal(err)
	}
	service.uploadTarget.Store(thirdJob.ID)
	for {
		var event struct {
			Event string `json:"event"`
		}
		if err := decoder.Decode(&event); err != nil {
			t.Fatal("worker stopped after finalization cancellation", err, diagnostic.String())
		}
		if event.Event == "attempt_terminal" {
			break
		}
	}
	var thirdState, thirdReason, thirdReservation, thirdAttempt string
	if err := pool.QueryRow(ctx, `SELECT j.state,a.reason,r.state,a.id::text FROM jobs j JOIN attempts a ON a.job_id=j.id JOIN reservations r ON r.attempt_id=a.id WHERE j.id=$1`, thirdJob.ID).Scan(&thirdState, &thirdReason, &thirdReservation, &thirdAttempt); err != nil || thirdState != "CANCELLED" || thirdReason != "USER_CANCELLED" || thirdReservation != "released" || !service.uploadFired.Load() {
		t.Fatal("mid-finalization cancellation was not acknowledged", thirdState, thirdReason, service.uploadFired.Load(), err, diagnostic.String())
	}
	if _, err := os.Stat(filepath.Join(root, "work", thirdAttempt)); !os.IsNotExist(err) {
		t.Fatal("mid-finalization cancellation left a workspace", err)
	}
	if containers := docker("ps", "-aq", "--filter", "label=dev.dispatch.worker="+worker.WorkerID); containers != "" {
		t.Fatal("finalization cancellation left a container", containers)
	}
	job.Spec.Outputs = nil
	job.Spec.Command = []string{"sh", "-c", "printf resumed"}
	_, fourthHash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	fourthJob, err := store.SubmitJob(ctx, pool, uuid.NewString(), fourthHash, job)
	if err != nil {
		t.Fatal(err)
	}
	for {
		var event struct {
			Event string `json:"event"`
		}
		if err := decoder.Decode(&event); err != nil {
			t.Fatal("worker stopped before resuming", err, diagnostic.String())
		}
		if event.Event == "attempt_terminal" {
			break
		}
	}
	var resumed string
	if err := pool.QueryRow(ctx, "SELECT state FROM jobs WHERE id=$1", fourthJob.ID).Scan(&resumed); err != nil || resumed != "SUCCEEDED" {
		t.Fatal("worker did not resume admission after finalization cancellation", resumed, err)
	}
}
