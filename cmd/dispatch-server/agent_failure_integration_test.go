//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type failedContainerInspection struct {
	State struct {
		Running, OOMKilled bool
		ExitCode           int32
	}
	HostConfig struct {
		Memory, MemorySwap int64
	}
	err error
}

type inspectFailedCompletion struct {
	pb.WorkerServiceServer
	pool     *pgxpool.Pool
	observed chan failedContainerInspection
}

func (s *inspectFailedCompletion) CompleteAttempt(ctx context.Context, request *pb.CompleteAttemptRequest) (*pb.CompleteAttemptResponse, error) {
	var container string
	var observed failedContainerInspection
	observed.err = s.pool.QueryRow(ctx, "SELECT container_id FROM attempts WHERE id=$1", request.GetAuthority().GetAttemptId()).Scan(&container)
	if observed.err == nil {
		var body []byte
		body, observed.err = exec.CommandContext(ctx, "docker", "inspect", "--format", "{{json .}}", container).Output()
		if observed.err == nil {
			observed.err = json.Unmarshal(body, &observed)
		}
	}
	// Inspect before accepting completion: normal agent cleanup removes the
	// container immediately afterward, including Docker's independent OOM flag.
	select {
	case s.observed <- observed:
	default:
	}
	return s.WorkerServiceServer.CompleteAttempt(ctx, request)
}

func TestWorkerDaemonDistinguishesOOMFromExit137(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		command      []string
		oom          bool
	}{
		{"memory_limit", "OOM", []string{"awk", "BEGIN { for (i=0; i<16777216; i++) a[i]=i }"}, true},
		{"application_exit_137", "APPLICATION_EXIT", []string{"sh", "-c", "exit 137"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) { testWorkerDaemonFailure(t, tc.command, nil, tc.reason, 137, tc.oom) })
	}
}

func TestWorkerDaemonRejectsMissingAndUnsafeRequiredOutputs(t *testing.T) {
	for _, tc := range []struct{ name, command string }{
		{"missing", "true"},
		{"oversized", "head -c 4097 /dev/zero > /outputs/result"},
		{"symlink", "ln -s /etc/passwd /outputs/result"},
		{"directory", "mkdir /outputs/result"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testWorkerDaemonFailure(t, []string{"sh", "-c", tc.command},
				[]spec.Output{{Name: "result", Path: "/outputs/result", Required: true, MaxBytes: 4096}},
				"OUTPUT_INVALID", 0, false)
		})
	}
}

func testWorkerDaemonFailure(t *testing.T, command []string, outputs []spec.Output, wantReason string, wantExit int32, wantOOM bool) {
	t.Helper()
	configureServerTestDatabase(t)
	objects := publicationStorage(t, "agent-failure", &publicationEvidence{})
	pki := testWorkerPKI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	docker := func(args ...string) string {
		t.Helper()
		body, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("Docker failure fixture: %v: %s", err, body)
		}
		return strings.TrimSpace(string(body))
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
	t.Cleanup(pool.Close)
	resources := spec.Resources{CPUMillis: 500, MemoryMiB: 128, ScratchMiB: 64}
	labels := map[string]string{"os": "linux", "architecture": architecture}
	worker, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{
		Name: "failure-worker", CertificateSHA256: sha256.Sum256(pki.client.Certificate[0]),
		Resources: resources, Slots: 1, Labels: labels, Projects: []string{"research"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// The random worker label confines forced cleanup to this fixture.
		body, _ := exec.Command("docker", "ps", "-aq", "--filter", "label=dev.dispatch.worker="+worker.WorkerID).Output()
		for _, container := range strings.Fields(string(body)) {
			_ = exec.Command("docker", "rm", "-f", container).Run()
		}
	})
	example, err := os.ReadFile("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	job, err := spec.DecodeJob(bytes.NewReader(example))
	if err != nil {
		t.Fatal(err)
	}
	job.Spec.Image, job.Spec.Inputs, job.Spec.Args = image, nil, nil
	job.Spec.Command, job.Spec.Outputs, job.Spec.Resources = command, outputs, resources
	// Retryable infrastructure reasons stay enabled: a classification regression
	// must not quietly turn a permanent workload failure into another attempt.
	job.Spec.Retry.MaxAttempts = 2
	job.Spec.Timeouts.StartupSeconds, job.Spec.Timeouts.ExecutionSeconds, job.Spec.Timeouts.FinalizationSeconds = 30, 30, 15
	job.Spec.Placement.Labels["architecture"] = architecture
	_, hash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	submitted, err := store.SubmitJob(ctx, pool, uuid.NewString(), hash, job)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.LoadX509KeyPair(pki.serverCert, pki.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	service := &inspectFailedCompletion{WorkerServiceServer: workerapi.NewService(pool, store.AcquisitionPolicy{AllowSoftScratch: true}, objects), pool: pool, observed: make(chan failedContainerInspection, 1)}
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
	configPath := filepath.Join(root, "worker.json")
	if err := os.WriteFile(configPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := filepath.Abs("../../.local/cargo-target/debug/dispatch-worker")
	if err != nil {
		t.Fatal(err)
	}
	process := exec.CommandContext(ctx, binary, "run", "--config", configPath, "--dev-soft-scratch")
	var logs lockedBuffer
	process.Stdout, process.Stderr = &logs, &logs
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Process.Kill(); _ = process.Wait() })
	var observed failedContainerInspection
	select {
	case observed = <-service.observed:
	case <-ctx.Done():
		t.Fatal("worker never completed the failed job", logs.String())
	}
	if observed.err != nil || observed.State.Running || observed.State.OOMKilled != wantOOM || observed.State.ExitCode != wantExit || observed.HostConfig.Memory != 128<<20 || observed.HostConfig.MemorySwap != 128<<20 {
		t.Fatalf("unexpected independent Docker exit evidence: %+v", observed)
	}
	t.Logf("Docker exit=%d OOMKilled=%t memory=%d swap=%d", observed.State.ExitCode, observed.State.OOMKilled, observed.HostConfig.Memory, observed.HostConfig.MemorySwap)
	var attempt string
	for {
		var jobState, attemptState, reason, reservation string
		var exit *int32
		var canonical, cleanup bool
		var attempts, completions int
		err := pool.QueryRow(ctx, `SELECT a.id::text,j.state,a.state,coalesce(a.reason,''),a.exit_code,r.state,
			j.accepted_attempt_id IS NOT NULL OR j.accepted_manifest IS NOT NULL,a.cleanup_pending,
			(SELECT count(*) FROM attempts WHERE job_id=j.id),(SELECT count(*) FROM attempt_completions WHERE attempt_id=a.id)
			FROM jobs j JOIN attempts a ON a.job_id=j.id JOIN reservations r ON r.attempt_id=a.id WHERE j.id=$1`, submitted.ID).
			Scan(&attempt, &jobState, &attemptState, &reason, &exit, &reservation, &canonical, &cleanup, &attempts, &completions)
		if err != nil {
			t.Fatal(err, logs.String())
		}
		if jobState == "FAILED" {
			if attemptState != "FAILED" || reason != wantReason || exit == nil || *exit != wantExit || reservation != "released" || canonical || cleanup || attempts != 1 || completions != 1 {
				t.Fatal("failure was retried, published, or misclassified", jobState, attemptState, reason, exit, reservation, canonical, cleanup, attempts, completions, logs.String())
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("failure was not accepted", jobState, attemptState, reason, logs.String())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if wantReason == "OUTPUT_INVALID" {
		var uploads int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM artifact_uploads WHERE attempt_id=$1 AND kind='OUTPUT'", attempt).Scan(&uploads); err != nil || uploads != 0 {
			t.Fatal("invalid required file reached the output upload boundary", uploads, err)
		}
	}
	for {
		_, err := os.Stat(filepath.Join(root, "work", attempt))
		if os.IsNotExist(err) && docker("ps", "-aq", "--filter", "label=dev.dispatch.worker="+worker.WorkerID) == "" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("failed workload was not locally cleaned up", err, logs.String())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
