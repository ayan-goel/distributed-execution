//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/google/uuid"
)

func TestWorkerRestartFencesAndRemovesItsOwnRunningJob(t *testing.T) {
	testWorkerRestartAndCleanup(t, "")
}

func TestWorkerExecutionTimeoutStopsAndReconcilesItsOwnJob(t *testing.T) {
	testWorkerRestartAndCleanup(t, "EXECUTION_TIMEOUT")
}

func TestWorkerStartupTimeoutCannotLaunchAfterStalledImagePreparation(t *testing.T) {
	testWorkerRestartAndCleanup(t, "STARTUP_TIMEOUT")
}

func testWorkerRestartAndCleanup(t *testing.T, timeoutReason string) {
	t.Helper()
	configureServerTestDatabase(t)
	pki := testWorkerPKI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
	var stalledImage *stalledImageProxy
	if timeoutReason == "STARTUP_TIMEOUT" {
		stalledImage = newStalledImageProxy(t, socket)
		socket = stalledImage.socket
	}
	pool, err := openPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	resources := spec.Resources{CPUMillis: 500, MemoryMiB: 128, ScratchMiB: 64}
	labels := map[string]string{"os": "linux", "architecture": architecture}
	worker, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{
		Name: "restart-worker", CertificateSHA256: sha256.Sum256(pki.client.Certificate[0]),
		Resources: resources, Slots: 1, Labels: labels, Projects: []string{"research"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		output, _ := exec.Command("docker", "ps", "-aq", "--filter", "label=dev.dispatch.worker="+worker.WorkerID).Output()
		for _, container := range strings.Fields(string(output)) {
			_ = exec.Command("docker", "rm", "-f", container).Run()
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
	job.Spec.Image = image
	job.Spec.Command = []string{"sleep", "120"}
	job.Spec.Args = nil
	job.Spec.Outputs = nil
	job.Spec.Resources = resources
	job.Spec.Retry.MaxAttempts = 1
	if timeoutReason != "" {
		// Keep worker-loss retries enabled so misclassifying the timeout would
		// schedule another attempt instead of preserving a permanent failure.
		job.Spec.Retry.MaxAttempts = 2
		job.Spec.Timeouts.ExecutionSeconds = 5
		job.Spec.TerminationGraceSeconds = 1
		if timeoutReason == "STARTUP_TIMEOUT" {
			job.Spec.Timeouts.StartupSeconds = 5
		}
	}
	job.Spec.Placement.Labels["architecture"] = architecture
	_, hash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SubmitJob(ctx, pool, uuid.NewString(), hash, job); err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.LoadX509KeyPair(pki.serverCert, pki.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	server, err := workerapi.NewServer(pool, certificate, pki.roots, workerapi.NewService(pool, store.AcquisitionPolicy{AllowSoftScratch: true}, nil))
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
	if timeoutReason != "" {
		reaping, stopReaping := context.WithCancel(ctx)
		reaped := make(chan struct{})
		go func() {
			defer close(reaped)
			runLeaseReaper(reaping, pool, slog.New(slog.NewTextHandler(io.Discard, nil)))
		}()
		defer func() { stopReaping(); <-reaped }()
	}
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
	type workerEvent struct {
		Event     string `json:"event"`
		SessionID string `json:"session_id"`
	}
	start := func() (*json.Decoder, func(), <-chan struct{}) {
		t.Helper()
		command := exec.CommandContext(ctx, binary, "run", "--config", path, "--dev-soft-scratch")
		stdout, err := command.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		var diagnostic bytes.Buffer
		command.Stderr = &diagnostic
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		exited := make(chan struct{})
		go func() { _ = command.Wait(); close(exited) }()
		var once sync.Once
		stop := func() {
			once.Do(func() { _ = command.Process.Kill(); <-exited })
		}
		t.Cleanup(stop)
		return json.NewDecoder(stdout), stop, exited
	}
	read := func(decoder *json.Decoder, expected string) workerEvent {
		t.Helper()
		for {
			var event workerEvent
			if err := decoder.Decode(&event); err != nil {
				t.Fatal("worker exited before", expected, err)
			}
			if event.Event == expected {
				return event
			}
		}
	}
	first, stopFirst, firstExited := start()
	oldSession := read(first, "session_pending").SessionID
	read(first, "ready")
	container := ""
	until := time.Now().Add(20 * time.Second)
	if stalledImage != nil {
		select {
		case <-stalledImage.entered:
		case <-ctx.Done():
			t.Fatal("worker did not reach the stalled image inspection")
		}
	}
	for stalledImage == nil && time.Now().Before(until) {
		output := docker("ps", "-q", "--filter", "label=dev.dispatch.worker="+worker.WorkerID)
		if fields := strings.Fields(output); len(fields) == 1 {
			container = fields[0]
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if stalledImage == nil && container == "" {
		t.Fatal("first agent never started its own Docker job")
	}
	var attempt string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM attempts WHERE worker_id=$1", worker.WorkerID).Scan(&attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "work", attempt)); err != nil {
		t.Fatal("first agent did not create its attempt workspace", err)
	}
	if timeoutReason != "" {
		until := time.Now().Add(15 * time.Second)
		stopped := stalledImage != nil
		for !stopped && time.Now().Before(until) {
			if docker("inspect", "--format", "{{.State.Running}}", container) == "false" {
				stopped = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !stopped {
			t.Fatal("worker failed to stop the actual Docker workload at execution expiry")
		}
		for {
			var state, reason, reservation string
			var cleanup bool
			if err := pool.QueryRow(ctx, `SELECT a.state,coalesce(a.reason,''),r.state,a.cleanup_pending
				FROM attempts a JOIN reservations r ON r.attempt_id=a.id WHERE a.id=$1`, attempt).
				Scan(&state, &reason, &reservation, &cleanup); err != nil {
				t.Fatal(err)
			}
			if state == "FAILED" && reason == timeoutReason && reservation == "quarantined" && cleanup {
				break
			}
			if time.Now().After(until) {
				t.Fatal("timeout was retried or released capacity before reconciliation", state, reason, reservation, cleanup)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	if stalledImage != nil {
		stalledImage.release()
		select {
		case <-firstExited:
		case <-time.After(10 * time.Second):
			t.Fatal("worker continued after startup authority expired")
		}
		if stalledImage.creates.Load() != 0 || docker("ps", "-aq", "--filter", "label=dev.dispatch.worker="+worker.WorkerID) != "" {
			t.Fatal("stalled preparation created a container after startup expired")
		}
	}
	stopFirst()
	if container != "" {
		if state := docker("inspect", "--format", "{{.State.Running}}", container); timeoutReason == "" && state != "true" {
			t.Fatal("job stopped before the replacement agent fenced it", state)
		}
	}
	second, stopSecond, _ := start()
	defer stopSecond()
	newSession := read(second, "session_pending").SessionID
	if newSession == "" || newSession == oldSession {
		t.Fatal("restart reused the old session", newSession)
	}
	// Name the exact replacement session while the old lease is still live.
	// Registration then fences it before any new local cleanup is trusted.
	if err := store.ApproveSessionTakeover(ctx, pool, worker.WorkerID, oldSession, newSession); err != nil {
		t.Fatal(err)
	}
	read(second, "ready")
	if container != "" && exec.CommandContext(ctx, "docker", "inspect", container).Run() == nil {
		t.Fatal("fenced container survived replacement readiness")
	}
	if _, err := os.Stat(filepath.Join(root, "work", attempt)); !os.IsNotExist(err) {
		t.Fatal("fenced workspace survived replacement readiness", err)
	}
	var jobState, attemptState, reason, reservation, workerState string
	var cleanup bool
	if err := pool.QueryRow(ctx, `SELECT j.state,a.state,a.reason,r.state,w.state,a.cleanup_pending
	FROM attempts a JOIN jobs j ON j.id=a.job_id JOIN reservations r ON r.attempt_id=a.id
	JOIN workers w ON w.id=a.worker_id WHERE a.id=$1`, attempt).Scan(&jobState, &attemptState, &reason, &reservation, &workerState, &cleanup); err != nil {
		t.Fatal(err)
	}
	wantState, wantReason := "LOST", "WORKER_LOST"
	if timeoutReason != "" {
		wantState, wantReason = "FAILED", timeoutReason
	}
	if jobState != "FAILED" || attemptState != wantState || reason != wantReason || reservation != "released" || workerState != "READY" || cleanup {
		t.Fatal("replacement did not fence, release, and reconcile the old job", jobState, attemptState, reason, reservation, workerState, cleanup)
	}
	var attempts, completions int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM attempts WHERE worker_id=$1),
	(SELECT count(*) FROM attempt_completions WHERE attempt_id=$2)`, worker.WorkerID, attempt).Scan(&attempts, &completions); err != nil || attempts != 1 || completions != 0 {
		t.Fatal("fenced execution created duplicate attempts or accepted stale completion", attempts, completions, err)
	}
}
