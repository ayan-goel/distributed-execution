//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func TestLostAgentJobRetriesOnAnotherWorker(t *testing.T) {
	configureServerTestDatabase(t)
	objects := publicationStorage(t, "agent-retry", &publicationEvidence{})
	serverPKI, otherPKI := testWorkerPKI(t), testWorkerPKI(t)
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
	pool, err := openPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	resources := spec.Resources{CPUMillis: 500, MemoryMiB: 128, ScratchMiB: 64}
	labels := map[string]string{"os": "linux", "architecture": architecture}
	provision := func(name string, pki workerPKI) store.WorkerIdentity {
		t.Helper()
		identity, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{
			Name: name, CertificateSHA256: sha256.Sum256(pki.client.Certificate[0]),
			Resources: resources, Slots: 1, Labels: labels, Projects: []string{"research"},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			output, _ := exec.Command("docker", "ps", "-aq", "--filter", "label=dev.dispatch.worker="+identity.WorkerID).Output()
			for _, container := range strings.Fields(string(output)) {
				_ = exec.Command("docker", "rm", "-f", container).Run()
			}
		})
		return identity
	}
	firstWorker := provision("retry-first", serverPKI)
	secondWorker := provision("retry-second", otherPKI)
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
	job.Spec.Command = []string{"sh", "-c", "sleep 10; printf abc > /outputs/result"}
	job.Spec.Args = nil
	job.Spec.Resources = resources
	job.Spec.Outputs = []spec.Output{{Name: "result", Path: "/outputs/result", Required: true, MaxBytes: 3}}
	job.Spec.Placement.Labels["architecture"] = architecture
	job.Spec.Retry.MaxAttempts = 2
	job.Spec.Retry.On = []string{"WORKER_LOST"}
	_, hash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	queued, err := store.SubmitJob(ctx, pool, uuid.NewString(), hash, job)
	if err != nil {
		t.Fatal(err)
	}
	ca1, err := os.ReadFile(serverPKI.ca)
	if err != nil {
		t.Fatal(err)
	}
	ca2, err := os.ReadFile(otherPKI.ca)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(t.TempDir(), "worker-cas.pem")
	if err := os.WriteFile(bundle, append(ca1, ca2...), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DISPATCH_S3_ACCESS_KEY", os.Getenv("DISPATCH_TEST_S3_ACCESS_KEY"))
	t.Setenv("DISPATCH_S3_SECRET_KEY", os.Getenv("DISPATCH_TEST_S3_SECRET_KEY"))
	reader, writer := io.Pipe()
	serverCtx, stopServer := context.WithCancel(ctx)
	served := make(chan error, 1)
	go func() {
		err := run(serverCtx, []string{
			"serve", "--dev-insecure", "--listen", "127.0.0.1:0", "--allow-registry", "index.docker.io",
			"--worker-listen", "127.0.0.1:0", "--worker-tls-cert", serverPKI.serverCert,
			"--worker-tls-key", serverPKI.serverKey, "--worker-client-ca", bundle,
			"--worker-dev-soft-scratch", "--object-endpoint", os.Getenv("DISPATCH_TEST_S3_ENDPOINT"),
			"--object-region", "us-east-1", "--object-bucket", "dispatch-test", "--object-dev-loopback",
		}, writer)
		_ = writer.CloseWithError(err)
		served <- err
	}()
	serverDecoder := json.NewDecoder(reader)
	workerAddress := ""
	for range 2 {
		var event struct{ Msg, Address string }
		if err := serverDecoder.Decode(&event); err != nil {
			t.Fatal("server stopped before worker listener", err)
		}
		if event.Msg == "worker_listening" {
			workerAddress = event.Address
		}
	}
	if workerAddress == "" {
		t.Fatal("server did not announce worker listener")
	}
	go func() { _, _ = io.Copy(io.Discard, reader) }()
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			stopServer()
			if err := <-served; err != nil {
				t.Error("retry server shutdown failed", err)
			}
			_ = reader.Close()
		})
	}
	defer stop()
	t.Cleanup(stop)
	binary, err := filepath.Abs("../../.local/cargo-target/debug/dispatch-worker")
	if err != nil {
		t.Fatal(err)
	}
	type workerEvent struct {
		Event     string `json:"event"`
		AttemptID string `json:"attempt_id"`
	}
	start := func(identity store.WorkerIdentity, pki workerPKI) (*json.Decoder, func()) {
		t.Helper()
		root := t.TempDir()
		for _, name := range []string{"state", "work"} {
			if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
				t.Fatal(err)
			}
		}
		config, err := json.Marshal(map[string]any{
			"worker_id": identity.WorkerID, "server_url": "https://" + workerAddress,
			"ca_cert": serverPKI.ca, "client_cert": pki.clientCert, "client_key": pki.clientKey,
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
		var once sync.Once
		stop := func() { once.Do(func() { _ = command.Process.Kill(); _ = command.Wait() }) }
		t.Cleanup(stop)
		return json.NewDecoder(stdout), stop
	}
	readEvent := func(decoder *json.Decoder, expected string) workerEvent {
		t.Helper()
		for {
			var event workerEvent
			if err := decoder.Decode(&event); err != nil {
				t.Fatal("worker stopped before", expected, err)
			}
			if event.Event == expected {
				return event
			}
		}
	}
	first, stopFirst := start(firstWorker, serverPKI)
	readEvent(first, "ready")
	oldAttempt := ""
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if containers := docker("ps", "-q", "--filter", "label=dev.dispatch.worker="+firstWorker.WorkerID); containers != "" {
			_ = pool.QueryRow(ctx, "SELECT id::text FROM attempts WHERE worker_id=$1 AND state='RUNNING'", firstWorker.WorkerID).Scan(&oldAttempt)
			if oldAttempt != "" {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if oldAttempt == "" {
		t.Fatal("first worker never entered a real running attempt")
	}
	stopFirst()
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", oldAttempt); err != nil {
		t.Fatal(err)
	}
	second, stopSecond := start(secondWorker, otherPKI)
	defer stopSecond()
	newAttempt := readEvent(second, "attempt_terminal").AttemptID
	if newAttempt == "" || newAttempt == oldAttempt {
		t.Fatal("retry did not execute under a fresh attempt", newAttempt)
	}
	var state, oldState, oldReservation, newState, newReservation string
	var accepted *string
	var attempts, completions, lossEvents int
	err = pool.QueryRow(ctx, `SELECT j.state,j.accepted_attempt_id::text,a1.state,r1.state,a2.state,r2.state,
	(SELECT count(*) FROM attempts WHERE job_id=j.id),
	(SELECT count(*) FROM attempt_completions WHERE attempt_id=a2.id),
	(SELECT count(*) FROM job_events WHERE job_id=j.id AND type='ATTEMPT_LOST')
	FROM jobs j JOIN attempts a1 ON a1.id=$2 JOIN reservations r1 ON r1.attempt_id=a1.id
	JOIN attempts a2 ON a2.id=$3 JOIN reservations r2 ON r2.attempt_id=a2.id WHERE j.id=$1`, queued.ID, oldAttempt, newAttempt).
		Scan(&state, &accepted, &oldState, &oldReservation, &newState, &newReservation, &attempts, &completions, &lossEvents)
	if err != nil || state != "SUCCEEDED" || accepted == nil || *accepted != newAttempt || oldState != "LOST" || oldReservation != "quarantined" || newState != "SUCCEEDED" || newReservation != "released" || attempts != 2 || completions != 1 || lossEvents != 1 {
		t.Fatal("lost job did not retry to one accepted result", state, accepted, oldState, oldReservation, newState, newReservation, attempts, completions, lossEvents, err)
	}
	var oldCompletions int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM attempt_completions WHERE attempt_id=$1", oldAttempt).Scan(&oldCompletions); err != nil || oldCompletions != 0 {
		t.Fatal("lost attempt published a stale completion", oldCompletions, err)
	}
	var object objectstore.Object
	if err := pool.QueryRow(ctx, `SELECT u.object_key,a.object_version,u.size_bytes,u.sha256
	FROM artifacts a JOIN artifact_uploads u ON u.upload_id=a.upload_id WHERE u.attempt_id=$1`, newAttempt).
		Scan(&object.Key, &object.Version, &object.Size, &object.SHA256); err != nil || object.Size != 3 {
		t.Fatal("retry did not publish its declared output", object, err)
	}
	if err := objects.Verify(ctx, object); err != nil {
		t.Fatal("retry artifact did not match its immutable stored version", err)
	}
}
