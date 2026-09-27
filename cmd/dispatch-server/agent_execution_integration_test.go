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

	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/google/uuid"
)

func TestWorkerDaemonAcquiresExecutesAndPublishes(t *testing.T) {
	configureServerTestDatabase(t)
	publication := &publicationEvidence{}
	objects := publicationStorage(t, "agent", publication)
	pki := testWorkerPKI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
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
		Name: "agent-execution-worker", CertificateSHA256: sha256.Sum256(pki.client.Certificate[0]),
		Resources: resources, Slots: 1, Labels: labels, Projects: []string{"research"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Only this worker's labeled containers belong to this fixture.
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
	job.Spec.Command = []string{"sh", "-c", "printf abc > /outputs/result; sleep 2; exit 0"}
	job.Spec.Retry.MaxAttempts = 1
	job.Spec.Outputs = []spec.Output{{Name: "result", Path: "/outputs/result", Required: true, MaxBytes: 3}}
	job.Spec.Args = nil
	job.Spec.Resources = resources
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
	service := &launchService{WorkerServiceServer: workerapi.NewService(pool, store.AcquisitionPolicy{AllowSoftScratch: true}, objects), pool: pool, t: t, mode: "agent", publication: publication}
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
	var diagnostic bytes.Buffer
	command.Stderr = &diagnostic
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	decoder := json.NewDecoder(stdout)
	var attempt string
	for attempt == "" {
		var event struct {
			Event     string `json:"event"`
			AttemptID string `json:"attempt_id"`
		}
		if err := decoder.Decode(&event); err != nil {
			t.Fatal("worker stopped before completion", err, diagnostic.String())
		}
		if event.Event == "attempt_terminal" {
			attempt = event.AttemptID
		}
	}
	service.mu.Lock()
	if !service.replayed || !service.running || !service.finalReplayed || publication.complete == nil || len(publication.complete.Outputs) != 1 {
		service.mu.Unlock()
		t.Fatal("worker missed replay, execution phase, or publication", diagnostic.String())
	}
	artifact := publication.complete.Outputs[0].ArtifactId
	service.mu.Unlock()
	verifyPublication(t, ctx, pool, objects, publication, attempt, artifact, "SUCCEEDED")
	var attempts int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM attempts WHERE worker_id=$1", worker.WorkerID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatal("one job produced an unexpected number of attempts", attempts, err)
	}
	if _, err := os.Stat(filepath.Join(root, "work", attempt)); !os.IsNotExist(err) {
		t.Fatal("attempt workspace survived terminal cleanup", err)
	}
	if containers := docker("ps", "-aq", "--filter", "label=dev.dispatch.worker="+worker.WorkerID); containers != "" {
		t.Fatal("terminal container survived cleanup", containers)
	}
}
