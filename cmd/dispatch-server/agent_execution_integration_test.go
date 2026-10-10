//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/api"
	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
)

type fixtureImageResolver string

func (r fixtureImageResolver) Resolve(_ context.Context, reference string) (string, error) {
	// Keep this end-to-end gate deterministic with a digest already cached in
	// Docker. Registry resolution is verified separately from workload execution.
	if reference != string(r) {
		return "", errors.New("unexpected test image")
	}
	return reference, nil
}

func TestWorkerDaemonAcquiresExecutesAndPublishes(t *testing.T) {
	testWorkerDaemonExecution(t, false)
}

func TestWorkerDaemonFinishesExistingJobAfterOperatorDrain(t *testing.T) {
	testWorkerDaemonExecution(t, true)
}

func testWorkerDaemonExecution(t *testing.T, drainDuringRun bool) {
	t.Helper()
	configureServerTestDatabase(t)
	publication := &publicationEvidence{}
	objects := publicationStorage(t, "agent", publication)
	pki := testWorkerPKI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
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
	job.Spec.Inputs = []spec.Input{{Dataset: "sample-v1", MountPath: "/inputs/data"}}
	job.Spec.Image = image
	job.Spec.Command = []string{"sh", "-c", "cat /inputs/data/input.txt > /outputs/result; printf 'run out'; head -c 3300000 /dev/zero; printf 'run err' >&2; sleep 15; exit 0"}
	if drainDuringRun {
		// Keep execution live until the agent observes drain. A bounded barrier
		// removes the race between heartbeat/storage latency and a fixed sleep.
		job.Spec.Command[2] = strings.Replace(job.Spec.Command[2], "sleep 15; exit 0", `i=0; until [ -f /outputs/release ]; do [ "$i" -lt 400 ] || exit 97; i=$((i+1)); sleep 0.1; done; exit 0`, 1)
	}
	job.Spec.Retry.MaxAttempts = 1
	job.Spec.Priority = 3
	job.Spec.Outputs = []spec.Output{{Name: "result", Path: "/outputs/result", Required: true, MaxBytes: 3}}
	job.Spec.Args = nil
	job.Spec.Resources = resources
	job.Spec.Placement.Labels["architecture"] = architecture
	canonical, _, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	jobFile := filepath.Join(t.TempDir(), "job.json")
	if err := os.WriteFile(jobFile, canonical, 0600); err != nil {
		t.Fatal(err)
	}
	token, _, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(api.New(pool, fixtureImageResolver(image), objects))
	t.Cleanup(httpServer.Close)
	cliEnv := func(key string) string {
		return map[string]string{"DISPATCH_URL": httpServer.URL, "DISPATCH_TOKEN": token, "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	cliRun := func(args ...string) []byte {
		t.Helper()
		var output, diagnostic bytes.Buffer
		if code := cli.Run(ctx, args, cliEnv, &output, &diagnostic); code != 0 {
			t.Fatalf("CLI failed with code %d: %s", code, diagnostic.String())
		}
		return output.Bytes()
	}
	datasetDir := filepath.Join(t.TempDir(), "dataset")
	if err := os.Mkdir(datasetDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(datasetDir, "input.txt"), []byte("abc"), 0600); err != nil {
		t.Fatal(err)
	}
	cliRun("dataset", "upload", datasetDir, "--name", "sample-v1", "--json")
	var submitted store.JobRecord
	if err := json.Unmarshal(cliRun("submit", jobFile, "--idempotency-key", "agent-execution", "--json"), &submitted); err != nil || submitted.State != "QUEUED" {
		t.Fatal("CLI submission did not queue the job", err, submitted.State)
	}
	var priority int
	if err := pool.QueryRow(ctx, "SELECT priority FROM jobs WHERE id=$1", submitted.ID).Scan(&priority); err != nil || priority != 3 {
		t.Fatal("CLI admission lost job priority", priority, err)
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
	t.Cleanup(func() {
		// Staged cache directories are sealed read-only. Reopen only this test's
		// root after execution so TempDir can remove verified bytes.
		err := filepath.WalkDir(filepath.Join(root, "work", ".dataset-cache"), func(path string, entry os.DirEntry, walkErr error) error {
			if os.IsNotExist(walkErr) {
				return nil
			}
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return os.Chmod(path, 0700)
			}
			return nil
		})
		if err != nil {
			t.Error("cannot reopen isolated dataset cache", err)
		}
	})
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
	var liveState string
	var liveSegments int
	liveDeadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(liveDeadline) {
		err := pool.QueryRow(ctx, `SELECT coalesce((SELECT a.state FROM attempts a
			WHERE a.job_id=j.id ORDER BY a.attempt_number DESC LIMIT 1),''),
			(SELECT count(*) FROM log_segments s
			JOIN attempts a ON a.id=s.attempt_id WHERE a.job_id=j.id)
			FROM jobs j WHERE j.id=$1`, submitted.ID).Scan(&liveState, &liveSegments)
		if err != nil {
			t.Fatal(err)
		}
		if liveSegments > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if liveSegments == 0 || liveState != "RUNNING" {
		t.Fatal("first sealed log segment was not published during execution", liveState, liveSegments)
	}
	if liveLogs := string(cliRun("logs", submitted.ID)); !strings.Contains(liveLogs, "[stdout #1] run out") {
		t.Fatal("CLI could not read the live verified log segment")
	}
	var queuedAfterDrain store.JobRecord
	if drainDuringRun {
		operator, _, err := store.IssueToken(ctx, pool, "research", store.RoleOperator)
		if err != nil {
			t.Fatal(err)
		}
		var out, errs bytes.Buffer
		code := cli.Run(ctx, []string{"workers", "drain", worker.WorkerID, "--json"}, func(key string) string {
			if key == "DISPATCH_TOKEN" {
				return operator
			}
			return cliEnv(key)
		}, &out, &errs)
		if code != 0 {
			t.Fatal("live worker drain failed", code, errs.String())
		}
		if err := json.Unmarshal(cliRun("submit", jobFile, "--idempotency-key", "after-drain", "--json"), &queuedAfterDrain); err != nil {
			t.Fatal(err)
		}
	}
	decoder := json.NewDecoder(stdout)
	var attempt string
	sawDraining := false
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
		if drainDuringRun && event.Event == "draining" && !sawDraining {
			var container string
			if err := pool.QueryRow(ctx, `SELECT a.container_id FROM attempts a
				JOIN jobs j ON j.current_attempt_id=a.id
				WHERE j.id=$1 AND a.worker_id=$2 AND a.state='RUNNING'`, submitted.ID, worker.WorkerID).Scan(&container); err != nil {
				t.Fatal("drain signal did not preserve RUNNING authority", err)
			}
			// Release only this fixture's worker-bound, live container. The workload
			// then exits normally and exercises the unchanged publication path.
			docker("exec", container, "sh", "-c", "touch /outputs/release")
		}
		sawDraining = sawDraining || event.Event == "draining"
	}
	if drainDuringRun && !sawDraining {
		t.Fatal("worker did not observe the heartbeat drain signal before completion")
	}
	service.mu.Lock()
	if !service.replayed || !service.running || !service.finalReplayed || publication.complete == nil || len(publication.complete.Outputs) != 1 {
		service.mu.Unlock()
		t.Fatal("worker missed replay, execution phase, or publication", diagnostic.String())
	}
	artifact := publication.complete.Outputs[0].ArtifactId
	service.mu.Unlock()
	verifyPublication(t, ctx, pool, objects, publication, attempt, artifact, "SUCCEEDED", true)
	var stdoutBytes, stdoutSegments int64
	if err := pool.QueryRow(ctx, `SELECT coalesce(sum(u.size_bytes),0),count(*) FROM log_segments s
		JOIN artifact_uploads u ON u.upload_id=s.upload_id WHERE s.attempt_id=$1 AND s.stream='STDOUT'`, attempt).Scan(&stdoutBytes, &stdoutSegments); err != nil {
		t.Fatal(err)
	}
	if stdoutBytes < 3300000 || stdoutSegments < 4 {
		t.Fatal("noisy stdout was not captured across multiple segments", stdoutBytes, stdoutSegments)
	}
	if publication.logCreates < 5 || publication.logFinalizes != publication.logCreates || publication.logRegistrations != publication.logCreates {
		t.Fatal("worker did not publish both verified log streams", publication.logCreates, publication.logFinalizes, publication.logRegistrations, stdoutBytes, stdoutSegments)
	}
	logs := string(cliRun("logs", submitted.ID))
	if !strings.Contains(logs, "[stdout #1] run out") || !strings.Contains(logs, "[stderr #1] run err") || strings.Contains(logs, "[logs incomplete") {
		t.Fatal("CLI did not render the verified log objects", logs)
	}
	var completed store.JobRecord
	if err := json.Unmarshal(cliRun("jobs", "get", submitted.ID, "--json"), &completed); err != nil || completed.State != "SUCCEEDED" {
		t.Fatal("CLI did not observe the accepted result", err, completed.State)
	}
	download := filepath.Join(root, "downloaded-result")
	cliRun("artifacts", "download", submitted.ID, "result", "--output", download, "--json")
	if body, err := os.ReadFile(download); err != nil || string(body) != "abc" {
		t.Fatal("CLI download did not verify the real workload bytes", err, string(body))
	}
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
	if drainDuringRun {
		var before, after time.Time
		if err := pool.QueryRow(ctx, "SELECT last_heartbeat_at FROM workers WHERE id=$1", worker.WorkerID).Scan(&before); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			if err := pool.QueryRow(ctx, "SELECT last_heartbeat_at FROM workers WHERE id=$1", worker.WorkerID).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if after.After(before) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		var state string
		var counter int64
		if err := pool.QueryRow(ctx, "SELECT state,attempt_counter FROM jobs WHERE id=$1", queuedAfterDrain.ID).Scan(&state, &counter); err != nil || state != "QUEUED" || counter != 0 || !after.After(before) {
			t.Fatal("drained agent acquired queued work or stopped heartbeating", state, counter, err)
		}
	}
}
