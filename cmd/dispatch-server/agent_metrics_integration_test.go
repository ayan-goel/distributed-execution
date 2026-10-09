//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/api"
	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
)

func TestWorkerDaemonPublishesMetrics(t *testing.T) {
	if os.Getenv("DISPATCH_TEST_S3_ENDPOINT") == "" {
		t.Skip("requires the combined PostgreSQL and object storage fixture")
	}
	for _, tc := range []struct {
		name, body, state string
		outage            bool
	}{
		{"valid", " {\"score\":9007199254740993,\"loss\":1e-3}\n", "SUCCEEDED", false},
		{"malformed", `{"score":"not-a-number"}`, "FAILED", false},
		{"oversized", strings.Repeat(" ", 65537), "FAILED", false},
		{"malformed_storage_outage", `{"score":"not-a-number"}`, "FAILED", true},
	} {
		t.Run(tc.name, func(t *testing.T) { testWorkerDaemonMetric(t, tc.body, tc.state, tc.outage) })
	}
}

func testWorkerDaemonMetric(t *testing.T, body, wantState string, outage bool) {
	t.Helper()
	configureServerTestDatabase(t)
	publication := &publicationEvidence{}
	mode := "agent-metrics"
	if outage {
		mode = "publish_transfer_failed"
	}
	objects := publicationStorage(t, mode, publication)
	pki := testWorkerPKI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
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
	t.Cleanup(pool.Close)
	resources := spec.Resources{CPUMillis: 500, MemoryMiB: 128, ScratchMiB: 64}
	labels := map[string]string{"os": "linux", "architecture": architecture}
	worker, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{Name: "metric-worker", CertificateSHA256: sha256.Sum256(pki.client.Certificate[0]), Resources: resources, Slots: 1, Labels: labels, Projects: []string{"research"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Only this fixture's random worker identity authorizes container cleanup.
		output, _ := exec.Command("docker", "ps", "-aq", "--filter", "label=dev.dispatch.worker="+worker.WorkerID).Output()
		for _, id := range strings.Fields(string(output)) {
			_ = exec.Command("docker", "rm", "-f", id).Run()
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
	job.Spec.Command = []string{"sh", "-c", "printf '%s' '" + body + "' > /outputs/evaluation.json"}
	if len(body) > 64<<10 {
		body = strings.Repeat("\x00", 65537)
		job.Spec.Command = []string{"sh", "-c", "head -c 65537 /dev/zero > /outputs/evaluation.json"}
	}
	job.Spec.Outputs = []spec.Output{{Name: "metrics", Path: "/outputs/evaluation.json", Required: true, MaxBytes: 65537}}
	job.Spec.Resources, job.Spec.Retry.MaxAttempts = resources, 1
	job.Spec.Placement.Labels["architecture"] = architecture
	canonical, _, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	jobFile := filepath.Join(root, "job.json")
	if err := os.WriteFile(jobFile, canonical, 0600); err != nil {
		t.Fatal(err)
	}
	token, _, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(api.New(pool, fixtureImageResolver(image), objects))
	t.Cleanup(httpServer.Close)
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": httpServer.URL, "DISPATCH_TOKEN": token, "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	var output, diagnostic bytes.Buffer
	if code := cli.Run(ctx, []string{"submit", jobFile, "--json"}, env, &output, &diagnostic); code != 0 {
		t.Fatal(code, diagnostic.String())
	}
	var submitted store.JobRecord
	if err := json.Unmarshal(output.Bytes(), &submitted); err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.LoadX509KeyPair(pki.serverCert, pki.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	var service pb.WorkerServiceServer = workerapi.NewService(pool, store.AcquisitionPolicy{AllowSoftScratch: true}, objects)
	var replay *launchService
	if wantState == "SUCCEEDED" {
		// Lose real upload/finalization/completion replies after their commits.
		// Existing fault hooks reject any retry that rewrites durable evidence.
		replay = &launchService{WorkerServiceServer: service, pool: pool, t: t, mode: "agent", publication: publication}
		service = replay
	}
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
	for _, name := range []string{"state", "work"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	config, err := json.Marshal(map[string]any{"worker_id": worker.WorkerID, "server_url": "https://" + listener.Addr().String(), "ca_cert": pki.ca, "client_cert": pki.clientCert, "client_key": pki.clientKey, "journal_dir": filepath.Join(root, "state"), "workspace_root": filepath.Join(root, "work"), "docker_socket": socket, "cpu_millis": 500, "memory_mib": 128, "scratch_mib": 64, "execution_slots": 1, "labels": labels})
	if err != nil {
		t.Fatal(err)
	}
	configFile := filepath.Join(root, "worker.json")
	if err := os.WriteFile(configFile, config, 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := filepath.Abs("../../.local/cargo-target/debug/dispatch-worker")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, binary, "run", "--config", configFile, "--dev-soft-scratch")
	var logs lockedBuffer
	command.Stdout, command.Stderr = &logs, &logs
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	var state, reason string
	var accepted *string
	var manifest []byte
	for {
		err := pool.QueryRow(ctx, `SELECT j.state,j.accepted_attempt_id::text,j.accepted_manifest,coalesce(a.reason,'') FROM jobs j LEFT JOIN attempts a ON a.job_id=j.id WHERE j.id=$1`, submitted.ID).Scan(&state, &accepted, &manifest, &reason)
		if err != nil {
			t.Fatal("metric job did not finish", err, logs.String())
		}
		if state == "SUCCEEDED" || state == "FAILED" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("metric job did not finish", state, logs.String())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if state != wantState {
		t.Fatal("incorrect metric outcome", state, reason, logs.String())
	}
	if wantState == "SUCCEEDED" {
		var result struct {
			Metrics           map[string]json.Number `json:"metrics"`
			MetricsArtifactID string                 `json:"metricsArtifactId"`
		}
		if json.Unmarshal(manifest, &result) != nil || accepted == nil || result.MetricsArtifactID == "" || result.Metrics["score"] != json.Number("9007199254740993") || result.Metrics["loss"] != json.Number("0.001") {
			t.Fatal("real metric source was missing or rounded", string(manifest))
		}
		for {
			replay.mu.Lock()
			completed := publication.completions >= 2
			original := string(publication.complete.GetMetricsJson())
			replay.mu.Unlock()
			if completed {
				if original != body {
					t.Fatal("metric replay changed original source bytes")
				}
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("metric completion did not replay after lost reply", logs.String())
			case <-time.After(100 * time.Millisecond):
			}
		}
	} else if accepted != nil || len(manifest) != 0 || reason != "OUTPUT_INVALID" {
		t.Fatal("invalid metrics became canonical", accepted, reason)
	}
	if outage {
		if publication.puts.Load() == 0 {
			t.Fatal("outage fixture did not attempt diagnostic transfer")
		}
		return
	}
	var object objectstore.Object
	if err := pool.QueryRow(ctx, `SELECT u.object_key,a.object_version,u.size_bytes,u.sha256 FROM artifacts a JOIN artifact_uploads u ON u.upload_id=a.upload_id JOIN attempts x ON x.id=u.attempt_id WHERE x.job_id=$1 AND u.logical_name='metrics'`, submitted.ID).Scan(&object.Key, &object.Version, &object.Size, &object.SHA256); err != nil {
		t.Fatal("original metric artifact was not retained", err)
	}
	hash := sha256.Sum256([]byte(body))
	if object.Size != int64(len(body)) || object.SHA256 != fmt.Sprintf("%x", hash) {
		t.Fatal("metric artifact changed source identity")
	}
	if err := objects.Verify(ctx, object); err != nil {
		t.Fatal("real metric artifact not verified", err)
	}
}
