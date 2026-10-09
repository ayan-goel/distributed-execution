//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/api"
	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/jackc/pgx/v5/pgxpool"
)

type sweepDaemonFixture struct {
	ctx          context.Context
	pool         *pgxpool.Pool
	objects      *objectstore.Store
	root, socket string
	job          spec.Job
	labels       map[string]string
	env          func(string) string
}

func newSweepDaemonFixture(t *testing.T) *sweepDaemonFixture {
	t.Helper()
	if os.Getenv("DISPATCH_TEST_S3_ENDPOINT") == "" {
		t.Skip("requires the combined PostgreSQL and object storage fixture")
	}
	configureServerTestDatabase(t)
	objects := publicationStorage(t, "agent-sweep", &publicationEvidence{})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)
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
	example, err := os.ReadFile("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	job, err := spec.DecodeJob(bytes.NewReader(example))
	if err != nil {
		t.Fatal(err)
	}
	resources := spec.Resources{CPUMillis: 500, MemoryMiB: 128, ScratchMiB: 64}
	job.Spec.Image, job.Spec.Inputs, job.Spec.Args = image, nil, nil
	job.Spec.Outputs = []spec.Output{{Name: "metrics", Path: "/outputs/evaluation.json", Required: true, MaxBytes: 65536}}
	job.Spec.Resources, job.Spec.Retry.MaxAttempts = resources, 1
	job.Spec.Placement.Labels["architecture"] = architecture
	token, _, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(api.New(pool, fixtureImageResolver(image), objects))
	t.Cleanup(httpServer.Close)
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": httpServer.URL, "DISPATCH_TOKEN": token, "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	return &sweepDaemonFixture{ctx: ctx, pool: pool, objects: objects, root: t.TempDir(), job: job, socket: socket, labels: map[string]string{"os": "linux", "architecture": architecture}, env: env}
}

func (f *sweepDaemonFixture) runCLI(t *testing.T, args ...string) []byte {
	t.Helper()
	var output, diagnostic bytes.Buffer
	if code := cli.Run(f.ctx, args, f.env, &output, &diagnostic); code != 0 {
		t.Fatal(code, diagnostic.String())
	}
	return output.Bytes()
}

func (f *sweepDaemonFixture) submit(t *testing.T, sweep spec.Sweep) store.SweepRecord {
	t.Helper()
	body, _, err := sweep.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.root, "sweep.json")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	var submitted store.SweepRecord
	if err := json.Unmarshal(f.runCLI(t, "sweep", "submit", path, "--idempotency-key", "live-sweep", "--json"), &submitted); err != nil {
		t.Fatal(err)
	}
	return submitted
}

type sweepTestDaemon struct {
	workerID string
	stop     func()
	endpoint string
	tls      *tls.Config
}

func (f *sweepDaemonFixture) startWorkers(t *testing.T, service pb.WorkerServiceServer) []sweepTestDaemon {
	t.Helper()
	pkis := []workerPKI{testWorkerPKI(t), testWorkerPKI(t), testWorkerPKI(t)}
	roots := x509.NewCertPool()
	for _, pki := range pkis {
		pem, err := os.ReadFile(pki.ca)
		if err != nil || !roots.AppendCertsFromPEM(pem) {
			t.Fatal("cannot trust fixture worker certificate", err)
		}
	}
	certificate, err := tls.LoadX509KeyPair(pkis[0].serverCert, pkis[0].serverKey)
	if err != nil {
		t.Fatal(err)
	}
	server, err := workerapi.NewServer(f.pool, certificate, roots, service)
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
	binary, err := filepath.Abs("../../.local/cargo-target/debug/dispatch-worker")
	if err != nil {
		t.Fatal(err)
	}
	logs := make([]*lockedBuffer, 0, len(pkis))
	daemons := make([]sweepTestDaemon, 0, len(pkis))
	for index, pki := range pkis {
		worker, err := store.ProvisionWorker(f.ctx, f.pool, store.WorkerProvision{Name: fmt.Sprintf("sweep-worker-%d", index), CertificateSHA256: sha256.Sum256(pki.client.Certificate[0]), Resources: f.job.Spec.Resources, Slots: 1, Labels: f.labels, Projects: []string{"research"}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			// Only unpredictable identities enrolled by this test authorize cleanup.
			output, _ := exec.Command("docker", "ps", "-aq", "--filter", "label=dev.dispatch.worker="+worker.WorkerID).Output()
			for _, id := range strings.Fields(string(output)) {
				_ = exec.Command("docker", "rm", "-f", id).Run()
			}
		})
		dir := filepath.Join(f.root, strconv.Itoa(index))
		for _, name := range []string{"state", "work"} {
			if err := os.MkdirAll(filepath.Join(dir, name), 0700); err != nil {
				t.Fatal(err)
			}
		}
		config, err := json.Marshal(map[string]any{"worker_id": worker.WorkerID, "server_url": "https://" + listener.Addr().String(), "ca_cert": pkis[0].ca, "client_cert": pki.clientCert, "client_key": pki.clientKey, "journal_dir": filepath.Join(dir, "state"), "workspace_root": filepath.Join(dir, "work"), "docker_socket": f.socket, "cpu_millis": 500, "memory_mib": 128, "scratch_mib": 64, "execution_slots": 1, "labels": f.labels})
		if err != nil {
			t.Fatal(err)
		}
		configFile := filepath.Join(dir, "worker.json")
		if err := os.WriteFile(configFile, config, 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.CommandContext(f.ctx, binary, "run", "--config", configFile, "--dev-soft-scratch")
		log := &lockedBuffer{}
		logs = append(logs, log)
		command.Stdout, command.Stderr = log, log
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		var stopped sync.Once
		stop := func() { stopped.Do(func() { _ = command.Process.Kill(); _ = command.Wait() }) }
		t.Cleanup(stop)
		daemons = append(daemons, sweepTestDaemon{workerID: worker.WorkerID, stop: stop,
			endpoint: listener.Addr().String(), tls: &tls.Config{RootCAs: pkis[0].roots, Certificates: []tls.Certificate{pki.client}, MinVersion: tls.VersionTLS13}})
	}
	t.Cleanup(func() {
		if t.Failed() {
			for index, log := range logs {
				t.Logf("worker %d: %s", index, log.String())
			}
		}
	})
	return daemons
}
