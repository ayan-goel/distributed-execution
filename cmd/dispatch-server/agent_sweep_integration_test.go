//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/api"
	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Observe committed acquisition state without changing scheduling decisions.
// Store concurrency tests cover the transaction invariant; this records its live use.
type sweepAcquisitionProbe struct {
	pb.WorkerServiceServer
	pool *pgxpool.Pool
	id   string
	peak atomic.Int32
}

func (s *sweepAcquisitionProbe) AcquireWork(ctx context.Context, request *pb.AcquireWorkRequest) (*pb.AcquireWorkResponse, error) {
	response, err := s.WorkerServiceServer.AcquireWork(ctx, request)
	if err != nil {
		return response, err
	}
	var active int32
	err = s.pool.QueryRow(ctx, `SELECT count(*) FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE j.sweep_id=$1 AND a.state IN ('ASSIGNED','STARTING','RUNNING','FINALIZING')`, s.id).Scan(&active)
	if err != nil {
		return nil, err
	}
	for previous := s.peak.Load(); active > previous; previous = s.peak.Load() {
		if s.peak.CompareAndSwap(previous, active) {
			break
		}
	}
	return response, nil
}

func TestWorkerDaemonsExecute27ChildSweep(t *testing.T) {
	if os.Getenv("DISPATCH_TEST_S3_ENDPOINT") == "" {
		t.Skip("requires the combined PostgreSQL and object storage fixture")
	}
	configureServerTestDatabase(t)
	objects := publicationStorage(t, "agent-sweep", &publicationEvidence{})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
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
	root := t.TempDir()
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
	// Keep work overlapping so two admitted attempts are observable with three workers.
	job.Spec.Command = []string{"sh", "-c", `sleep 2; printf '{"seed":%s,"rate":%s,"score":9007199254740993}\n' "$SEED" "$RATE" > /outputs/evaluation.json`}
	job.Spec.Outputs = []spec.Output{{Name: "metrics", Path: "/outputs/evaluation.json", Required: true, MaxBytes: 65536}}
	job.Spec.Resources, job.Spec.Retry.MaxAttempts = resources, 1
	job.Spec.Placement.Labels["architecture"] = architecture
	sweep := spec.Sweep{APIVersion: spec.APIVersion, Kind: "Sweep", Metadata: job.Metadata, Spec: spec.SweepSpec{
		JobTemplate: job, MaxConcurrent: 2,
		Matrix: map[string][]string{"ALGORITHM": {"a", "b", "c"}, "RATE": {"0.1", "0.01", "0.001"}, "SEED": {"1", "2", "3"}},
	}}
	body, _, err := sweep.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	sweepFile := filepath.Join(root, "sweep.json")
	if err := os.WriteFile(sweepFile, body, 0600); err != nil {
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
	runCLI := func(args ...string) []byte {
		t.Helper()
		var output, diagnostic bytes.Buffer
		if code := cli.Run(ctx, args, env, &output, &diagnostic); code != 0 {
			t.Fatal(code, diagnostic.String())
		}
		return output.Bytes()
	}
	var submitted, replay store.SweepRecord
	args := []string{"sweep", "submit", sweepFile, "--idempotency-key", "live-sweep", "--json"}
	if err := json.Unmarshal(runCLI(args...), &submitted); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(runCLI(args...), &replay); err != nil {
		t.Fatal(err)
	}
	if submitted.ID != replay.ID || len(submitted.ChildIDs) != 27 || !reflect.DeepEqual(submitted.ChildIDs, replay.ChildIDs) {
		t.Fatal("sweep replay changed child membership")
	}
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
	probe := &sweepAcquisitionProbe{WorkerServiceServer: workerapi.NewService(pool, store.AcquisitionPolicy{AllowSoftScratch: true}, objects), pool: pool, id: submitted.ID}
	server, err := workerapi.NewServer(pool, certificate, roots, probe)
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
	labels := map[string]string{"os": "linux", "architecture": architecture}
	for index, pki := range pkis {
		worker, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{Name: fmt.Sprintf("sweep-worker-%d", index), CertificateSHA256: sha256.Sum256(pki.client.Certificate[0]), Resources: resources, Slots: 1, Labels: labels, Projects: []string{"research"}})
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
		dir := filepath.Join(root, strconv.Itoa(index))
		for _, name := range []string{"state", "work"} {
			if err := os.MkdirAll(filepath.Join(dir, name), 0700); err != nil {
				t.Fatal(err)
			}
		}
		config, err := json.Marshal(map[string]any{"worker_id": worker.WorkerID, "server_url": "https://" + listener.Addr().String(), "ca_cert": pkis[0].ca, "client_cert": pki.clientCert, "client_key": pki.clientKey, "journal_dir": filepath.Join(dir, "state"), "workspace_root": filepath.Join(dir, "work"), "docker_socket": socket, "cpu_millis": 500, "memory_mib": 128, "scratch_mib": 64, "execution_slots": 1, "labels": labels})
		if err != nil {
			t.Fatal(err)
		}
		configFile := filepath.Join(dir, "worker.json")
		if err := os.WriteFile(configFile, config, 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.CommandContext(ctx, binary, "run", "--config", configFile, "--dev-soft-scratch")
		log := &lockedBuffer{}
		logs = append(logs, log)
		command.Stdout, command.Stderr = log, log
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	}
	defer func() {
		if t.Failed() {
			for index, log := range logs {
				t.Logf("worker %d: %s", index, log.String())
			}
		}
	}()
	for {
		var page client.SweepPage
		if err := json.Unmarshal(runCLI("sweep", "get", submitted.ID, "--limit", "7", "--json"), &page); err != nil {
			t.Fatal(err)
		}
		if page.Sweep.Progress.Failed > 0 || page.Sweep.Progress.Cancelled > 0 {
			t.Fatal("sweep child did not succeed", page.Sweep.Progress)
		}
		if page.Sweep.State == "SUCCEEDED" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("live sweep timed out", page.Sweep.Progress)
		case <-time.After(200 * time.Millisecond):
		}
	}
	if probe.peak.Load() != 2 {
		t.Fatal("live sweep did not exercise its concurrency cap", probe.peak.Load())
	}
	var attempts, workers, held int
	err = pool.QueryRow(ctx, `SELECT count(*),count(DISTINCT a.worker_id),count(*) FILTER(WHERE r.state<>'released') FROM attempts a JOIN jobs j ON j.id=a.job_id JOIN reservations r ON r.attempt_id=a.id WHERE j.sweep_id=$1`, submitted.ID).Scan(&attempts, &workers, &held)
	if err != nil || attempts != 27 || workers < 2 || held != 0 {
		t.Fatal("incorrect execution or terminal release", attempts, workers, held, err)
	}
	var results client.SweepResults
	if err := json.Unmarshal(runCLI("sweep", "export", submitted.ID), &results); err != nil {
		t.Fatal(err)
	}
	if results.SweepID != submitted.ID || len(results.Children) != 27 {
		t.Fatal("incomplete live export")
	}
	for index, child := range results.Children {
		parameters := map[string]string{"ALGORITHM": []string{"a", "b", "c"}[index/9], "RATE": []string{"0.1", "0.01", "0.001"}[index/3%3], "SEED": strconv.Itoa(index%3 + 1)}
		if child.Index != index || child.ID != submitted.ChildIDs[index] || child.State != "SUCCEEDED" || child.AcceptedAttemptID == nil || !reflect.DeepEqual(child.Parameters, parameters) || len(child.Metrics) != 3 || child.Metrics["seed"].String() != parameters["SEED"] || child.Metrics["rate"].String() != parameters["RATE"] || child.Metrics["score"].String() != "9007199254740993" {
			t.Fatal("live export lost stable parameters or accepted metric precision", child)
		}
	}
	rows, err := csv.NewReader(bytes.NewReader(runCLI("sweep", "export", submitted.ID, "--format", "csv"))).ReadAll()
	if err != nil || len(rows) != 28 {
		t.Fatal("incomplete live CSV", len(rows), err)
	}
	if !reflect.DeepEqual(rows[0], []string{"index", "jobId", "state", "acceptedAttemptId", "parameters", "metric.rate", "metric.score", "metric.seed"}) {
		t.Fatal("incorrect metric columns", rows[0])
	}
	for index, row := range rows[1:] {
		child := results.Children[index]
		parameters, _ := json.Marshal(child.Parameters)
		if !reflect.DeepEqual(row, []string{strconv.Itoa(index), child.ID, child.State, *child.AcceptedAttemptID, string(parameters), child.Metrics["rate"].String(), "9007199254740993", child.Metrics["seed"].String()}) {
			t.Fatal("CSV disagrees with accepted JSON results", row)
		}
	}
	var children []client.SweepChild
	cursor := ""
	for {
		var page client.SweepPage
		args := []string{"sweep", "get", submitted.ID, "--limit", "7", "--json"}
		if cursor != "" {
			args = append(args, "--cursor", cursor)
		}
		if err := json.Unmarshal(runCLI(args...), &page); err != nil {
			t.Fatal(err)
		}
		children = append(children, page.Children...)
		if !page.HasMore {
			break
		}
		cursor = page.NextCursor
	}
	if !reflect.DeepEqual(children, results.Children) {
		t.Fatal("live pagination disagrees with export")
	}
	destination := filepath.Join(root, "downloaded-metrics.json")
	runCLI("artifacts", "download", submitted.ChildIDs[0], "metrics", "--output", destination, "--json")
	download, err := os.ReadFile(destination)
	if err != nil || string(download) != "{\"seed\":1,\"rate\":0.1,\"score\":9007199254740993}\n" {
		t.Fatal("metric artifact download changed source bytes", string(download), err)
	}
	t.Logf("completed 27 children across %d local worker identities; observed peak=%d, JSON/CSV/pagination/artifact checks passed", workers, probe.peak.Load())
}
