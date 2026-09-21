//go:build integration

package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type launchService struct {
	publication *publicationEvidence
	pb.WorkerServiceServer
	pool                           *pgxpool.Pool
	t                              *testing.T
	mu                             sync.Mutex
	starting                       *pb.ReportPhaseRequest
	replayed, running, fenced      bool
	mode                           string
	finalizing                     *pb.ReportPhaseRequest
	finalReplayed, finalRenewed    bool
	runningDeadline                time.Time
	finalizedBeforeRunningDeadline bool
}

func (s *launchService) ReportPhase(ctx context.Context, r *pb.ReportPhaseRequest) (*pb.MutationResponse, error) {
	reply, err := s.WorkerServiceServer.ReportPhase(ctx, r)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if r.Phase == pb.AttemptState_STARTING {
		if s.starting == nil {
			// Lose a reply only after the real state transition commits. Launch must
			// replay the journaled event before creating its container.
			s.starting = proto.Clone(r).(*pb.ReportPhaseRequest)
			s.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "injected lost STARTING reply")
		}
		if !proto.Equal(s.starting, r) {
			s.t.Error("STARTING retry changed durable identity")
		}
		s.replayed = true
	}
	if r.Phase == pb.AttemptState_RUNNING && reply.Decision == pb.Decision_ACCEPTED {
		s.running = true
		if s.mode == "uncertain_running" {
			// Commit RUNNING, then withhold its reply until the Rust coordinator
			// observes process exit and cancels this RPC to report FINALIZING.
			s.runningDeadline, _ = ctx.Deadline()
			s.mu.Unlock()
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	if r.Phase == pb.AttemptState_FINALIZING && reply.Decision == pb.Decision_ACCEPTED {
		if s.finalizing == nil {
			if s.mode == "uncertain_running" {
				s.finalizedBeforeRunningDeadline = !s.runningDeadline.IsZero() && time.Now().Before(s.runningDeadline)
			}
			s.finalizing = proto.Clone(r).(*pb.ReportPhaseRequest)
			s.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "injected lost FINALIZING reply")
		}
		if !proto.Equal(s.finalizing, r) {
			s.t.Error("FINALIZING retry changed durable identity")
		}
		s.finalReplayed = true
	}
	s.mu.Unlock()
	return reply, nil
}
func (s *launchService) RenewLeases(ctx context.Context, r *pb.RenewLeasesRequest) (*pb.RenewLeasesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running && s.mode == "fenced" {
		if _, err := s.pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE worker_id=$1", r.GetSession().GetWorkerId()); err != nil {
			return nil, err
		}
	}
	reply, err := s.WorkerServiceServer.RenewLeases(ctx, r)
	if err == nil && s.running {
		for _, result := range reply.Results {
			if result.Decision == pb.Decision_FENCED {
				s.fenced = true
			}
			if result.Decision == pb.Decision_ACCEPTED && s.finalReplayed {
				s.finalRenewed = true
			}
		}
	}
	return reply, err
}

func TestRustDurableLaunchAndRenewalStopsRealContainerAfterFencing(t *testing.T) {
	testRustLaunchScenario(t, "fenced")
}

func TestRustExecutionReplaysFinalizationAndRetainsRealExitEvidence(t *testing.T) {
	testRustLaunchScenario(t, "finalize")
}

func TestRustExecutionObservesExitWhileRunningReplyIsUncertain(t *testing.T) {
	testRustLaunchScenario(t, "uncertain_running")
}

func testRustLaunchScenario(t *testing.T, mode string) {
	t.Helper()
	configureServerTestDatabase(t)
	publish := strings.HasPrefix(mode, "publish")
	publication := &publicationEvidence{}
	var objects *objectstore.Store
	if publish {
		objects = publicationStorage(t, mode, publication)
	}
	p := testWorkerPKI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	id, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{Name: "launch-worker", CertificateSHA256: sha256.Sum256(p.client.Certificate[0]), Resources: resources, Slots: 1, Labels: labels, Projects: []string{"research"}})
	if err != nil {
		t.Fatal(err)
	}
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
	if mode != "fenced" {
		job.Spec.Command = []string{"sh", "-c", "printf abc > /outputs/result; sleep 2; exit 7"}
	}
	if publish && mode != "publish_failed" && mode != "publish_failed_missing" {
		job.Spec.Command = []string{"sh", "-c", "printf abc > /outputs/result; sleep 2; exit 0"}
	}
	if mode == "publish_missing" {
		job.Spec.Command = []string{"sh", "-c", "sleep 2; exit 0"}
	}
	if mode == "publish_failed_missing" {
		job.Spec.Command = []string{"sh", "-c", "sleep 2; exit 7"}
	}
	if mode == "publish_oversized" {
		job.Spec.Command = []string{"sh", "-c", "printf abcdef > /outputs/result; sleep 2; exit 0"}
	}
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
	root := t.TempDir()
	for _, name := range []string{"state", "work"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("DISPATCH_LAUNCH_STATE", filepath.Join(root, "state"))
	t.Setenv("DISPATCH_LAUNCH_WORK", filepath.Join(root, "work"))
	t.Setenv("DISPATCH_LAUNCH_SOCKET", socket)
	if publish {
		t.Setenv("DISPATCH_LAUNCH_MODE", "publish")
	} else if mode != "fenced" {
		t.Setenv("DISPATCH_LAUNCH_MODE", "finalize")
	} else {
		t.Setenv("DISPATCH_LAUNCH_MODE", "launch")
	}
	t.Cleanup(func() {
		// Remove only this fixture's unpredictable worker identity. Other Docker
		// workloads, images, volumes, and workspaces are outside this cleanup.
		output, _ := exec.Command("docker", "ps", "-aq", "--filter", "label=dev.dispatch.worker="+id.WorkerID).Output()
		for _, container := range strings.Fields(string(output)) {
			_ = exec.Command("docker", "rm", "-f", container).Run()
		}
	})
	certificate, err := tls.LoadX509KeyPair(p.serverCert, p.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	service := &launchService{WorkerServiceServer: workerapi.NewService(pool, store.AcquisitionPolicy{AllowSoftScratch: true}, objects), pool: pool, t: t, mode: mode, publication: publication}
	server, err := workerapi.NewServer(pool, certificate, p.roots, service)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	claims := &pb.RegisterWorkerRequest{WorkerId: id.WorkerID, ProtocolVersion: 1, Allocatable: &pb.Resources{CpuMillis: 500, MemoryBytes: 128 << 20, ScratchBytes: 64 << 20}, ExecutionSlots: 1, Labels: labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.soft"}}
	output, diagnostic, err := rustProtocolProbe(t, "launch_probe", listener.Addr().String(), p, claims)
	if err != nil {
		t.Fatal("real launch failed", err, diagnostic)
	}
	var result struct {
		Attempt            string `json:"attempt_id"`
		Container          string `json:"container_id"`
		Exit               int    `json:"exit_code"`
		OOM                bool   `json:"oom_killed"`
		CompletionDecision int32  `json:"completion_decision"`
		Outputs            []struct {
			Name     string `json:"name"`
			Size     int64  `json:"size_bytes"`
			SHA256   string `json:"sha256"`
			Artifact string `json:"artifact_id"`
		} `json:"outputs"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(result.Attempt); err != nil || len(result.Container) != 64 {
		t.Fatal("invalid launch evidence", result, err)
	}
	if state := docker("inspect", "--format", "{{.State.Running}}", result.Container); state != "false" {
		t.Fatal("fenced workload is still running", state)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if !service.replayed || !service.running || (mode == "fenced" && !service.fenced) {
		t.Fatal("launch missed replay, running, or fencing", service.replayed, service.running, service.fenced)
	}
	var phaseCount int
	var state string
	wantState, wantCount := "RUNNING", 2
	if mode != "fenced" {
		wantState, wantCount = "FINALIZING", 3
		wantExit := 7
		if publish && mode != "publish_failed" && mode != "publish_failed_missing" {
			wantExit = 0
			wantState = "SUCCEEDED"
		}
		if publish && mode != "publish" {
			wantState = "FAILED"
		}
		invalidOutput := mode == "publish_missing" || mode == "publish_failed_missing" || mode == "publish_oversized"
		if invalidOutput && len(result.Outputs) != 0 {
			t.Fatal("invalid output was journaled", result.Outputs)
		}
		if !invalidOutput && (len(result.Outputs) != 1 || result.Outputs[0].Name != "result" || result.Outputs[0].Size != 3 || result.Outputs[0].SHA256 != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad") {
			t.Fatal("missing verified workload output", result.Outputs)
		}
		if !service.finalReplayed || !service.finalRenewed || result.Exit != wantExit || result.OOM {
			t.Fatal("missing finalization retry, fresh authority, or real exit evidence", service.finalReplayed, service.finalRenewed, result)
		}
		if mode == "uncertain_running" && !service.finalizedBeforeRunningDeadline {
			t.Fatal("finalization waited for the stalled RUNNING RPC deadline")
		}
		var exit int
		if err := pool.QueryRow(ctx, "SELECT exit_code FROM attempts WHERE id=$1", result.Attempt).Scan(&exit); err != nil || exit != wantExit {
			t.Fatal("database lost exit evidence", exit, err)
		}
		if publish {
			if result.CompletionDecision != int32(pb.Decision_ACCEPTED) {
				t.Fatal("completion not acknowledged", result.CompletionDecision)
			}
			if invalidOutput || mode == "publish_transfer_failed" {
				verifyFailedPublication(t, ctx, pool, service.publication, result.Attempt, mode)
			} else {
				verifyPublication(t, ctx, pool, objects, service.publication, result.Attempt, result.Outputs[0].Artifact, wantState)
			}
		}
	}
	if err := pool.QueryRow(ctx, "SELECT state,(SELECT count(*) FROM attempt_phase_reports WHERE attempt_id=a.id) FROM attempts a WHERE id=$1", result.Attempt).Scan(&state, &phaseCount); err != nil || state != wantState || phaseCount != wantCount {
		t.Fatal("unexpected launch phase history", state, phaseCount, err)
	}
}
