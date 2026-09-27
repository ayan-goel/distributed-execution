//go:build integration

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedServerReaperAttempt(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	pki := testWorkerPKI(t)
	resources := spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}
	labels := map[string]string{"os": "linux", "architecture": "amd64"}
	identity, err := store.ProvisionWorker(ctx, pool, store.WorkerProvision{
		Name: "reaper-worker", CertificateSHA256: sha256.Sum256(pki.client.Certificate[0]),
		Resources: resources, Slots: 1, Labels: labels, Projects: []string{"research"},
	})
	if err != nil {
		t.Fatal(err)
	}
	registration := store.Registration{RequestID: uuid.NewString(), SessionID: uuid.NewString(), ProtocolVersion: 1, Resources: resources, Slots: 1, Labels: labels, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.quota"}}
	if _, err := store.RegisterSession(ctx, pool, identity, registration); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordHeartbeat(ctx, pool, identity, store.HeartbeatReport{RequestID: uuid.NewString(), SessionID: registration.SessionID, Sequence: 1, RuntimeHealthy: true, ReconciliationComplete: true}); err != nil {
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
	job.Spec.Image = "example.org/test@sha256:" + strings.Repeat("a", 64)
	_, hash, err := job.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SubmitJob(ctx, pool, uuid.NewString(), hash, job); err != nil {
		t.Fatal(err)
	}
	result, err := store.AcquireWork(ctx, pool, identity, store.AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, store.AcquisitionPolicy{})
	if err != nil || result.Assignment == nil {
		t.Fatal("fixture could not acquire its durable attempt", err, result.NoWorkReason)
	}
	return result.Assignment.Authority.AttemptID
}

func startReaperServer(t *testing.T) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := run(ctx, []string{"serve", "--dev-insecure", "--listen", "127.0.0.1:0", "--allow-registry", "index.docker.io"}, writer)
		_ = writer.CloseWithError(err)
		done <- err
	}()
	var event struct{ Msg string }
	if err := json.NewDecoder(reader).Decode(&event); err != nil || event.Msg != "http_listening" {
		cancel()
		t.Fatal("reaper server did not start", err, event.Msg)
	}
	go func() { _, _ = io.Copy(io.Discard, reader) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-done; err != nil {
			t.Error("reaper server shutdown failed", err)
		}
		_ = reader.Close()
	}
	t.Cleanup(stop)
	return stop
}

func TestServerReapsExpiredAttemptBeforeListening(t *testing.T) {
	configureServerTestDatabase(t)
	ctx := context.Background()
	pool, err := openPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	attempt := seedServerReaperAttempt(t, pool)
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", attempt); err != nil {
		t.Fatal(err)
	}
	stop := startReaperServer(t)
	defer stop()
	var state string
	if err := pool.QueryRow(ctx, "SELECT state FROM attempts WHERE id=$1", attempt).Scan(&state); err != nil || state != "LOST" {
		t.Fatal("startup announced HTTP before recovering expired authority", state, err)
	}
	stop()
	stopAgain := startReaperServer(t)
	defer stopAgain()
	var events int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_events WHERE attempt_id=$1 AND type='ATTEMPT_LOST'", attempt).Scan(&events); err != nil || events != 1 {
		t.Fatal("restart repeated an already committed loss", events, err)
	}
}

func TestServerReapsExpiredAttemptWhileListening(t *testing.T) {
	configureServerTestDatabase(t)
	ctx := context.Background()
	pool, err := openPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	attempt := seedServerReaperAttempt(t, pool)
	stop := startReaperServer(t)
	defer stop()
	if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", attempt); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(6 * time.Second)
	for {
		var state string
		if err := pool.QueryRow(ctx, "SELECT state FROM attempts WHERE id=$1", attempt).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == "LOST" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic reaper left expired authority active", state)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
