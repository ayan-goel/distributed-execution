//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
)

func provisionTestWorker(t *testing.T, p workerPKI) store.WorkerIdentity {
	t.Helper()
	var output bytes.Buffer
	if err := run(context.Background(), []string{"worker", "create", "--name", "worker-a", "--certificate", p.clientCert, "--project", "research", "--architecture", "arm64", "--cpu-millis", "4000", "--memory-mib", "8192", "--scratch-mib", "16384", "--slots", "4"}, &output); err != nil {
		t.Fatal(err)
	}
	var id store.WorkerIdentity
	if err := json.Unmarshal(output.Bytes(), &id); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestWorkerOperatorProvisionTakeoverAndRevoke(t *testing.T) {
	configureServerTestDatabase(t)
	p := testWorkerPKI(t)
	id := provisionTestWorker(t, p)
	ctx := context.Background()
	pool, err := openPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if authenticated, err := store.AuthenticateWorker(ctx, pool, sha256.Sum256(p.client.Certificate[0])); err != nil || authenticated != id {
		t.Fatal("command did not provision identity", err)
	}
	r := store.Registration{RequestID: uuid.NewString(), SessionID: uuid.NewString(), ProtocolVersion: 1, Resources: spec.Resources{CPUMillis: 4000, MemoryMiB: 8192, ScratchMiB: 16384}, Slots: 4, Labels: map[string]string{"os": "linux", "architecture": "arm64"}, Capabilities: []string{"docker.v1", "cpu.hard", "memory.hard", "pids.hard", "scratch.soft"}}
	if _, err := store.RegisterSession(ctx, pool, id, r); err != nil {
		t.Fatal(err)
	}
	next := r
	next.RequestID = uuid.NewString()
	next.SessionID = uuid.NewString()
	if err := run(ctx, []string{"worker", "takeover", "--id", id.WorkerID, "--from-session", r.SessionID, "--to-session", next.SessionID}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if result, err := store.RegisterSession(ctx, pool, id, next); err != nil || result.Generation != 2 {
		t.Fatal("operator approval not applied", err)
	}
	for range 2 {
		if err := run(ctx, []string{"worker", "revoke", "--id", id.WorkerID, "--credential", id.CredentialID}, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.AuthenticateWorker(ctx, pool, sha256.Sum256(p.client.Certificate[0])); !errors.Is(err, store.ErrUnauthorized) {
		t.Fatal("operator revocation not applied", err)
	}
}
