//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/cli"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestServerCommandsSubmitAndRecoverAfterRestart(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("DISPATCH_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("database test URL required")
	}
	root, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("server_%d", time.Now().UnixNano())
	if _, err := root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := root.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		root.Close()
		if err != nil {
			t.Error(err)
		}
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	t.Setenv("DISPATCH_DATABASE_URL", u.String())
	if err := run(ctx, []string{"migrate"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, []string{"project", "create", "--name", "research"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run(ctx, []string{"token", "create", "--project", "research"}, &output); err != nil {
		t.Fatal(err)
	}
	var credential struct {
		Token string
		ID    string
	}
	if err := json.Unmarshal(output.Bytes(), &credential); err != nil {
		t.Fatal(err)
	}
	reg := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	defer reg.Close()
	host := strings.TrimPrefix(reg.URL, "http://")
	tag, err := name.NewTag(host + "/evaluation:latest")
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(tag, empty.Image); err != nil {
		t.Fatal(err)
	}
	start := func() (string, func()) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		reader, writer := io.Pipe()
		done := make(chan error, 1)
		go func() {
			err := run(ctx, []string{"serve", "--listen", "127.0.0.1:0", "--dev-insecure", "--allow-registry", host}, writer)
			_ = writer.CloseWithError(err)
			done <- err
		}()
		var event struct{ Address string }
		if err := json.NewDecoder(reader).Decode(&event); err != nil {
			cancel()
			t.Fatal("server startup failed", err)
		}
		stopped := false
		stop := func() {
			if stopped {
				return
			}
			stopped = true
			cancel()
			if err := <-done; err != nil {
				t.Error("shutdown failed", err)
			}
			_ = reader.Close()
		}
		t.Cleanup(stop)
		return "http://" + event.Address, stop
	}
	address, stop := start()
	b, err := os.ReadFile("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	j, err := spec.DecodeJob(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	j.Spec.Inputs = nil
	j.Spec.Image = tag.Name()
	b, _, err = j.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()
	submit := func(address string) (store.JobRecord, int) {
		req, err := http.NewRequest("POST", address+"/v1/jobs", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+credential.Token)
		req.Header.Set("Idempotency-Key", "restart-test")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var result store.JobRecord
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result, resp.StatusCode
	}
	first, status := submit(address)
	if status != 201 || first.ID == "" || first.State != "QUEUED" {
		t.Fatal("real submission failed", status)
	}
	file := filepath.Join(t.TempDir(), "job.json")
	if err := os.WriteFile(file, b, 0600); err != nil {
		t.Fatal(err)
	}
	cliRun := func(args ...string) store.JobRecord {
		t.Helper()
		var out, errs bytes.Buffer
		env := func(key string) string {
			return map[string]string{"DISPATCH_URL": address, "DISPATCH_TOKEN": credential.Token, "DISPATCH_DEV_INSECURE": "1"}[key]
		}
		if code := cli.Run(ctx, args, env, &out, &errs); code != 0 {
			t.Fatalf("CLI failed: %d %s", code, errs.String())
		}
		var job store.JobRecord
		if err := json.Unmarshal(out.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		return job
	}
	cliFirst := cliRun("submit", file, "--idempotency-key", "cli-recovery", "--json")
	inspected := cliRun("jobs", "get", cliFirst.ID, "--json")
	if cliFirst.ID == first.ID || inspected.ID != cliFirst.ID || inspected.State != "QUEUED" || inspected.SpecHash != cliFirst.SpecHash || !bytes.Contains(inspected.Spec, []byte("@sha256:")) {
		t.Fatal("CLI did not inspect the admitted immutable job")
	}
	stop()
	reg.Close()
	address, _ = start()
	again, status := submit(address)
	if status != 200 || again.ID != first.ID {
		t.Fatal("restart/outage recovery changed job", status)
	}
	cliAgain := cliRun("submit", file, "--idempotency-key", "cli-recovery", "--json")
	if cliAgain.ID != cliFirst.ID || cliAgain.SpecHash != cliFirst.SpecHash {
		t.Fatal("CLI retry changed the admitted job")
	}
	if err := run(ctx, []string{"token", "revoke", "--project", "research", "--id", credential.ID}, io.Discard); err != nil {
		t.Fatal(err)
	}
	_, status = submit(address)
	if status != 401 {
		t.Fatal("revocation not observed by running server", status)
	}
}
