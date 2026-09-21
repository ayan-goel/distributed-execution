//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/admission"
	"dispatch.local/dispatch/internal/spec"
	"dispatch.local/dispatch/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

type resolverFunc func(context.Context, string) (string, error)

func (f resolverFunc) Resolve(c context.Context, s string) (string, error) { return f(c, s) }

func apiPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("DISPATCH_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("integration requires DISPATCH_TEST_DATABASE_URL")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	root, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("api_%d", time.Now().UnixNano())
	if _, err = root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, err := root.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		root.Close()
		if err != nil {
			t.Error(err)
		}
	})
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES ('research',4000,8192,4),('other',4000,8192,4)"); err != nil {
		t.Fatal(err)
	}
	return pool
}

func jobBody(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../../schema/examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}
	j, err := spec.DecodeJob(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	j.Spec.Inputs = nil
	b, _, err = j.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func call(h http.Handler, method, path, token, key string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestHTTPSubmissionRecoveryAndAuthorization(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	token, _, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := store.IssueToken(ctx, pool, "research", store.RoleRead)
	if err != nil {
		t.Fatal(err)
	}
	foreign, _, err := store.IssueToken(ctx, pool, "other", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	var lookups atomic.Int64
	var unavailable atomic.Bool
	h := New(pool, resolverFunc(func(context.Context, string) (string, error) {
		lookups.Add(1)
		if unavailable.Load() {
			return "", admission.ErrImageUnavailable
		}
		return "registry.example.org/eval@sha256:" + strings.Repeat("a", 64), nil
	}), nil)
	body := jobBody(t)
	for _, test := range []struct {
		token  string
		status int
	}{{"", 401}, {reader, 403}, {foreign, 403}} {
		w := call(h, "POST", "/v1/jobs", test.token, "same", body)
		if w.Code != test.status {
			t.Fatalf("auth status %d want %d", w.Code, test.status)
		}
		if w.Header().Get("X-Request-ID") == "" {
			t.Fatal("missing request ID")
		}
	}
	var wg sync.WaitGroup
	results := make(chan *httptest.ResponseRecorder, 100)
	for range 100 {
		wg.Add(1)
		go func() { defer wg.Done(); results <- call(h, "POST", "/v1/jobs", token, "same", body) }()
	}
	wg.Wait()
	close(results)
	id := ""
	for w := range results {
		if w.Code != 200 && w.Code != 201 {
			t.Fatalf("submit status %d: %s", w.Code, w.Body.String())
		}
		var j store.JobRecord
		if err := json.Unmarshal(w.Body.Bytes(), &j); err != nil {
			t.Fatal(err)
		}
		if id == "" {
			id = j.ID
		}
		if j.ID != id {
			t.Fatal("duplicate HTTP submission")
		}
	}
	before := lookups.Load()
	unavailable.Store(true)
	if w := call(h, "POST", "/v1/jobs", token, "same", body); w.Code != 200 || lookups.Load() != before {
		t.Fatal("lost-reply retry depended on registry availability", w.Code)
	}
	if w := call(h, "POST", "/v1/jobs", token, "new-key", body); w.Code != 503 {
		t.Fatal("registry failure did not return 503", w.Code)
	}
	changed := bytes.Replace(body, []byte(`"SEED":"1"`), []byte(`"SEED":"2"`), 1)
	if w := call(h, "POST", "/v1/jobs", token, "same", changed); w.Code != 409 {
		t.Fatal("conflicting request accepted", w.Code)
	}
	if w := call(h, "GET", "/v1/jobs/"+id, reader, "", nil); w.Code != 200 {
		t.Fatal("reader cannot inspect job", w.Code)
	}
	if w := call(h, "GET", "/v1/jobs/"+id, foreign, "", nil); w.Code != 404 {
		t.Fatal("cross-project job disclosed", w.Code)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&count); err != nil || count != 1 {
		t.Fatal("failed admission left rows", count, err)
	}
}

func TestHTTPBoundariesAndSanitizedErrors(t *testing.T) {
	pool := apiPool(t)
	ctx := context.Background()
	token, _, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
	if err != nil {
		t.Fatal(err)
	}
	h := New(pool, resolverFunc(func(context.Context, string) (string, error) {
		return "", errors.New("private credentials must never leak")
	}), nil)
	for _, test := range []struct {
		body   []byte
		key    string
		status int
	}{{[]byte("{}"), "key", 422}, {jobBody(t), "", 400}, {bytes.Repeat([]byte(" "), spec.MaxDocumentBytes+1), "key", 413}, {jobBody(t), "key", 503}} {
		w := call(h, "POST", "/v1/jobs", token, test.key, test.body)
		if w.Code != test.status {
			t.Fatalf("got %d want %d: %s", w.Code, test.status, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "private credentials") {
			t.Fatal("internal error leaked")
		}
		var envelope struct {
			Error struct {
				Code      string
				RequestID string
				Retryable bool
			}
		}
		if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil || envelope.Error.Code == "" || envelope.Error.RequestID == "" {
			t.Fatal("unstructured error", err)
		}
	}
}
