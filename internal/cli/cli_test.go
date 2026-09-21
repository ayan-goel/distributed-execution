package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOfflineValidationAndUsage(t *testing.T) {
	for _, args := range [][]string{{"--version"}, {"--help"}, {"validate", "../../schema/examples/job.yaml", "--json"}} {
		var out, errs bytes.Buffer
		code := Run(context.Background(), args, func(string) string { t.Fatal("offline command consulted credentials"); return "" }, &out, &errs)
		if code != 0 || out.Len() == 0 || errs.Len() != 0 {
			t.Fatalf("offline command failed: %v: %d %s", args, code, errs.String())
		}
	}
	for _, args := range [][]string{nil, {"submit"}, {"jobs", "get"}, {"validate", "missing.yaml"}, {"validate", "../../schema/examples/job.yaml", "extra"}, {"submit", "../../schema/examples/job.yaml", "--typo"}} {
		var out, errs bytes.Buffer
		if code := Run(context.Background(), args, func(string) string { return "" }, &out, &errs); code != 2 || errs.Len() == 0 || out.Len() != 0 {
			t.Fatalf("invalid command accepted: %v: %d", args, code)
		}
	}
}

func TestSubmitKeepsRecoveryKeyAndJSONOutputSeparate(t *testing.T) {
	const id = "00000000-0000-0000-0000-000000000001"
	var errs bytes.Buffer
	var gotKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private" {
			t.Error("credentials missing")
		}
		if r.Method == "POST" {
			gotKey = r.Header.Get("Idempotency-Key")
			if gotKey == "" || !strings.Contains(errs.String(), gotKey) {
				t.Error("key was not recorded before submitting")
			}
			w.WriteHeader(201)
		}
		_, _ = io.WriteString(w, `{"id":"`+id+`","state":"QUEUED"}`)
	}))
	defer server.Close()
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": "private", "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	for _, args := range [][]string{{"submit", "../../schema/examples/job.yaml", "--json"}, {"submit", "../../schema/examples/job.yaml", "--idempotency-key", "retry-key", "--json"}, {"jobs", "get", id, "--json"}} {
		var out bytes.Buffer
		errs.Reset()
		if code := Run(context.Background(), args, env, &out, &errs); code != 0 {
			t.Fatal("command failed", code, errs.String())
		}
		var result struct{ ID string }
		if json.Unmarshal(out.Bytes(), &result) != nil || result.ID != id {
			t.Fatal("stdout not a single job JSON value", out.String())
		}
		if len(args) > 4 && args[2] == "--idempotency-key" && gotKey != "retry-key" {
			t.Fatal("explicit key changed")
		}
	}
}

func TestValidationRejectsTerminalControlCharactersSafely(t *testing.T) {
	file := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(file, []byte("bad: \"\\u001b[31m\""), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errs bytes.Buffer
	if code := Run(context.Background(), []string{"validate", file}, func(string) string { return "" }, &out, &errs); code != 2 {
		t.Fatal("invalid document accepted")
	}
	if strings.ContainsRune(errs.String(), '\x1b') {
		t.Fatal("terminal escape passed through")
	}
}
