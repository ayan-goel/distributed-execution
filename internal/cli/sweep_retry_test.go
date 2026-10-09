package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/client"
	"github.com/google/uuid"
)

func TestSweepRetryCLIRecordsKeyAndRecoversAnUncertainResponse(t *testing.T) {
	source := uuid.NewString()
	result := client.SweepRetry{ID: uuid.NewString(), ProjectID: uuid.NewString(), ParentSweepID: source,
		SpecHash: strings.Repeat("a", 64), MaxConcurrent: 2, CreatedAt: time.Now().UTC(),
		Children: []client.SweepRetryChild{{ID: uuid.NewString(), Index: 0, ParentJobID: uuid.NewString()}}}
	var out, errout bytes.Buffer
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		body, _ := io.ReadAll(r.Body)
		if r.Method != "POST" || r.URL.Path != "/v1/sweeps/"+source+"/retry" || len(body) != 0 || r.Header.Get("Authorization") != "Bearer private" || r.Header.Get("Idempotency-Key") != "recover-retry" || !strings.Contains(errout.String(), "recover-retry") {
			t.Error("retry request missing identity or prior recovery key")
		}
		if requests == 1 {
			// A malformed reply models an uncertain result after admission: the
			// caller must retain its key rather than create another retry implicitly.
			w.WriteHeader(201)
			_, _ = io.WriteString(w, `{"id":`)
			return
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": "private", "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	args := []string{"sweep", "retry", source, "--idempotency-key", "recover-retry", "--json"}
	if code := Run(context.Background(), args, env, &out, &errout); code != 2 || out.Len() != 0 || !strings.Contains(errout.String(), "recover-retry") || requests != 1 {
		t.Fatal("uncertain retry lost recovery key or retried implicitly", code, out.String(), errout.String(), requests)
	}
	out.Reset()
	errout.Reset()
	if code := Run(context.Background(), args, env, &out, &errout); code != 0 || requests != 2 {
		t.Fatal("retry recovery failed", code, errout.String(), requests)
	}
	var recovered client.SweepRetry
	if err := json.Unmarshal(out.Bytes(), &recovered); err != nil || !reflect.DeepEqual(result, recovered) {
		t.Fatal("CLI changed recovered mapping", out.String(), err)
	}
	out.Reset()
	errout.Reset()
	if code := Run(context.Background(), args[:5], env, &out, &errout); code != 0 || !strings.Contains(out.String(), result.ID+" 1 children") {
		t.Fatal("text retry output failed", code, out.String(), errout.String())
	}
	for _, flags := range [][]string{{"--all"}, {"--mode", "all"}, {"unexpected"}, {"--json", "extra"}} {
		out.Reset()
		errout.Reset()
		if code := Run(context.Background(), append([]string{"sweep", "retry", source}, flags...), env, &out, &errout); code != 2 || out.Len() != 0 || requests != 3 {
			t.Fatal("invalid retry options reached server", code, flags, requests)
		}
	}
	out.Reset()
	if code := Run(context.Background(), args, env, &out, failingRetryKeyWriter{}); code != 2 || requests != 3 || out.Len() != 0 {
		t.Fatal("retry proceeded without recording recovery key", code, requests)
	}
}

type failingRetryKeyWriter struct{}

func (failingRetryKeyWriter) Write([]byte) (int, error) {
	return 0, errors.New("unwritable recovery log")
}

func TestSweepRetryCLIGeneratesAKeyBeforeAdmission(t *testing.T) {
	source := uuid.NewString()
	var out, errout bytes.Buffer
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		key := r.Header.Get("Idempotency-Key")
		if parsed, err := uuid.Parse(key); err != nil || parsed == uuid.Nil || !strings.Contains(errout.String(), key) {
			t.Error("generated recovery key missing before admission", key)
		}
		w.WriteHeader(409)
		_, _ = io.WriteString(w, `{"error":{"code":"CONFLICT","message":"source unfinished","requestId":"request"}}`)
	}))
	defer server.Close()
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": "private", "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	if code := Run(context.Background(), []string{"sweep", "retry", source}, env, &out, &errout); code != 2 || requests != 1 || out.Len() != 0 || !strings.Contains(errout.String(), "Idempotency-Key:") {
		t.Fatal("generated-key rejection contract failed", code, out.String(), errout.String(), requests)
	}
}
