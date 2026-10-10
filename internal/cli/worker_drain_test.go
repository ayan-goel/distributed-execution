package cli

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWorkerDrainCLIValidationOutputAndUncertainResponse(t *testing.T) {
	const worker = "00000000-0000-0000-0000-000000000001"
	for _, args := range [][]string{{"workers", "drain"}, {"workers", "get", worker}, {"workers", "drain", "../escape"}, {"workers", "drain", worker, "extra"}, {"workers", "drain", worker, "--typo"}} {
		var out, errs bytes.Buffer
		if code := Run(context.Background(), args, func(string) string { t.Error("invalid arguments read credentials"); return "" }, &out, &errs); code != 2 || out.Len() != 0 {
			t.Fatal(args, code)
		}
	}
	uncertain := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if uncertain {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		_, _ = io.WriteString(w, `{"workerId":"`+worker+`","state":"DRAINING","drainRequested":true}`)
	}))
	defer server.Close()
	for _, extra := range [][]string{nil, {"--json"}} {
		var errs bytes.Buffer
		if code := Run(context.Background(), append([]string{"workers", "drain", worker}, extra...), waitEnv(server.URL), failedExportWriter{}, &errs); code != 2 || !strings.Contains(errs.String(), "output unavailable") {
			t.Fatal("output failure lost", code, errs.String())
		}
	}
	uncertain = true
	var out, errs bytes.Buffer
	if code := Run(context.Background(), []string{"workers", "drain", worker}, waitEnv(server.URL), &out, &errs); code != 2 || out.Len() != 0 || !strings.Contains(errs.String(), "unconfirmed") || !strings.Contains(errs.String(), worker) {
		t.Fatal("lost reply claimed drain success", code, out.String(), errs.String())
	}
}
