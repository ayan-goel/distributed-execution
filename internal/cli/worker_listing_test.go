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

func TestWorkerListingRejectsInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		{"workers", "list", "--limit=0"}, {"workers", "list", "--limit=101"},
		{"workers", "list", "--cursor=" + strings.Repeat("a", 257)},
		{"workers", "list", "extra"}, {"workers", "list", "--unknown"},
	} {
		var out, errs bytes.Buffer
		code := Run(context.Background(), args, func(string) string { t.Fatal("invalid options reached credentials"); return "" }, &out, &errs)
		if code != 2 || out.Len() != 0 || errs.Len() == 0 {
			t.Fatal(args, code, out.String(), errs.String())
		}
	}
}

func TestWorkerListingEmptyPageAndOutputErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"project":"research","projectId":"00000000-0000-0000-0000-000000000001","asOf":"2026-10-10T12:00:00Z","workers":[],"hasMore":false,"nextCursor":""}`)
	}))
	defer server.Close()
	for _, extra := range [][]string{nil, {"--json"}, {"--cursor=opaque"}} {
		args := append([]string{"workers", "list"}, extra...)
		var out, errs bytes.Buffer
		if code := Run(context.Background(), args, waitEnv(server.URL), &out, &errs); code != 0 || errs.Len() != 0 {
			t.Fatal(code, errs.String())
		}
		if (len(extra) == 0 || extra[0] != "--json") && !strings.Contains(out.String(), "No workers on this page.") {
			t.Fatal(out.String())
		}
		if code := Run(context.Background(), args, waitEnv(server.URL), failedExportWriter{}, &errs); code != 2 || !strings.Contains(errs.String(), "output unavailable") {
			t.Fatal(code, errs.String())
		}
	}
}
