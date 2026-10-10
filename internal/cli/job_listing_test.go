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

func TestJobListingCLIValidatesFlagsAndPropagatesOutputFailure(t *testing.T) {
	const body = `{"project":"research","projectId":"00000000-0000-0000-0000-000000000001","jobs":[{"id":"00000000-0000-0000-0000-000000000002","projectId":"00000000-0000-0000-0000-000000000001","name":"training","state":"QUEUED","labels":{"eq":"a=b"},"priority":0,"createdAt":"2026-10-01T12:00:00Z"}],"hasMore":true,"nextCursor":"opaque-next"}`
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Get("project") != "research" || len(r.URL.Query()["label"]) != 1 || r.URL.Query()["label"][0] != "eq=a=b" {
			t.Error("CLI filters changed", r.URL.String())
		}
		io.WriteString(w, body)
	}))
	defer server.Close()
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": "private", "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	args := []string{"jobs", "list", "--project", "research", "--label", "eq=a=b"}
	var out, errs bytes.Buffer
	if code := Run(context.Background(), args, env, &out, &errs); code != 0 || !strings.Contains(out.String(), "opaque-next") || !strings.Contains(out.String(), "training") {
		t.Fatal(code, out.String(), errs.String())
	}
	for _, extra := range [][]string{nil, {"--json"}} {
		errs.Reset()
		if code := Run(context.Background(), append(append([]string{}, args...), extra...), env, failedExportWriter{}, &errs); code != 2 || !strings.Contains(errs.String(), "output unavailable") {
			t.Fatal("lost output failure", code, errs.String())
		}
	}
	requests = 0
	for _, bad := range [][]string{{"--label", "x"}, {"--label", "x=1", "--label", "x=2"}, {"--label", "bad key=x"}, {"--limit", "0"}, {"--limit", "101"}, {"--state", "running"}, {"--typo"}, {"unexpected"}} {
		out.Reset()
		errs.Reset()
		if code := Run(context.Background(), append([]string{"jobs", "list"}, bad...), env, &out, &errs); code != 2 || out.Len() != 0 {
			t.Fatal("invalid listing flags accepted", bad, code)
		}
	}
	if requests != 0 {
		t.Fatal("invalid flags reached server", requests)
	}
}
