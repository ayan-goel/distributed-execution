package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAttemptsCLIShowsOrderedEvidenceAndSafeOutput(t *testing.T) {
	const job = "00000000-0000-0000-0000-000000000001"
	body := `{"jobId":"` + job + `","attempts":[{"id":"00000000-0000-0000-0000-000000000002","number":1,"state":"LOST","reason":"worker\n\u001b[31m","exitCode":null,"workerId":"00000000-0000-0000-0000-000000000004","cleanupPending":true,"createdAt":"2026-10-01T00:00:00Z","finishedAt":"2026-10-01T00:01:00Z"},{"id":"00000000-0000-0000-0000-000000000003","number":2,"state":"SUCCEEDED","reason":null,"exitCode":0,"workerId":"00000000-0000-0000-0000-000000000005","cleanupPending":false,"createdAt":"2026-10-01T00:02:00Z","finishedAt":"2026-10-01T00:03:00Z"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/jobs/"+job+"/attempts" || r.Header.Get("Authorization") != "Bearer private" {
			t.Error("history request changed", r.Method, r.URL.Path)
		}
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()
	env := waitEnv(server.URL)
	args := []string{"attempts", "list", job}
	var out, errs bytes.Buffer
	if code := Run(context.Background(), args, env, &out, &errs); code != 0 || errs.Len() != 0 {
		t.Fatal(code, out.String(), errs.String())
	}
	for _, want := range []string{job, "number=1", "LOST", "cleanupPending=true", `reason="worker\n\x1b[31m"`, "exitCode=null", "number=2", "SUCCEEDED", "exitCode=0", "finishedAt=2026-10-01T00:03:00Z"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in %s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "\x1b") || strings.Count(out.String(), "\n") != 3 || strings.Contains(out.String(), "private") {
		t.Fatal("unsafe or unexpected human output", out.String())
	}
	out.Reset()
	if code := Run(context.Background(), append(args, "--json"), env, &out, &errs); code != 0 {
		t.Fatal(code, errs.String())
	}
	var got, expected any
	if json.Unmarshal(out.Bytes(), &got) != nil || json.Unmarshal([]byte(body), &expected) != nil {
		t.Fatal("invalid JSON")
	}
	gotJSON, _ := json.Marshal(got)
	expectedJSON, _ := json.Marshal(expected)
	if !bytes.Equal(gotJSON, expectedJSON) {
		t.Fatal("history JSON changed", out.String())
	}
	for _, extra := range [][]string{nil, {"--json"}} {
		if code := Run(context.Background(), append(args, extra...), env, failedExportWriter{}, &errs); code != 2 {
			t.Fatal("output error lost", code)
		}
	}
	body = `{"jobId":"` + job + `","attempts":[]}`
	out.Reset()
	if code := Run(context.Background(), args, env, &out, &errs); code != 0 || !strings.Contains(out.String(), job) || !strings.Contains(out.String(), "No attempts yet.") {
		t.Fatal("empty history lost identity", code, out.String())
	}
}

func TestAttemptsCLIRejectsInvalidArgumentsBeforeCredentials(t *testing.T) {
	const job = "00000000-0000-0000-0000-000000000001"
	for _, args := range [][]string{{"attempts"}, {"attempts", "list"}, {"attempts", "get", job}, {"attempts", "list", "../escape"}, {"attempts", "list", "00000000-0000-0000-0000-000000000000"}, {"attempts", "list", job, "extra"}, {"attempts", "list", job, "--typo"}} {
		var out, errs bytes.Buffer
		if code := Run(context.Background(), args, func(string) string { t.Error("invalid arguments read credentials"); return "" }, &out, &errs); code != 2 || out.Len() != 0 {
			t.Fatal("invalid arguments accepted", args, code)
		}
	}
}

func TestAttemptsCLIFailsWithoutPartialHistory(t *testing.T) {
	const job = "00000000-0000-0000-0000-000000000001"
	for _, tc := range []struct {
		name string
		code int
		body string
	}{
		{"foreign identity", 200, `{"jobId":"00000000-0000-0000-0000-000000000002","attempts":[]}`},
		{"missing history", 200, `{"jobId":"` + job + `"}`},
		{"malformed JSON", 200, `{`},
		{"server unavailable", 503, `{"code":"UNAVAILABLE","message":"try later"}`},
		{"unauthenticated", 401, `{"code":"UNAUTHENTICATED","message":"invalid token"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.code)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			for _, extra := range [][]string{nil, {"--json"}} {
				var out, errs bytes.Buffer
				if code := Run(context.Background(), append([]string{"attempts", "list", job}, extra...), waitEnv(server.URL), &out, &errs); code != 2 || out.Len() != 0 || errs.Len() == 0 {
					t.Fatal("error emitted a history result", code, out.String(), errs.String())
				}
			}
		})
	}
}
