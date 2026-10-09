package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/client"
)

func TestSweepGetCLIReadsPagesAndEscapesTerminalValues(t *testing.T) {
	const id = "00000000-0000-0000-0000-000000000001"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != "GET" || r.URL.Path != "/v1/sweeps/"+id || r.URL.Query().Get("limit") != "1" || r.Header.Get("Authorization") != "Bearer private" {
			t.Error("incorrect inspection request")
		}
		page := client.SweepPage{Sweep: client.SweepSummary{ID: id, ProjectID: "00000000-0000-0000-0000-000000000002", Name: "grid", State: "QUEUED", SpecHash: strings.Repeat("a", 64), MaxConcurrent: 1, CreatedAt: time.Now().UTC(), Progress: client.SweepProgress{Total: 2, Queued: 2}},
			Children: []client.SweepChild{{ID: "00000000-0000-0000-0000-000000000003", Index: 0, State: "QUEUED", Parameters: map[string]string{"SEED": "\x1b[31m\n"}, Metrics: map[string]json.Number{}}}, HasMore: true, NextCursor: "next"}
		if r.URL.Query().Get("cursor") == "next" {
			attempt := "00000000-0000-0000-0000-000000000005"
			page.Sweep.State, page.Sweep.Progress.Queued, page.Sweep.Progress.Succeeded = "ACTIVE", 1, 1
			page.Children[0] = client.SweepChild{ID: "00000000-0000-0000-0000-000000000004", Index: 1, State: "SUCCEEDED", Parameters: map[string]string{"SEED": "2"}, AcceptedAttemptID: &attempt, Metrics: map[string]json.Number{"score": "9007199254740993"}}
			page.HasMore, page.NextCursor = false, ""
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer server.Close()
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": "private", "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	var out, errs bytes.Buffer
	if code := Run(context.Background(), []string{"sweep", "get", id, "--limit", "1"}, env, &out, &errs); code != 0 || !strings.Contains(out.String(), "QUEUED") || !strings.Contains(out.String(), `nextCursor: "next"`) || strings.ContainsAny(out.String(), "\x1b") || !strings.Contains(out.String(), `\u001b[31m\n`) {
		t.Fatal("incorrect or unsafe text inspection", code, out.String(), errs.String())
	}
	out.Reset()
	errs.Reset()
	if code := Run(context.Background(), []string{"sweep", "get", id, "--limit", "1", "--cursor", "next", "--json"}, env, &out, &errs); code != 0 {
		t.Fatal(code, errs.String())
	}
	var page client.SweepPage
	if json.Unmarshal(out.Bytes(), &page) != nil || len(page.Children) != 1 || page.Children[0].Index != 1 || page.HasMore || page.Children[0].Metrics["score"] != json.Number("9007199254740993") || errs.Len() != 0 || requests != 2 {
		t.Fatal("incorrect JSON continuation", out.String(), requests)
	}
	for _, flags := range [][]string{{"--limit", "0"}, {"--limit", "101"}, {"--unknown"}, {"extra"}} {
		out.Reset()
		errs.Reset()
		args := append([]string{"sweep", "get", id}, flags...)
		if code := Run(context.Background(), args, env, &out, &errs); code != 2 || out.Len() != 0 || requests != 2 {
			t.Fatal("invalid flags reached server", args, code, requests)
		}
	}
}
