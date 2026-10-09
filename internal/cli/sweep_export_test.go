package cli

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/client"
)

func TestSweepExportCLIProducesCompleteJSONAndCSV(t *testing.T) {
	const id = "00000000-0000-0000-0000-000000000001"
	const attempt = "00000000-0000-0000-0000-000000000004"
	broken := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private" || r.URL.Query().Get("limit") != "100" {
			t.Error("incorrect export request")
		}
		page := client.SweepPage{Sweep: client.SweepSummary{ID: id, ProjectID: "00000000-0000-0000-0000-000000000002", Name: "grid", State: "ACTIVE", SpecHash: strings.Repeat("a", 64), MaxConcurrent: 1, CreatedAt: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC), Progress: client.SweepProgress{Total: 2, Queued: 1, Succeeded: 1}},
			Children: []client.SweepChild{{ID: "00000000-0000-0000-0000-000000000003", Index: 0, State: "SUCCEEDED", Parameters: map[string]string{"SEED": "=cmd(),\n\"quoted\""}, AcceptedAttemptID: stringPointer(attempt), Metrics: map[string]json.Number{"score": "9007199254740993", "loss": "0.125"}}}, HasMore: true, NextCursor: "next"}
		if r.URL.Query().Get("cursor") == "next" {
			if broken {
				w.WriteHeader(503)
				_, _ = io.WriteString(w, `{"error":{"code":"UNAVAILABLE","message":"try later"}}`)
				return
			}
			page.Children[0] = client.SweepChild{ID: "00000000-0000-0000-0000-000000000005", Index: 1, State: "QUEUED", Parameters: map[string]string{"SEED": "2"}, Metrics: map[string]json.Number{}}
			page.HasMore, page.NextCursor = false, ""
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer server.Close()
	env := func(key string) string {
		return map[string]string{"DISPATCH_URL": server.URL, "DISPATCH_TOKEN": "private", "DISPATCH_DEV_INSECURE": "1"}[key]
	}
	var out, errs bytes.Buffer
	for _, format := range []string{"json", "csv"} {
		out.Reset()
		errs.Reset()
		if code := Run(context.Background(), []string{"sweep", "export", id, "--format", format}, env, &out, &errs); code != 0 || errs.Len() != 0 {
			t.Fatal("export failed", format, code, errs.String())
		}
		if format == "json" {
			var result client.SweepResults
			if json.Unmarshal(out.Bytes(), &result) != nil || len(result.Children) != 2 || result.SweepID != id || result.Children[0].Metrics["score"] != json.Number("9007199254740993") {
				t.Fatal("incomplete or rounded JSON export", out.String())
			}
		} else {
			rows, err := csv.NewReader(&out).ReadAll()
			if err != nil || len(rows) != 3 || strings.Join(rows[0], ",") != "index,jobId,state,acceptedAttemptId,parameters,metric.loss,metric.score" || rows[1][5] != "0.125" || rows[1][6] != "9007199254740993" || rows[2][5] != "" || rows[2][6] != "" {
				t.Fatal("incorrect CSV columns or missing metrics", rows, err)
			}
			var parameters map[string]string
			if json.Unmarshal([]byte(rows[1][4]), &parameters) != nil || parameters["SEED"] != "=cmd(),\n\"quoted\"" || !strings.HasPrefix(rows[1][4], "{") {
				t.Fatal("CSV parameter escaping changed data", rows[1][4])
			}
		}
	}
	broken = true
	out.Reset()
	errs.Reset()
	if code := Run(context.Background(), []string{"sweep", "export", id}, env, &out, &errs); code != 2 || out.Len() != 0 {
		t.Fatal("late page failure wrote incomplete export", code, out.String())
	}
}

func stringPointer(s string) *string { return &s }

type failedExportWriter struct{}

func (failedExportWriter) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }

func TestSweepCSVBoundsColumnsAndPropagatesFlushFailure(t *testing.T) {
	child := client.SweepChild{Parameters: map[string]string{}, Metrics: map[string]json.Number{}}
	if err := writeSweepCSV(failedExportWriter{}, []client.SweepChild{child}); err == nil {
		t.Fatal("CSV flush error lost")
	}
	for i := range 257 {
		child.Metrics[fmt.Sprintf("m%d", i)] = "1"
	}
	var out bytes.Buffer
	if err := writeSweepCSV(&out, []client.SweepChild{child}); err == nil || out.Len() != 0 {
		t.Fatal("wide CSV wrote before rejecting its column bound")
	}
}
