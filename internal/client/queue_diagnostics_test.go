package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

const diagnosticObservation = `{"sequence":3,"workerId":"00000000-0000-0000-0000-000000000002","sessionId":"00000000-0000-0000-0000-000000000003","requestId":"00000000-0000-0000-0000-000000000004","attemptCounter":0,"reason":"NO_RESOURCE_FIT","observedAt":"2026-10-10T12:00:00Z"}`
const diagnosticSnapshot = `{"asOf":"2026-10-10T11:59:00Z","attemptCounter":1,"observations":[` + diagnosticObservation + `]}`

func diagnosticClient(t *testing.T, diagnostics string) (*Client, string) {
	t.Helper()
	const id = "00000000-0000-0000-0000-000000000001"
	body := `{"id":"` + id + `","state":"ACTIVE"`
	if diagnostics != "" {
		body += `,"queueDiagnostics":` + diagnostics
	}
	body += `}`
	c, err := New("https://dispatch.example.org", "private", false, transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	return c, id
}

func TestClientPreservesHistoricalQueueDiagnosticsAndOldServers(t *testing.T) {
	reasons := []string{"PLACEMENT_MISMATCH", "NO_RESOURCE_FIT", "PROJECT_QUOTA", "SWEEP_CONCURRENCY", "PROJECT_DISABLED", "RETRY_BACKOFF"}
	observations := make([]string, 16)
	for i := range observations {
		o := strings.Replace(diagnosticObservation, `"sequence":3`, fmt.Sprintf(`"sequence":%d`, 100-i), 1)
		o = strings.Replace(o, "00000000-0000-0000-0000-000000000004", fmt.Sprintf("00000000-0000-0000-0000-%012d", i+10), 1)
		observations[i] = strings.Replace(o, "NO_RESOURCE_FIT", reasons[i%len(reasons)], 1)
	}
	maximum := `{"asOf":"2026-10-10T11:59:00Z","attemptCounter":1,"observations":[` + strings.Join(observations, ",") + `]}`
	for _, diagnostics := range []string{"", diagnosticSnapshot, maximum, `{"asOf":"2026-10-10T12:00:00Z","attemptCounter":0,"observations":[]}`} {
		c, id := diagnosticClient(t, diagnostics)
		job, err := c.GetJob(context.Background(), id)
		if err != nil {
			t.Fatal("valid history or old-server response rejected", err)
		}
		body, err := json.Marshal(job)
		if err != nil {
			t.Fatal(err)
		}
		var response map[string]json.RawMessage
		if err := json.Unmarshal(body, &response); err != nil {
			t.Fatal(err)
		}
		if diagnostics == "" {
			if _, exists := response["queueDiagnostics"]; exists {
				t.Fatal("old server gained invented diagnostics")
			}
		} else {
			var want, got any
			_ = json.Unmarshal([]byte(diagnostics), &want)
			_ = json.Unmarshal(response["queueDiagnostics"], &got)
			wantBody, _ := json.Marshal(want)
			gotBody, _ := json.Marshal(got)
			if string(wantBody) != string(gotBody) {
				t.Fatal("historical context lost, including clock rollback", string(body))
			}
		}
	}
}

func TestClientRejectsMalformedQueueDiagnostics(t *testing.T) {
	withObservation := func(observation string) string {
		return `{"asOf":"2026-10-10T12:00:00Z","attemptCounter":1,"observations":[` + observation + `]}`
	}
	bad := map[string]string{
		"null snapshot":               "null",
		"null read time":              strings.Replace(diagnosticSnapshot, `"2026-10-10T11:59:00Z"`, "null", 1),
		"wrong field case":            strings.Replace(diagnosticSnapshot, `"asOf"`, `"AsOf"`, 1),
		"missing counter":             strings.Replace(diagnosticSnapshot, `"attemptCounter":1,`, "", 1),
		"null counter":                strings.Replace(diagnosticSnapshot, `"attemptCounter":1`, `"attemptCounter":null`, 1),
		"negative counter":            strings.Replace(diagnosticSnapshot, `"attemptCounter":1`, `"attemptCounter":-1`, 1),
		"overflow counter":            strings.Replace(diagnosticSnapshot, `"attemptCounter":1`, `"attemptCounter":9223372036854775808`, 1),
		"unknown field":               strings.Replace(diagnosticSnapshot, `"asOf":`, `"blocked":true,"asOf":`, 1),
		"duplicate field":             strings.Replace(diagnosticSnapshot, `"attemptCounter":1`, `"attemptCounter":1,"attemptCounter":1`, 1),
		"null observations":           `{"asOf":"2026-10-10T12:00:00Z","attemptCounter":1,"observations":null}`,
		"too many observations":       withObservation(strings.TrimSuffix(strings.Repeat(diagnosticObservation+",", 17), ",")),
		"null observation":            withObservation("null"),
		"unknown reason":              withObservation(strings.Replace(diagnosticObservation, "NO_RESOURCE_FIT", "ANYTHING", 1)),
		"missing observation counter": withObservation(strings.Replace(diagnosticObservation, `"attemptCounter":0,`, "", 1)),
		"null observation counter":    withObservation(strings.Replace(diagnosticObservation, `"attemptCounter":0`, `"attemptCounter":null`, 1)),
		"future attempt":              withObservation(strings.Replace(diagnosticObservation, `"attemptCounter":0`, `"attemptCounter":2`, 1)),
		"zero sequence":               withObservation(strings.Replace(diagnosticObservation, `"sequence":3`, `"sequence":0`, 1)),
		"overflow sequence":           withObservation(strings.Replace(diagnosticObservation, `"sequence":3`, `"sequence":9223372036854775808`, 1)),
		"fractional sequence":         withObservation(strings.Replace(diagnosticObservation, `"sequence":3`, `"sequence":0.5`, 1)),
		"ascending sequences":         withObservation(diagnosticObservation + "," + strings.Replace(diagnosticObservation, `"sequence":3`, `"sequence":4`, 1)),
		"duplicate identity":          withObservation(diagnosticObservation + "," + strings.Replace(diagnosticObservation, `"sequence":3`, `"sequence":2`, 1)),
		"unknown observation field":   withObservation(strings.Replace(diagnosticObservation, `"reason":`, `"extra":0,"reason":`, 1)),
		"duplicate observation field": withObservation(strings.Replace(diagnosticObservation, `"sequence":3`, `"sequence":3,"sequence":3`, 1)),
		"invalid time":                withObservation(strings.Replace(diagnosticObservation, "2026-10-10T12:00:00Z", "yesterday", 1)),
		"zero time":                   withObservation(strings.Replace(diagnosticObservation, "2026-10-10T12:00:00Z", "0001-01-01T00:00:00Z", 1)),
		"nil worker":                  withObservation(strings.Replace(diagnosticObservation, "00000000-0000-0000-0000-000000000002", "00000000-0000-0000-0000-000000000000", 1)),
		"nil session":                 withObservation(strings.Replace(diagnosticObservation, "00000000-0000-0000-0000-000000000003", "00000000-0000-0000-0000-000000000000", 1)),
		"nil request":                 withObservation(strings.Replace(diagnosticObservation, "00000000-0000-0000-0000-000000000004", "00000000-0000-0000-0000-000000000000", 1)),
		"noncanonical worker":         withObservation(strings.Replace(diagnosticObservation, "00000000-0000-0000-0000-000000000002", "00000000000000000000000000000002", 1)),
	}
	for name, diagnostics := range bad {
		t.Run(name, func(t *testing.T) {
			c, id := diagnosticClient(t, diagnostics)
			if _, err := c.GetJob(context.Background(), id); err == nil {
				t.Fatal("malformed diagnostic response accepted")
			}
		})
	}
}
