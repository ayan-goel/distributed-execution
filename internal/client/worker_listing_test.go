package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func workerPageBody() string {
	return `{"project":"research","projectId":"` + listedProject + `","asOf":"2026-10-10T12:00:00Z","workers":[{"id":"` + listedJob + `","name":"linux-a","state":"REGISTERING","labels":{"os":"linux","architecture":"amd64"},"drainRequested":false,"runtimeHealthy":false,"reconciliationComplete":false,"diskPressure":false,"lastHeartbeatAt":null,"capacity":{"cpuMillis":4000,"memoryMiB":8192,"scratchMiB":16384,"slots":4},"reserved":{"cpuMillis":0,"memoryMiB":0,"scratchMiB":0,"slots":0},"available":{"cpuMillis":4000,"memoryMiB":8192,"scratchMiB":16384,"slots":4}}],"hasMore":false,"nextCursor":""}`
}

func TestClientWorkerListingValidatesEvidence(t *testing.T) {
	opts := WorkerListOptions{Limit: 50}
	page, err := listingClient(t, workerPageBody()).ListWorkers(context.Background(), opts)
	if err != nil || len(page.Workers) != 1 || page.Workers[0].LastHeartbeatAt != nil {
		t.Fatal(page, err)
	}
	for _, replace := range [][2]string{
		{`"asOf":"2026-10-10T12:00:00Z"`, `"asOf":null`},
		{`"workers":[`, `"workers":null,"other":[`},
		{`"lastHeartbeatAt":null`, `"lastHeartbeatAt":"bad"`},
		{`"lastHeartbeatAt":null`, `"lastHeartbeatAt":"0001-01-01T00:00:00Z"`},
		{`"drainRequested":false`, `"drainRequested":null`},
		{`"state":"REGISTERING"`, `"state":"INVALID"`},
		{`"labels":{"os":"linux","architecture":"amd64"}`, `"labels":{"os":"linux","os":"linux","architecture":"amd64"}`},
		{`"labels":{"os":"linux","architecture":"amd64"}`, `"labels":{"os":"linux","architecture":"amd64","rack":"\u001b[31m"}`},
		{`"cpuMillis":4000`, `"cpuMillis":0`},
		{`"cpuMillis":0`, `"cpuMillis":-1`},
		{`"cpuMillis":0`, `"cpuMillis":1`},
		{`"cpuMillis":4000`, `"cpuMillis":4000,"cpuMillis":4000`},
		{`"name":"linux-a"`, `"name":"bad name"`},
		{`"hasMore":false`, `"hasMore":true`},
		{`"nextCursor":""`, `"nextCursor":"unexpected"`},
	} {
		body := strings.Replace(workerPageBody(), replace[0], replace[1], 1)
		if _, err := listingClient(t, body).ListWorkers(context.Background(), opts); err == nil {
			t.Fatal("malformed worker evidence accepted", replace)
		}
	}
	var raw struct{ Workers []json.RawMessage }
	if err := json.Unmarshal([]byte(workerPageBody()), &raw); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	json.Unmarshal(raw.Workers[0], &fields)
	for field := range fields {
		copy := map[string]json.RawMessage{}
		for key, value := range fields {
			if key != field {
				copy[key] = value
			}
		}
		row, _ := json.Marshal(copy)
		body := strings.Replace(workerPageBody(), string(raw.Workers[0]), string(row), 1)
		if _, err := listingClient(t, body).ListWorkers(context.Background(), opts); err == nil {
			t.Fatal("missing summary field accepted", field)
		}
	}
	// Reduced claims do not erase reservations: only available headroom clamps.
	body := strings.Replace(workerPageBody(), `"reserved":{"cpuMillis":0`, `"reserved":{"cpuMillis":5000`, 1)
	body = strings.Replace(body, `"available":{"cpuMillis":4000`, `"available":{"cpuMillis":0`, 1)
	body = strings.Replace(body, `"lastHeartbeatAt":null`, `"lastHeartbeatAt":"2026-10-10T13:00:00+01:00"`, 1)
	page, err = listingClient(t, body).ListWorkers(context.Background(), opts)
	if err != nil || page.Workers[0].Reserved.CPUMillis != 5000 || page.Workers[0].LastHeartbeatAt.Location().String() != "UTC" {
		t.Fatal(page, err)
	}
}

func TestClientWorkerListingBoundsAndQuery(t *testing.T) {
	c, _ := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != "GET" || r.URL.Path != "/v1/workers" || r.URL.Query().Get("limit") != "1" || r.URL.Query().Get("cursor") != "opaque" {
			t.Error(r.URL)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(workerPageBody()))}, nil
	}))
	if _, err := c.ListWorkers(context.Background(), WorkerListOptions{Limit: 1, Cursor: "opaque"}); err != nil {
		t.Fatal(err)
	}
	c, _ = New("https://dispatch.example.org", "private", false, transportFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid option reached transport")
		return nil, nil
	}))
	for _, opts := range []WorkerListOptions{{Limit: 0}, {Limit: 101}, {Limit: 1, Cursor: strings.Repeat("a", 257)}, {Limit: 1, Cursor: "\n"}} {
		if _, err := c.ListWorkers(context.Background(), opts); err == nil {
			t.Fatal(opts)
		}
	}
	if _, err := listingClient(t, strings.Repeat(" ", (1<<20)+1)).ListWorkers(context.Background(), WorkerListOptions{Limit: 1}); err == nil {
		t.Fatal("oversized response accepted")
	}
}

func TestClientWorkerListingRejectsMissingEnvelopeAndUnorderedRows(t *testing.T) {
	if requiredDiagnosticObject([]byte(`{"":null}`), map[string]any{"": new(string)}) == nil {
		t.Fatal("default strict decoder accepted null")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(workerPageBody()), &envelope); err != nil {
		t.Fatal(err)
	}
	for field := range envelope {
		copy := map[string]json.RawMessage{}
		for key, value := range envelope {
			if key != field {
				copy[key] = value
			}
		}
		body, _ := json.Marshal(copy)
		if _, err := decodeWorkerPage(body, WorkerListOptions{Limit: 50}); err == nil {
			t.Fatal("missing envelope field", field)
		}
	}
	var rows []json.RawMessage
	json.Unmarshal(envelope["workers"], &rows)
	for _, tc := range []struct {
		id    string
		valid bool
	}{
		{listedJob, false}, {"00000000-0000-0000-0000-000000000001", false},
		{"00000000-0000-0000-0000-000000000003", true},
	} {
		second := strings.Replace(string(rows[0]), listedJob, tc.id, 1)
		body := strings.Replace(workerPageBody(), `],"hasMore"`, `,`+second+`],"hasMore"`, 1)
		_, err := decodeWorkerPage([]byte(body), WorkerListOptions{Limit: 50})
		if (err == nil) != tc.valid {
			t.Fatal("row order validation", tc.id, err)
		}
	}
	body := strings.Replace(workerPageBody(), `"hasMore":false,"nextCursor":""`, `"hasMore":true,"nextCursor":"same"`, 1)
	if _, err := decodeWorkerPage([]byte(body), WorkerListOptions{Limit: 50, Cursor: "same"}); err == nil {
		t.Fatal("nonadvancing cursor accepted")
	}
}
