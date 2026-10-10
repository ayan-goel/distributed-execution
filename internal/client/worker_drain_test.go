package client

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestWorkerDrainClientChecksIdentityAndAcknowledgement(t *testing.T) {
	const worker = "00000000-0000-0000-0000-000000000001"
	valid := `{"workerId":"` + worker + `","state":"DRAINING","drainRequested":true}`
	body := valid
	requests := 0
	c, _ := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		if r.Method != "POST" || r.URL.Path != "/v1/workers/"+worker+"/drain" || r.Header.Get("Authorization") != "Bearer private" || r.ContentLength > 0 {
			t.Error("drain request changed")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
	}))
	if result, err := c.DrainWorker(context.Background(), worker); err != nil || result.WorkerID != worker || !result.DrainRequested {
		t.Fatal(result, err)
	}
	for _, bad := range []string{
		strings.Replace(valid, worker, "00000000-0000-0000-0000-000000000002", 1),
		strings.Replace(valid, "DRAINING", "\u001b[31m", 1),
		strings.Replace(valid, "true", "false", 1),
		strings.Replace(valid, "true", "null", 1),
		strings.Replace(valid, `,"drainRequested":true`, "", 1),
		strings.Replace(valid, `"state":"DRAINING"`, `"state":"DRAINING","state":"READY"`, 1),
		strings.Replace(valid, `"drainRequested":true`, `"drainRequested":true,"extra":1`, 1),
	} {
		body = bad
		if _, err := c.DrainWorker(context.Background(), worker); err == nil {
			t.Fatal("invalid drain acknowledgement accepted", body)
		}
	}
	before := requests
	if _, err := c.DrainWorker(context.Background(), "../escape"); err == nil || requests != before {
		t.Fatal("unsafe worker ID sent")
	}
}
