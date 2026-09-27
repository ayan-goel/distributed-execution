package client

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestAttemptHistoryBindsJobAndRejectsForeignOrInvalidRows(t *testing.T) {
	const job = "00000000-0000-0000-0000-000000000001"
	const attempt = "00000000-0000-0000-0000-000000000002"
	const worker = "00000000-0000-0000-0000-000000000003"
	body := `{"jobId":"` + job + `","attempts":[{"id":"` + attempt + `","number":1,"state":"RUNNING","workerId":"` + worker + `","createdAt":"2026-09-27T00:00:00Z"}]}`
	c, _ := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1/jobs/"+job+"/attempts" || r.Header.Get("Authorization") != "Bearer private" {
			t.Fatal("attempt history request lost scope")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
	}))
	attempts, err := c.ListAttempts(context.Background(), job)
	if err != nil || len(attempts) != 1 || attempts[0].ID != attempt {
		t.Fatal(attempts, err)
	}
	body = strings.Replace(body, `"jobId":"`+job+`"`, `"jobId":"`+attempt+`"`, 1)
	if _, err := c.ListAttempts(context.Background(), job); err == nil {
		t.Fatal("foreign job response accepted")
	}
	if _, err := c.ListAttempts(context.Background(), "../escape"); err == nil {
		t.Fatal("unsafe request path accepted")
	}
}
