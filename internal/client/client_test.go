package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestClientEndpointSecurity(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		dev, ok  bool
	}{
		{"https://dispatch.example.org", false, true}, {"http://127.0.0.1:8080", true, true},
		{"http://dispatch.example.org", true, false}, {"http://127.0.0.1:8080", false, false},
		{"https://user:password@dispatch.example.org", false, false}, {"https://dispatch.example.org?token=secret", false, false},
	} {
		_, err := New(tc.endpoint, "token", tc.dev, nil)
		if (err == nil) != tc.ok {
			t.Fatalf("endpoint validation mismatch for %q", tc.endpoint)
		}
	}
}

func TestClientRejectsRedirectsAndPreservesAPIErrors(t *testing.T) {
	var redirected bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected = true }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	c, err := New(origin.URL, "private", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Submit(context.Background(), []byte(`{}`), "key"); err == nil {
		t.Fatal("redirect accepted")
	}
	if redirected {
		t.Fatal("redirect leaked authenticated request")
	}
	c.http.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 409, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"CONFLICT","message":"changed request","retryable":false,"requestId":"req-1"}}`)), Header: http.Header{}}, nil
	})
	_, err = c.Submit(context.Background(), []byte(`{}`), "key")
	var remote *APIError
	if !errors.As(err, &remote) || remote.Code != "CONFLICT" || remote.Status != 409 || remote.RequestID != "req-1" {
		t.Fatal("lost structured error", err)
	}
}

func TestClientRejectsInvalidResponsesAndUnsafeArguments(t *testing.T) {
	for _, body := range []string{`{}`, `{"id":"bad","state":"QUEUED"}`, `{"id":"00000000-0000-0000-0000-000000000001","state":"QUEUED"} {}`} {
		c, _ := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
		}))
		if _, err := c.Submit(context.Background(), []byte(`{}`), "key"); err == nil {
			t.Fatal("invalid response accepted")
		}
	}
	c, _ := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
		t.Fatal("invalid arguments reached transport")
		return nil, nil
	}))
	if _, err := c.Submit(context.Background(), []byte(`{}`), "key\nInjected: value"); err == nil {
		t.Fatal("unsafe key accepted")
	}
	if _, err := c.GetJob(context.Background(), "../healthz"); err == nil {
		t.Fatal("unsafe job ID accepted")
	}
}

func TestClientPreservesSubmissionKeyAndBoundsResponses(t *testing.T) {
	const id = "00000000-0000-0000-0000-000000000001"
	c, err := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer private" || r.Header.Get("Idempotency-Key") != "stable-key" {
			t.Fatal("missing request authority/identity")
		}
		return &http.Response{StatusCode: 201, Body: io.NopCloser(strings.NewReader(`{"id":"` + id + `","state":"QUEUED"}`)), Header: http.Header{}, Request: r}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	j, err := c.Submit(context.Background(), []byte(`{}`), "stable-key")
	if err != nil || j.ID != id {
		t.Fatal("invalid submit response", err)
	}
	c.http.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(strings.Repeat(" ", MaxResponseBytes+1))), Header: http.Header{}, Request: r}, nil
	})
	if _, err := c.GetJob(context.Background(), id); err == nil {
		t.Fatal("oversized response accepted")
	}
}

func TestClientPreservesAcceptedManifestPrecision(t *testing.T) {
	const id = "00000000-0000-0000-0000-000000000001"
	const attempt = "00000000-0000-0000-0000-000000000002"
	const manifest = `{"version":1,"metrics":{"count":9007199254740993},"outputs":[]}`
	c, err := New("https://dispatch.example.org", "private", false, transportFunc(func(r *http.Request) (*http.Response, error) {
		body := `{"id":"` + id + `","state":"SUCCEEDED","acceptedAttemptId":"` + attempt + `","acceptedManifest":` + manifest + `}`
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	j, err := c.GetJob(context.Background(), id)
	if err != nil || j.AcceptedAttemptID == nil || *j.AcceptedAttemptID != attempt || string(j.AcceptedManifest) != manifest {
		t.Fatal("result identity/number text lost", err)
	}
}
