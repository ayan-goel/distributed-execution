package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestShutdownDeadlineClosesHTTPOnlyRequests(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
	defer server.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		response, err := server.Client().Get(server.URL)
		if err == nil {
			_ = response.Body.Close()
		}
	}()
	<-entered
	if err := shutdownServers(server.Config, nil, 10*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("shutdown did not report exhausted budget", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forced shutdown left HTTP request running")
	}
}
