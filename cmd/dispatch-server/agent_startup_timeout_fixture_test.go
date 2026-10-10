//go:build integration

package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type stalledImageProxy struct {
	socket  string
	entered <-chan struct{}
	release func()
	creates atomic.Int32
}

func newStalledImageProxy(t *testing.T, upstream string) *stalledImageProxy {
	t.Helper()
	// Keep the Unix path below macOS's socket length limit. This private root
	// belongs only to the test; no Docker files or shared images are changed.
	root, err := os.MkdirTemp("/tmp", "dispatch-image-stall-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	f := &stalledImageProxy{socket: filepath.Join(root, "docker.sock"), entered: entered,
		release: func() { releaseOnce.Do(func() { close(release) }) }}
	listener, err := net.Listen("unix", f.socket)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", upstream)
	}}
	proxy := &httputil.ReverseProxy{Director: func(r *http.Request) {
		r.URL.Scheme, r.URL.Host = "http", "docker"
	}, Transport: transport}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/containers/create") {
			f.creates.Add(1)
		}
		if strings.Contains(r.URL.Path, "/images/") && strings.HasSuffix(r.URL.Path, "/json") {
			// Hold preparation before the real daemon can answer. Releasing this
			// read after expiry must never permit a container mutation.
			enteredOnce.Do(func() { close(entered) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		proxy.ServeHTTP(w, r)
	})}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() {
		f.release()
		_ = server.Close()
		<-done
		transport.CloseIdleConnections()
	})
	return f
}
