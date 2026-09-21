package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/admission"
	"dispatch.local/dispatch/internal/api"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/jackc/pgx/v5/pgxpool"
)

type stringsFlag []string

func (s *stringsFlag) String() string         { return strings.Join(*s, ",") }
func (s *stringsFlag) Set(value string) error { *s = append(*s, value); return nil }

type serveConfig struct {
	listen, cert, key     string
	dev                   bool
	registries, authHosts stringsFlag
}

func flags(name string) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	return f
}

func parseServeConfig(args []string) (serveConfig, error) {
	var c serveConfig
	f := flags("serve")
	f.StringVar(&c.listen, "listen", "127.0.0.1:8080", "HTTP listen address")
	f.StringVar(&c.cert, "tls-cert", "", "TLS certificate")
	f.StringVar(&c.key, "tls-key", "", "TLS private key")
	f.BoolVar(&c.dev, "dev-insecure", false, "allow plaintext loopback development")
	f.Var(&c.registries, "allow-registry", "approved registry host (repeatable)")
	f.Var(&c.authHosts, "allow-registry-auth-host", "approved registry auth host (repeatable)")
	if err := f.Parse(args); err != nil {
		return c, err
	}
	if f.NArg() != 0 {
		return c, errors.New("unexpected serve arguments")
	}
	host, _, err := net.SplitHostPort(c.listen)
	if err != nil {
		return c, errors.New("invalid listen address")
	}
	// Development plaintext must never silently become a public listener. Require
	// a literal loopback IP so DNS cannot change the security boundary.
	if c.dev {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return c, errors.New("--dev-insecure requires a literal loopback listen address")
		}
	}
	if !c.dev && (c.cert == "" || c.key == "") {
		return c, errors.New("TLS certificate and key are required")
	}
	if (c.cert == "") != (c.key == "") {
		return c, errors.New("TLS certificate and key must be supplied together")
	}
	if len(c.registries) == 0 {
		return c, errors.New("at least one --allow-registry is required")
	}
	for _, host := range append(append([]string{}, c.registries...), c.authHosts...) {
		if _, err := name.NewRegistry(host, name.StrictValidation); err != nil || strings.Contains(host, "/") {
			return c, errors.New("registry configuration requires host names, not URLs")
		}
	}
	return c, nil
}

func serve(ctx context.Context, pool *pgxpool.Pool, c serveConfig, out io.Writer) error {
	images := admission.RegistryResolver{Allowed: c.registries, AuthHosts: c.authHosts, AllowLoopbackHTTP: c.dev}
	server := &http.Server{Handler: api.New(pool, images), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13}}
	listener, err := net.Listen("tcp", c.listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	logger := slog.New(slog.NewJSONHandler(out, nil))
	logger.Info("http_listening", "address", listener.Addr().String(), "tls", c.cert != "")
	finished := make(chan error, 1)
	go func() {
		if c.cert != "" {
			finished <- server.ServeTLS(listener, c.cert, c.key)
		} else {
			finished <- server.Serve(listener)
		}
	}()
	select {
	case err := <-finished:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		// Drain accepted HTTP requests before closing the pool. Force-close only
		// after the bounded shutdown budget, preserving durable submission replay.
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}
