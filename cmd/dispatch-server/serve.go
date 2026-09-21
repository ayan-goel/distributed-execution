package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"dispatch.local/dispatch/internal/admission"
	"dispatch.local/dispatch/internal/api"
	"dispatch.local/dispatch/internal/store"
	"dispatch.local/dispatch/internal/workerapi"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

type stringsFlag []string

func (s *stringsFlag) String() string         { return strings.Join(*s, ",") }
func (s *stringsFlag) Set(value string) error { *s = append(*s, value); return nil }

type serveConfig struct {
	listen, cert, key                             string
	dev                                           bool
	registries, authHosts                         stringsFlag
	workerListen, workerCert, workerKey, workerCA string
	objectEndpoint, objectRegion, objectBucket    string
	objectLoopback                                bool
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
	f.StringVar(&c.workerListen, "worker-listen", "", "optional worker gRPC listen address; mTLS always required")
	f.StringVar(&c.workerCert, "worker-tls-cert", "", "worker listener server certificate")
	f.StringVar(&c.workerKey, "worker-tls-key", "", "worker listener server private key")
	f.StringVar(&c.workerCA, "worker-client-ca", "", "trusted worker client CA bundle")
	f.StringVar(&c.objectEndpoint, "object-endpoint", "", "explicit S3-compatible origin")
	f.StringVar(&c.objectRegion, "object-region", "", "S3 signing region")
	f.StringVar(&c.objectBucket, "object-bucket", "", "versioned object bucket")
	f.BoolVar(&c.objectLoopback, "object-dev-loopback", false, "allow plaintext loopback object storage")
	if err := f.Parse(args); err != nil {
		return c, err
	}
	if f.NArg() != 0 {
		return c, errors.New("unexpected serve arguments")
	}
	if c.objectEndpoint != "" || c.objectRegion != "" || c.objectBucket != "" || c.objectLoopback {
		if c.objectEndpoint == "" || c.objectRegion == "" || c.objectBucket == "" {
			return c, errors.New("object storage requires --object-endpoint, --object-region, and --object-bucket")
		}
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
	if c.workerListen != "" {
		if _, _, err := net.SplitHostPort(c.workerListen); err != nil {
			return c, errors.New("invalid worker listen address")
		}
		if c.workerCert == "" || c.workerKey == "" || c.workerCA == "" {
			return c, errors.New("worker listener requires --worker-tls-cert, --worker-tls-key, and --worker-client-ca")
		}
	} else if c.workerCert != "" || c.workerKey != "" || c.workerCA != "" {
		return c, errors.New("worker TLS options require --worker-listen")
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
	objects, err := configuredObjectStore(ctx, c)
	if err != nil {
		return err
	}
	images := admission.RegistryResolver{Allowed: c.registries, AuthHosts: c.authHosts, AllowLoopbackHTTP: c.dev}
	var downloads api.DownloadSigner
	if objects != nil {
		downloads = objects
	}
	server := &http.Server{Handler: api.New(pool, images, downloads), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13}}
	// Validate both TLS configurations before opening either listener. Startup
	// must not advertise a working HTTP service when worker credentials are broken.
	if c.cert != "" {
		certificate, err := tls.LoadX509KeyPair(c.cert, c.key)
		if err != nil {
			return errors.New("cannot load HTTP TLS certificate/key")
		}
		server.TLSConfig.Certificates = []tls.Certificate{certificate}
	}
	var worker *grpc.Server
	if c.workerListen != "" {
		certificate, err := tls.LoadX509KeyPair(c.workerCert, c.workerKey)
		if err != nil {
			return errors.New("cannot load worker listener TLS certificate/key")
		}
		file, err := os.Open(c.workerCA)
		if err != nil {
			return errors.New("cannot read worker client CA bundle")
		}
		body, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
		_ = file.Close()
		if err != nil || len(body) > 1<<20 {
			return errors.New("worker client CA bundle must be readable and at most 1 MiB")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(body) {
			return errors.New("worker client CA bundle contains no valid certificates")
		}
		worker, err = workerapi.NewServer(pool, certificate, roots, workerapi.NewService(pool, store.AcquisitionPolicy{}, objects))
		if err != nil {
			return err
		}
	}
	listener, err := net.Listen("tcp", c.listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	var workerListener net.Listener
	if worker != nil {
		workerListener, err = net.Listen("tcp", c.workerListen)
		if err != nil {
			return err
		}
		defer workerListener.Close()
	}
	logger := slog.New(slog.NewJSONHandler(out, nil))
	finished := make(chan error, 2)
	go func() {
		if c.cert != "" {
			finished <- server.ServeTLS(listener, "", "")
		} else {
			finished <- server.Serve(listener)
		}
	}()
	if worker != nil {
		go func() { finished <- worker.Serve(workerListener) }()
	}
	logger.Info("http_listening", "address", listener.Addr().String(), "tls", c.cert != "")
	if worker != nil {
		logger.Info("worker_listening", "address", workerListener.Addr().String(), "mtls", true)
	}
	var servingError error
	select {
	case err := <-finished:
		if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			servingError = err
		}
	case <-ctx.Done():
	}
	return errors.Join(servingError, shutdownServers(server, worker, 10*time.Second))
}

func shutdownServers(server *http.Server, worker *grpc.Server, budget time.Duration) error {
	// Stop both admission paths before the caller closes their shared pool.
	// Force-close after one common budget; durable request IDs resolve uncertainty.
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	done := make(chan struct{})
	if worker != nil {
		go func() { worker.GracefulStop(); close(done) }()
	} else {
		close(done)
	}
	httpErr := server.Shutdown(ctx)
	if httpErr != nil {
		_ = server.Close()
	}
	if worker == nil {
		return httpErr
	}
	var workerErr error
	select {
	case <-done:
	case <-ctx.Done():
		worker.Stop()
		<-done
		workerErr = errors.New("worker RPC shutdown deadline exceeded")
	}
	return errors.Join(httpErr, workerErr)
}
