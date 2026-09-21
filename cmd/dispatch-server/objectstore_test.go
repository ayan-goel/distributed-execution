package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestObjectStorageConfigurationUsesOnlyExplicitCredentials(t *testing.T) {
	base := []string{"--dev-insecure", "--allow-registry", "index.docker.io"}
	for _, args := range [][]string{
		{"--object-region", "us-east-1"}, {"--object-bucket", "dispatch-test"}, {"--object-dev-loopback"},
		{"--object-endpoint", "https://storage.example"},
	} {
		if _, err := parseServeConfig(append(append([]string{}, base...), args...)); err == nil {
			t.Fatal("partial storage settings accepted")
		}
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)
	}))
	defer backend.Close()
	args := append(base, "--object-endpoint", backend.URL, "--object-region", "us-east-1", "--object-bucket", "dispatch-test", "--object-dev-loopback")
	c, err := parseServeConfig(args)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "unrelated-account")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "unrelated-secret")
	t.Setenv("DISPATCH_S3_ACCESS_KEY", "")
	t.Setenv("DISPATCH_S3_SECRET_KEY", "")
	t.Setenv("DISPATCH_S3_SESSION_TOKEN", "")
	if _, err := configuredObjectStore(context.Background(), c); err == nil {
		t.Fatal("ambient credentials enabled storage")
	}
	t.Setenv("DISPATCH_S3_ACCESS_KEY", "fixture-access")
	t.Setenv("DISPATCH_S3_SECRET_KEY", "fixture-secret")
	if objects, err := configuredObjectStore(context.Background(), c); err != nil || objects == nil {
		t.Fatal("explicit local backend rejected", err)
	}
	c.objectLoopback = false
	if _, err := configuredObjectStore(context.Background(), c); err == nil {
		t.Fatal("plaintext storage accepted without explicit opt-in")
	}
	if objects, err := configuredObjectStore(context.Background(), serveConfig{}); err != nil || objects != nil {
		t.Fatal("environment enabled unconfigured storage", err)
	}
}

func TestObjectStorageStartupRejectsUnversionedBucket(t *testing.T) {
	t.Setenv("DISPATCH_S3_ACCESS_KEY", "fixture-access")
	t.Setenv("DISPATCH_S3_SECRET_KEY", "fixture-secret")
	t.Setenv("DISPATCH_S3_SESSION_TOKEN", "")
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprint(w, `<VersioningConfiguration/>`) }))
	defer backend.Close()
	_, err := configuredObjectStore(context.Background(), serveConfig{objectEndpoint: backend.URL, objectRegion: "us-east-1", objectBucket: "dispatch-test", objectLoopback: true})
	if err == nil {
		t.Fatal("server accepted an unversioned bucket")
	}
}
