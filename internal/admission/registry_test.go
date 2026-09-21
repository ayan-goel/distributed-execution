package admission

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

const manifest = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":2},"layers":[]}`

func TestRegistryResolutionPinsVerifiedManifest(t *testing.T) {
	calls := 0
	r := RegistryResolver{Allowed: []string{"registry.example.org"}, Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.URL.Host != "registry.example.org" {
			t.Fatal("unexpected host")
		}
		body := "{}"
		header := http.Header{}
		if strings.Contains(req.URL.Path, "/manifests/") {
			body = manifest
			header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		}
		return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})}
	want := fmt.Sprintf("registry.example.org/eval@sha256:%x", sha256.Sum256([]byte(manifest)))
	got, err := r.Resolve(context.Background(), "registry.example.org/eval:latest")
	if err != nil || got != want {
		t.Fatalf("resolution: %s %v", got, err)
	}
	if _, err = r.Resolve(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Resolve(context.Background(), "registry.example.org/eval@sha256:"+strings.Repeat("b", 64)); err == nil {
		t.Fatal("accepted wrong digest")
	}
	before := calls
	if _, err = r.Resolve(context.Background(), "unapproved.example.org/eval:tag"); err == nil || calls != before {
		t.Fatal("unapproved registry contacted")
	}
}

func TestRegistryTransportRejectsRedirectAndAuthEscapes(t *testing.T) {
	for _, target := range []string{"http://registry.example.org/v2/", "https://unapproved.example.org/token"} {
		req, _ := http.NewRequest("GET", target, nil)
		guard := registryTransport{base: roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("unsafe request reached network")
			return nil, nil
		}), hosts: map[string]bool{"registry.example.org": true}}
		if _, err := guard.RoundTrip(req); err == nil {
			t.Fatal("unsafe request accepted")
		}
	}
}

func TestRegistryCancellationAndMalformedResponse(t *testing.T) {
	r := RegistryResolver{Allowed: []string{"registry.example.org"}, Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/vnd.oci.image.manifest.v1+json"}}, Body: io.NopCloser(strings.NewReader("not a manifest")), Request: req}, nil
	})}
	if _, err := r.Resolve(context.Background(), "registry.example.org/eval:tag"); err == nil {
		t.Fatal("malformed manifest accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Resolve(ctx, "registry.example.org/eval:tag"); err == nil {
		t.Fatal("cancelled lookup succeeded")
	}
}
