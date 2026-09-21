//go:build integration

package admission

import (
	"context"
	"io"
	"log"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

func TestRealRegistryTagMovesWithoutChangingPinnedReference(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	tag, err := name.NewTag(host + "/eval:latest")
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(tag, empty.Image); err != nil {
		t.Fatal(err)
	}
	r := RegistryResolver{Allowed: []string{host}, AllowLoopbackHTTP: true}
	first, err := r.Resolve(context.Background(), tag.Name())
	if err != nil {
		t.Fatal(err)
	}
	changed, err := mutate.CreatedAt(empty.Image, v1.Time{Time: time.Unix(1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(tag, changed); err != nil {
		t.Fatal(err)
	}
	second, err := r.Resolve(context.Background(), tag.Name())
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("moving tag did not change digest")
	}
	if recovered, err := r.Resolve(context.Background(), first); err != nil || recovered != first {
		t.Fatal("pinned image identity changed", err)
	}
	r.AllowLoopbackHTTP = false
	if _, err := r.Resolve(context.Background(), tag.Name()); err == nil {
		t.Fatal("implicit plaintext registry access")
	}
}
