package admission

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

var ErrRegistryDenied = errors.New("registry is not approved")
var ErrImageUnavailable = errors.New("image could not be resolved")

const maxRegistryBody = 4 << 20

type RegistryResolver struct {
	Allowed           []string
	AuthHosts         []string
	AllowLoopbackHTTP bool
	Transport         http.RoundTripper
}

func (r RegistryResolver) Resolve(ctx context.Context, image string) (string, error) {
	ref, err := name.ParseReference(image)
	if err != nil {
		return "", ErrImageUnavailable
	}
	host := ref.Context().RegistryStr()
	if !slices.Contains(r.Allowed, host) {
		return "", ErrRegistryDenied
	}
	hosts := map[string]bool{host: true}
	for _, h := range r.AuthHosts {
		hosts[h] = true
	}
	if host == name.DefaultRegistry {
		hosts["registry-1.docker.io"] = true
		hosts["auth.docker.io"] = true
	}
	base := r.Transport
	if base == nil {
		base = remote.DefaultTransport
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// The same guard covers registry pings, bearer-token realms, and redirects.
	// A server-provided URL must not expand the operator's outbound allowlist.
	transport := registryTransport{base: base, hosts: hosts, loopbackHTTP: r.AllowLoopbackHTTP}
	desc, err := remote.Get(ref, remote.WithContext(ctx), remote.WithTransport(transport), remote.WithRetryPredicate(func(error) bool { return false }))
	if err != nil {
		return "", ErrImageUnavailable
	}
	if len(desc.Manifest) > maxRegistryBody {
		return "", ErrImageUnavailable
	}
	if !slices.Contains([]types.MediaType{types.OCIManifestSchema1, types.OCIImageIndex, types.DockerManifestSchema2, types.DockerManifestList}, desc.MediaType) {
		return "", ErrImageUnavailable
	}
	var envelope struct {
		SchemaVersion int `json:"schemaVersion"`
	}
	if json.Unmarshal(desc.Manifest, &envelope) != nil || envelope.SchemaVersion != 2 {
		return "", ErrImageUnavailable
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(desc.Manifest))
	if desc.Digest.String() != digest {
		return "", ErrImageUnavailable
	}
	if pinned, ok := ref.(name.Digest); ok && pinned.DigestStr() != digest {
		return "", ErrImageUnavailable
	}
	return ref.Context().Digest(digest).Name(), nil
}

type registryTransport struct {
	base         http.RoundTripper
	hosts        map[string]bool
	loopbackHTTP bool
}

func (t registryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !t.hosts[req.URL.Host] || req.URL.User != nil {
		return nil, ErrRegistryDenied
	}
	if req.URL.Scheme != "https" {
		ip := net.ParseIP(req.URL.Hostname())
		loopback := req.URL.Hostname() == "localhost" || (ip != nil && ip.IsLoopback())
		if req.URL.Scheme != "http" || !t.loopbackHTTP || !loopback {
			return nil, ErrRegistryDenied
		}
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	// Bound all responses, including error/auth bodies; a registry outage must
	// not turn admission into an unbounded memory allocation.
	resp.Body = &boundedBody{Reader: io.LimitReader(resp.Body, maxRegistryBody+1), Closer: resp.Body}
	return resp, nil
}

type boundedBody struct {
	io.Reader
	io.Closer
}
