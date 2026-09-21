package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func config(endpoint string) Config {
	return Config{Endpoint: endpoint, Region: "us-east-1", Bucket: "dispatch-test", AccessKey: "fixture-access", SecretKey: "fixture-secret", AllowLoopbackHTTP: true, MaxObjectBytes: 1024, MaxConcurrent: 1}
}
func checksum(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}
func versioning(w http.ResponseWriter, status string) {
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprintf(w, `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>%s</Status></VersioningConfiguration>`, status)
}
func TestConfigurationRejectsImplicitCredentialsAndUnscopedEndpoints(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Endpoint = "" },
		func(c *Config) { c.Endpoint = "https://:443" },
		func(c *Config) { c.Endpoint = "http://example.com" },
		func(c *Config) { c.AllowLoopbackHTTP = false },
		func(c *Config) { c.Endpoint = "https://user:pass@example.com" },
		func(c *Config) { c.Endpoint = "https://example.com/path" },
		func(c *Config) { c.Endpoint = "https://example.com?bucket=x" },
		func(c *Config) { c.Endpoint = "https://example.com#x" },
		func(c *Config) { c.AccessKey = "" },
		func(c *Config) { c.SecretKey = "" },
		func(c *Config) { c.Region = "" },
		func(c *Config) { c.Bucket = "../bucket" },
		func(c *Config) { c.MaxObjectBytes = MaxSinglePartBytes + 1 },
		func(c *Config) { c.MaxConcurrent = 65 },
	} {
		c := config("http://127.0.0.1:1234")
		mutate(&c)
		if _, err := New(c); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid config accepted: %v", err)
		}
	}
	c := config("https://objects.example.com")
	c.AllowLoopbackHTTP = false
	if _, err := New(c); err != nil {
		t.Fatal(err)
	}
}

func TestPresignBindsOneKeySizeChecksumAndVersion(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/dispatch-test" || !r.URL.Query().Has("versioning") {
			t.Error("unexpected capability check", r.URL.Path)
		}
		versioning(w, "Enabled")
	}))
	defer server.Close()
	store, err := New(config(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	key := "projects/p/jobs/j/attempts/a/uploads/u"
	grant, err := store.PresignUpload(context.Background(), key, 3, checksum("abc"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(grant.URL)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Method != "PUT" || u.Path != "/dispatch-test/"+key || u.Query().Get("X-Amz-Expires") != "60" || u.Query().Get("X-Amz-Signature") == "" {
		t.Fatal("unscoped upload grant")
	}
	if !strings.Contains(u.Query().Get("X-Amz-SignedHeaders"), "content-length") {
		t.Fatal("upload size is not signed")
	}
	if u.Query().Get("X-Amz-Checksum-Sha256") == "" && grant.Headers.Get("X-Amz-Checksum-Sha256") == "" {
		t.Fatal("checksum is not bound")
	}
	if calls.Load() != 1 {
		t.Fatal("versioning not checked")
	}
	obj := Object{Key: key, Version: "v+/opaque=", Size: 3, SHA256: checksum("abc")}
	download, err := store.PresignDownload(context.Background(), obj, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, _ = url.Parse(download.URL)
	if download.Method != "GET" || u.Query().Get("versionId") != obj.Version {
		t.Fatal("download did not pin exact opaque version")
	}
	for _, bad := range []Object{
		{Key: "../escape", Version: "v", Size: 3, SHA256: obj.SHA256},
		{Key: key, Version: "null", Size: 3, SHA256: obj.SHA256},
		{Key: key, Version: "", Size: 3, SHA256: obj.SHA256},
		{Key: key, Version: "v", Size: 1025, SHA256: obj.SHA256},
		{Key: key, Version: "v", Size: 3, SHA256: "BAD"},
	} {
		if _, err := store.PresignDownload(context.Background(), bad, time.Minute); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid object accepted", err)
		}
	}
	for _, ttl := range []time.Duration{0, time.Millisecond, 6 * time.Minute} {
		if _, err := store.PresignUpload(context.Background(), key, 3, obj.SHA256, ttl); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid expiry accepted", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("invalid inputs reached storage")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.PresignDownload(ctx, obj, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled request minted a capability", err)
	}
}

func TestVerifyChecksBytesAndExactVersionRatherThanETag(t *testing.T) {
	for _, tc := range []struct {
		name, body, version string
		size                int64
		valid               bool
	}{
		{"valid", "abc", "version-one", 3, true},
		{"wrong-bytes", "xyz", "version-one", 3, false},
		{"wrong-version", "abc", "latest-version", 3, false},
		{"unversioned", "abc", "null", 3, false},
		{"wrong-size", "abcd", "version-one", 4, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("versionId") != "version-one" {
					t.Error("verification read latest object")
				}
				w.Header().Set("X-Amz-Version-Id", tc.version)
				w.Header().Set("Content-Length", fmt.Sprint(tc.size))
				w.Header().Set("ETag", checksum("abc"))
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			store, _ := New(config(server.URL))
			err := store.Verify(context.Background(), Object{Key: "outputs/object", Version: "version-one", Size: 3, SHA256: checksum("abc")})
			if tc.valid && err != nil || !tc.valid && !errors.Is(err, ErrIntegrity) {
				t.Fatal("unexpected integrity result", err)
			}
		})
	}
}

func TestVerificationDoesNotFollowRedirectsOrWaitBeyondCallerDeadline(t *testing.T) {
	var leaked atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1) }))
	defer other.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	store, _ := New(config(redirect.URL))
	obj := Object{Key: "outputs/object", Version: "v", Size: 3, SHA256: checksum("abc")}
	if err := store.Verify(context.Background(), obj); err == nil || leaked.Load() != 0 {
		t.Fatal("followed storage redirect", err)
	}

	entered := make(chan struct{}, 1)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { entered <- struct{}{}; <-r.Context().Done() }))
	defer slow.Close()
	store, _ = New(config(slow.URL))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- store.Verify(ctx, obj) }()
	<-entered
	queued, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := store.Verify(queued, obj); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("queued verification ignored deadline", err)
	}
	if err := <-finished; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("stalled transfer ignored deadline", err)
	}
	select {
	case <-entered:
		t.Fatal("concurrency bound was exceeded")
	default:
	}
}
