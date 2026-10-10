package objectstore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

const recoveryIdentity = "a34827f1-daf6-43de-97b4-9bfbf29e1fbd"

func TestMultipartRecoverySelectsOnlyOneExactIdentifiedVersion(t *testing.T) {
	for _, mode := range []string{"found", "missing", "ambiguous", "truncated", "bad-version", "wrong-size", "wrong-head-version"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				switch {
				case r.URL.Query().Has("versioning"):
					versioning(w, "Enabled")
				case r.URL.Query().Has("versions"):
					if r.URL.Query().Get("prefix") != "outputs/result" || r.URL.Query().Get("max-keys") != "16" {
						t.Error("recovery listing is not scoped and bounded")
					}
					version := "old-version"
					if mode == "bad-version" {
						version = "null"
					}
					fmt.Fprintf(w, `<ListVersionsResult><IsTruncated>%t</IsTruncated><Version><Key>outputs/result</Key><VersionId>new-version</VersionId></Version><Version><Key>outputs/result</Key><VersionId>%s</VersionId></Version><Version><Key>outputs/result-other</Key><VersionId>unrelated</VersionId></Version></ListVersionsResult>`, mode == "truncated", version)
				case r.Method == "HEAD":
					v := r.URL.Query().Get("versionId")
					if r.URL.Path != "/dispatch-test/outputs/result" || (v != "old-version" && v != "new-version") {
						t.Error("recovery used a latest or unrelated object read")
					}
					w.Header().Set("X-Amz-Version-Id", v)
					w.Header().Set("Content-Length", "5242883")
					if mode != "missing" && (v == "old-version" || mode == "ambiguous") {
						w.Header().Set("X-Amz-Meta-Dispatch-Initialization-Id", recoveryIdentity)
					}
					if mode == "wrong-size" && v == "old-version" {
						w.Header().Set("Content-Length", "1")
					}
					if mode == "wrong-head-version" {
						w.Header().Set("X-Amz-Version-Id", "substituted")
					}
				default:
					t.Error("unexpected recovery operation")
					w.WriteHeader(500)
				}
			}))
			defer server.Close()
			cfg := config(server.URL)
			cfg.MaxObjectBytes = 16 << 20
			s, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			u := MultipartUpload{Key: "outputs/result", UploadID: "backend", Size: 5242883, PartSize: MinMultipartPartBytes}
			version, err := s.RecoverMultipartVersion(context.Background(), u, recoveryIdentity)
			want := ErrIntegrity
			switch mode {
			case "found":
				want = nil
			case "missing":
				want = ErrMultipartGone
			case "truncated":
				want = ErrUnavailable
			}
			if !errors.Is(err, want) || (want == nil && version != "old-version") || (want != nil && version != "") {
				t.Fatal("unsafe recovery", version, err)
			}
		})
	}
}

func TestMultipartInitializationMetadataIsServerBound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("versioning") {
			versioning(w, "Enabled")
			return
		}
		if r.Method != "POST" || !r.URL.Query().Has("uploads") || r.Header.Get("X-Amz-Meta-Dispatch-Initialization-Id") != recoveryIdentity {
			t.Error("initialization identity not bound to storage creation")
		}
		fmt.Fprint(w, `<InitiateMultipartUploadResult><UploadId>backend</UploadId></InitiateMultipartUploadResult>`)
	}))
	defer server.Close()
	cfg := config(server.URL)
	cfg.MaxObjectBytes = 16 << 20
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginIdentifiedMultipart(context.Background(), "outputs/result", 5242883, MinMultipartPartBytes, recoveryIdentity); err != nil {
		t.Fatal(err)
	}
	for _, identity := range []string{"", "not-a-uuid", "00000000-0000-0000-0000-000000000000"} {
		if _, err := s.BeginIdentifiedMultipart(context.Background(), "outputs/result", 5242883, MinMultipartPartBytes, identity); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid initialization accepted", err)
		}
		if _, err := s.RecoverMultipartVersion(context.Background(), MultipartUpload{Key: "outputs/result", UploadID: "backend", Size: 5242883, PartSize: MinMultipartPartBytes}, identity); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid recovery identity accepted", err)
		}
	}
}
