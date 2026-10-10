package objectstore

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMultipartBindsScopeAndOrderedCompletion(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.URL.Query().Has("versioning"):
			versioning(w, "Enabled")
		case r.Method == "POST" && r.URL.Query().Has("uploads"):
			if r.Header.Get("X-Amz-Checksum-Algorithm") != "SHA256" {
				t.Error("multipart checksums not requested")
			}
			fmt.Fprint(w, `<InitiateMultipartUploadResult><UploadId>backend+/id=</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == "POST" && r.URL.Query().Get("uploadId") == "backend+/id=":
			body, _ := io.ReadAll(io.LimitReader(r.Body, 65536))
			var completion struct {
				Parts []struct {
					Number               int `xml:"PartNumber"`
					ETag, ChecksumSHA256 string
				} `xml:"Part"`
			}
			if xml.Unmarshal(body, &completion) != nil || len(completion.Parts) != 2 || completion.Parts[0].Number != 1 || completion.Parts[1].Number != 2 || completion.Parts[0].ChecksumSHA256 == "" {
				t.Error("completion did not preserve ordered part evidence")
			}
			w.Header().Set("X-Amz-Version-Id", "immutable+/v=")
			fmt.Fprint(w, `<CompleteMultipartUploadResult><Key>outputs/result</Key></CompleteMultipartUploadResult>`)
		default:
			t.Error("unexpected storage operation")
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	cfg := config(server.URL)
	cfg.MaxObjectBytes = 16 << 20
	store, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	upload, err := store.BeginMultipart(ctx, "outputs/result", (5<<20)+3, 5<<20)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := store.PresignPart(ctx, upload, 2, checksum("abc"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(grant.URL)
	if grant.Method != "PUT" || u.Path != "/dispatch-test/outputs/result" || u.Query().Get("uploadId") != upload.UploadID || u.Query().Get("partNumber") != "2" || !strings.Contains(u.Query().Get("X-Amz-SignedHeaders"), "content-length") {
		t.Fatal("part capability escaped its upload/part/length scope")
	}
	if u.Query().Get("X-Amz-Checksum-Sha256") == "" && grant.Headers.Get("X-Amz-Checksum-Sha256") == "" {
		t.Fatal("part checksum not bound")
	}
	parts := []CompletedPart{{Number: 1, ETag: `"part-1"`, SHA256: checksum("first")}, {Number: 2, ETag: `"part-2"`, SHA256: checksum("abc")}}
	before := calls.Load()
	for _, bad := range [][]CompletedPart{nil, parts[:1], {parts[1], parts[0]}, {parts[0], parts[0]}, {{Number: 1, ETag: "bad\nheader", SHA256: checksum("first")}, parts[1]}} {
		if _, err := store.CompleteMultipart(ctx, upload, bad); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid completion accepted", err)
		}
	}
	for _, number := range []int32{0, 3, 10001} {
		if _, err := store.PresignPart(ctx, upload, number, checksum("abc"), time.Minute); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid part accepted", err)
		}
	}
	if calls.Load() != before {
		t.Fatal("invalid input reached storage")
	}
	version, err := store.CompleteMultipart(ctx, upload, parts)
	if err != nil || version != "immutable+/v=" {
		t.Fatal("completion did not return the exact version", err)
	}
}

func TestMultipartRejectsUnversionedStorageAndUncertainCompletion(t *testing.T) {
	for _, mode := range []string{"unversioned", "embedded-error", "null-version", "missing-upload"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				if r.URL.Query().Has("versioning") {
					if mode == "unversioned" {
						versioning(w, "Suspended")
					} else {
						versioning(w, "Enabled")
					}
					return
				}
				switch mode {
				case "embedded-error":
					fmt.Fprint(w, `<Error><Code>InternalError</Code><Message>signed-sensitive-diagnostic</Message></Error>`)
				case "missing-upload":
					w.WriteHeader(404)
					fmt.Fprint(w, `<Error><Code>NoSuchUpload</Code></Error>`)
				case "null-version":
					w.Header().Set("X-Amz-Version-Id", "null")
					fmt.Fprint(w, `<CompleteMultipartUploadResult/>`)
				default:
					t.Error("unversioned bucket was mutated")
				}
			}))
			defer server.Close()
			cfg := config(server.URL)
			cfg.MaxObjectBytes = 16 << 20
			store, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			upload := MultipartUpload{Key: "outputs/result", UploadID: "backend-id", Size: (5 << 20) + 3, PartSize: 5 << 20}
			parts := []CompletedPart{{Number: 1, ETag: "first", SHA256: checksum("first")}, {Number: 2, ETag: "last", SHA256: checksum("abc")}}
			_, err = store.CompleteMultipart(context.Background(), upload, parts)
			want := ErrUnavailable
			if mode == "unversioned" {
				want = ErrVersioning
			}
			if mode == "null-version" {
				want = ErrIntegrity
			}
			if mode == "missing-upload" {
				want = ErrMultipartGone
			}
			if !errors.Is(err, want) || strings.Contains(fmt.Sprint(err), "signed-sensitive") {
				t.Fatal("unsafe completion outcome", err)
			}
		})
	}
}

func TestMultipartBoundsAndAbortVerification(t *testing.T) {
	for _, mode := range []string{"gone", "empty", "parts-remain", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/dispatch-test/outputs/result" || r.URL.Query().Get("uploadId") != "owned-upload" {
					t.Error("abort escaped its upload")
				}
				w.Header().Set("Content-Type", "application/xml")
				if mode == "gone" {
					w.WriteHeader(404)
					fmt.Fprint(w, `<Error><Code>NoSuchUpload</Code></Error>`)
					return
				}
				if r.Method == "DELETE" {
					w.WriteHeader(204)
					return
				}
				if r.Method != "GET" || r.URL.Query().Get("max-parts") != "1" {
					t.Error("unbounded abort verification")
				}
				switch mode {
				case "empty":
					fmt.Fprint(w, `<ListPartsResult/>`)
				case "parts-remain":
					fmt.Fprint(w, `<ListPartsResult><Part><PartNumber>1</PartNumber></Part></ListPartsResult>`)
				case "truncated":
					fmt.Fprint(w, `<ListPartsResult><IsTruncated>true</IsTruncated></ListPartsResult>`)
				}
			}))
			defer server.Close()
			cfg := config(server.URL)
			cfg.MaxObjectBytes = 16 << 20
			store, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			for _, plan := range []MultipartUpload{{Key: "../escape", Size: 1, PartSize: 5 << 20}, {Key: "outputs/result", Size: 0, PartSize: 5 << 20}, {Key: "outputs/result", Size: 17 << 20, PartSize: 5 << 20}, {Key: "outputs/result", Size: 1, PartSize: (5 << 20) - 1}, {Key: "outputs/result", Size: 1, PartSize: MaxSinglePartBytes + 1}} {
				if _, err := store.BeginMultipart(ctx, plan.Key, plan.Size, plan.PartSize); !errors.Is(err, ErrInvalid) {
					t.Fatal("invalid upload plan accepted", err)
				}
			}
			if calls.Load() != 0 {
				t.Fatal("invalid plan reached backend")
			}
			upload := MultipartUpload{Key: "outputs/result", UploadID: "owned-upload", Size: 6 << 20, PartSize: 5 << 20}
			bad := upload
			bad.Key = "../escape"
			if err := store.AbortMultipart(ctx, bad); !errors.Is(err, ErrInvalid) {
				t.Fatal("unsafe abort identity accepted", err)
			}
			cfg.MaxObjectBytes = 1024
			store, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			err = store.AbortMultipart(ctx, upload)
			if mode == "parts-remain" || mode == "truncated" {
				if !errors.Is(err, ErrUnavailable) {
					t.Fatal("abort falsely confirmed cleanup", err)
				}
			} else if err != nil {
				t.Fatal("abort did not reconcile absence", err)
			}
		})
	}
}
