//go:build integration

package workerapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/api"
	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAcceptedArtifactDownloadGrantsAreScopedAndVersionPinned(t *testing.T) {
	for _, success := range []bool{true, false} {
		t.Run(map[bool]string{true: "success", false: "failure"}[success], func(t *testing.T) {
			pool, worker, completion, _ := completionRPCFixture(t)
			ctx := context.Background()
			token, tokenID, err := store.IssueToken(ctx, pool, "research", store.RoleRead)
			if err != nil {
				t.Fatal(err)
			}
			objects, err := objectstore.New(objectstore.Config{Endpoint: "https://storage.example.org", Region: "test", Bucket: "results", AccessKey: "test-access", SecretKey: "test-secret"})
			if err != nil {
				t.Fatal(err)
			}
			h := api.New(pool, nil, objects)
			path := "/v1/jobs/" + completion.Authority.JobId + "/artifacts"
			call := func(handler http.Handler, token, path string) *httptest.ResponseRecorder {
				r := httptest.NewRequest(http.MethodGet, path, nil)
				if token != "" {
					r.Header.Set("Authorization", "Bearer "+token)
				}
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				return w
			}
			if w := call(h, "", path); w.Code != 401 {
				t.Fatal("unauthenticated download grant", w.Code)
			}
			if w := call(h, token, "/v1/jobs/"+uuid.NewString()+"/artifacts"); w.Code != 404 {
				t.Fatal("unknown job exposed grants", w.Code)
			}
			before := call(api.New(pool, nil, nil), token, path)
			var pending api.ArtifactList
			if err := json.Unmarshal(before.Body.Bytes(), &pending); err != nil || before.Code != 200 || len(pending.Artifacts) != 0 || pending.AcceptedAttemptID != nil {
				t.Fatal("pending output downloadable", before.Code, err)
			}
			if !success {
				completion.Reason = pb.FailureReason_OUTPUT_INVALID
				signWireCompletion(t, completion)
			}
			if _, err := worker.CompleteAttempt(ctx, completion); err != nil {
				t.Fatal(err)
			}
			w := call(h, token, path)
			var result api.ArtifactList
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != 200 || result.JobID != completion.Authority.JobId || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("artifact response failed", w.Code, err)
			}
			if success {
				if len(result.Artifacts) != 1 || result.AcceptedAttemptID == nil || *result.AcceptedAttemptID != completion.Authority.AttemptId {
					t.Fatal("accepted outputs missing")
				}
				a := result.Artifacts[0]
				u, err := url.Parse(a.DownloadURL)
				if err != nil || u.Host != "storage.example.org" || u.Query().Get("versionId") != "exact-version" || u.Query().Get("X-Amz-Expires") != "60" || !strings.HasSuffix(u.Path, "/"+a.Object.Key) || a.Method != "GET" || a.ArtifactID != completion.Outputs[0].ArtifactId || a.Object.Version != "exact-version" || a.Object.SizeBytes != int64(len(artifactBody)) || !a.ExpiresAt.After(time.Now()) || a.ExpiresAt.After(time.Now().Add(time.Minute)) {
					t.Fatal("download capability not pinned/bounded")
				}
				if absent := call(api.New(pool, nil, nil), token, path); absent.Code != 503 {
					t.Fatal("missing storage fabricated grant", absent.Code)
				}
			} else if len(result.Artifacts) != 0 || result.AcceptedAttemptID != nil {
				t.Fatal("failed-attempt artifacts exposed as accepted")
			}
			if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES('other',4000,8192,4)"); err != nil {
				t.Fatal(err)
			}
			foreign, _, err := store.IssueToken(ctx, pool, "other", store.RoleRead)
			if err != nil {
				t.Fatal(err)
			}
			if w := call(h, foreign, path); w.Code != 404 || strings.Contains(w.Body.String(), "X-Amz") {
				t.Fatal("cross-project capability disclosed", w.Code)
			}
			if err := store.RevokeToken(ctx, pool, "research", tokenID); err != nil {
				t.Fatal(err)
			}
			if w := call(h, token, path); w.Code != 401 {
				t.Fatal("revoked token minted grant", w.Code)
			}
		})
	}
}

type downloadSignerFunc func(context.Context, objectstore.Object, time.Duration) (objectstore.Grant, error)

func (f downloadSignerFunc) PresignDownload(ctx context.Context, o objectstore.Object, ttl time.Duration) (objectstore.Grant, error) {
	return f(ctx, o, ttl)
}

func TestDownloadSigningRechecksRevocationAndRedactsFailures(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(map[bool]string{false: "failure", true: "revoked while signing"}[revoke], func(t *testing.T) {
			pool, worker, completion, _ := completionRPCFixture(t)
			ctx := context.Background()
			if _, err := worker.CompleteAttempt(ctx, completion); err != nil {
				t.Fatal(err)
			}
			token, tokenID, err := store.IssueToken(ctx, pool, "research", store.RoleRead)
			if err != nil {
				t.Fatal(err)
			}
			signer := downloadSignerFunc(func(ctx context.Context, o objectstore.Object, ttl time.Duration) (objectstore.Grant, error) {
				if !revoke {
					return objectstore.Grant{}, errors.New("private signing diagnostic and secret URL")
				}
				bounded, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				if err := store.RevokeToken(bounded, pool, "research", tokenID); err != nil {
					t.Fatal("download signing held an authorization lock", err)
				}
				return objectstore.Grant{URL: "https://storage.example.org/private-capability", Method: "GET", ExpiresAt: time.Now().Add(ttl)}, nil
			})
			h := api.New(pool, nil, signer)
			r := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+completion.Authority.JobId+"/artifacts", nil)
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			expected := 503
			if revoke {
				expected = 401
			}
			if w.Code != expected || strings.Contains(w.Body.String(), "private") || strings.Contains(w.Body.String(), "storage.example.org") {
				t.Fatal("signing failure/revocation exposed a capability or diagnostic", w.Code)
			}
		})
	}
}

func verifyAcceptedDownload(t *testing.T, pool *pgxpool.Pool, worker pb.WorkerServiceClient, objects *objectstore.Store, authority *pb.AttemptAuthority, artifact *pb.FinalizeUploadResponse, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	completion := validCompletionWire()
	completion.Authority = authority
	completion.Outputs = []*pb.OutputReference{{Name: "result", ArtifactId: artifact.ArtifactId}}
	signWireCompletion(t, completion)
	if _, err := worker.CompleteAttempt(ctx, completion); err != nil {
		t.Fatal(err)
	}
	token, _, err := store.IssueToken(ctx, pool, "research", store.RoleRead)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.New(pool, nil, objects))
	defer server.Close()
	consumer, err := client.New(server.URL, token, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "verified-output")
	receipt, err := consumer.DownloadArtifact(ctx, authority.JobId, "result", destination)
	if err != nil || receipt.ArtifactID != artifact.ArtifactId || receipt.Version != artifact.Object.VersionId {
		t.Fatal("client did not verify accepted download", err)
	}
	local, err := os.ReadFile(destination)
	if err != nil || string(local) != body {
		t.Fatal("client published different bytes", err)
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v1/jobs/"+authority.JobId+"/artifacts", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+token)
	transport := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := transport.Do(r)
	if err != nil {
		t.Fatal("artifact metadata request failed")
	}
	var list api.ArtifactList
	err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&list)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != 200 || len(list.Artifacts) != 1 {
		t.Fatal("artifact capability missing", response.StatusCode, err)
	}
	a := list.Artifacts[0]
	download, err := http.NewRequestWithContext(ctx, a.Method, a.DownloadURL, nil)
	if err != nil {
		t.Fatal("invalid download capability")
	}
	download.Header = a.RequiredHeaders.Clone()
	downloaded, err := transport.Do(download)
	if err != nil {
		t.Fatal("accepted version download failed")
	}
	content, err := io.ReadAll(io.LimitReader(downloaded.Body, int64(len(body)+1)))
	_ = downloaded.Body.Close()
	if err != nil || downloaded.StatusCode != 200 || string(content) != body || downloaded.Header.Get("X-Amz-Version-Id") != artifact.Object.VersionId || a.Object.Version != artifact.Object.VersionId || a.Object.SHA256 != artifact.Object.Sha256 {
		t.Fatal("download returned a replacement or unverified version", downloaded.StatusCode)
	}
	// Removing the signed version changes the capability. It must never turn the
	// accepted-version grant into permission to fetch the current object at the key.
	u, err := url.Parse(a.DownloadURL)
	if err != nil {
		t.Fatal("invalid signed URL")
	}
	q := u.Query()
	q.Del("versionId")
	u.RawQuery = q.Encode()
	tampered, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		t.Fatal("invalid tampered request")
	}
	tampered.Header = a.RequiredHeaders.Clone()
	denied, err := transport.Do(tampered)
	if err != nil {
		t.Fatal("tampered download transport failed")
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(denied.Body, 65536))
	_ = denied.Body.Close()
	if denied.StatusCode == 200 {
		t.Fatal("download capability permitted an unsigned current-version read")
	}
	verifyDownloadCLI(t, server.URL, token, authority.JobId, artifact, body)
}

func verifyDownloadCLI(t *testing.T, endpoint, token, jobID string, artifact *pb.FinalizeUploadResponse, body string) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "dispatch")
	goTool := os.Getenv("GO")
	if goTool == "" {
		goTool = "go"
	}
	buildCtx, stopBuild := context.WithTimeout(context.Background(), time.Minute)
	defer stopBuild()
	build := exec.CommandContext(buildCtx, goTool, "build", "-o", binary, "./cmd/dispatch")
	build.Dir = "../.."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("CLI build failed: %v: %s", err, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	destination := filepath.Join(dir, "cli-output")
	run := func() ([]byte, []byte, error) {
		command := exec.CommandContext(ctx, binary, "artifacts", "download", jobID, "result", "--output", destination, "--json")
		command.Env = append(os.Environ(), "DISPATCH_URL="+endpoint, "DISPATCH_TOKEN="+token, "DISPATCH_DEV_INSECURE=1")
		var out, errs bytes.Buffer
		command.Stdout, command.Stderr = &out, &errs
		err := command.Run()
		return out.Bytes(), errs.Bytes(), err
	}
	output, diagnostics, err := run()
	if err != nil || len(diagnostics) != 0 || bytes.Contains(output, []byte(token)) || bytes.Contains(output, []byte("X-Amz")) {
		t.Fatal("CLI download failed or exposed credentials", err)
	}
	var receipt client.DownloadReceipt
	if err := json.Unmarshal(output, &receipt); err != nil || receipt.JobID != jobID || receipt.ArtifactID != artifact.ArtifactId || receipt.Version != artifact.Object.VersionId || receipt.Path != destination || receipt.SHA256 != artifact.Object.Sha256 {
		t.Fatal("CLI receipt lost accepted identity", err)
	}
	content, err := os.ReadFile(destination)
	if err != nil || string(content) != body {
		t.Fatal("CLI did not publish accepted bytes", err)
	}
	if output, _, err := run(); err == nil || len(output) != 0 {
		t.Fatal("CLI overwrite attempt did not fail")
	}
	content, err = os.ReadFile(destination)
	if err != nil || string(content) != body {
		t.Fatal("CLI overwrite attempt changed accepted bytes", err)
	}
}
