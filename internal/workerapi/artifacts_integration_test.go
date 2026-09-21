//go:build integration

package workerapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const artifactBody = "verified worker output"

func finalizationRPCFixture(t *testing.T, handler http.HandlerFunc) (*pgxpool.Pool, pb.WorkerServiceClient, *pb.FinalizeUploadRequest) {
	t.Helper()
	pool, client, upload := uploadRPCFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("versioning") {
			enabledVersioning(w, r)
			return
		}
		handler(w, r)
	})
	checksum := sha256.Sum256([]byte(artifactBody))
	upload.Sha256 = hex.EncodeToString(checksum[:])
	upload.SizeBytes = uint64(len(artifactBody))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	grant, err := client.CreateUpload(ctx, upload)
	if err != nil {
		t.Fatal(err)
	}
	return pool, client, &pb.FinalizeUploadRequest{Authority: upload.Authority, RequestId: uuid.NewString(), UploadId: grant.UploadId, Object: &pb.ObjectVersion{Key: grant.ObjectKey, VersionId: "exact-version", SizeBytes: upload.SizeBytes, Sha256: upload.Sha256}}
}

func artifactHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Amz-Version-Id", r.URL.Query().Get("versionId"))
	w.Header().Set("Content-Length", strconv.Itoa(len(artifactBody)))
}

func TestFinalizeRPCRejectsAuthorityChangesDuringObjectBodyRead(t *testing.T) {
	for _, tc := range []struct {
		name, sql, reason string
		code              codes.Code
	}{
		{"lease", "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second'", "UPLOAD_FENCED", codes.FailedPrecondition},
		{"phase", "UPDATE attempts SET phase_deadline=clock_timestamp()-interval '1 second'", "UPLOAD_STOP_REQUESTED", codes.FailedPrecondition},
		{"cancel", "UPDATE jobs SET state='CANCELLING',cancel_requested=true", "UPLOAD_STOP_REQUESTED", codes.FailedPrecondition},
		{"credential", "UPDATE worker_credentials SET revoked_at=clock_timestamp()", "UNAUTHORIZED_WORKER", codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, release := make(chan struct{}, 1), make(chan struct{})
			var once sync.Once
			pool, client, request := finalizationRPCFixture(t, func(w http.ResponseWriter, r *http.Request) {
				artifactHeaders(w, r)
				_, _ = fmt.Fprint(w, artifactBody[:1])
				w.(http.Flusher).Flush()
				started <- struct{}{}
				select {
				case <-release:
					_, _ = fmt.Fprint(w, artifactBody[1:])
				case <-r.Context().Done():
				}
			})
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			failed := make(chan error, 1)
			go func() {
				result, err := client.FinalizeUpload(ctx, request)
				if result != nil {
					err = fmt.Errorf("returned stale verified artifact")
				}
				failed <- err
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("object verification did not start")
			}
			mutationCtx, stop := context.WithTimeout(ctx, time.Second)
			_, err := pool.Exec(mutationCtx, tc.sql)
			stop()
			if err != nil {
				t.Fatal("object read held database locks", err)
			}
			once.Do(func() { close(release) })
			if err := <-failed; status.Code(err) != tc.code || status.Convert(err).Message() != tc.reason {
				t.Fatal("stale verification accepted", err)
			}
			var count int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM artifacts").Scan(&count); err != nil || count != 0 {
				t.Fatal("stale artifact persisted", count, err)
			}
		})
	}
}

func TestFinalizeRPCIntegrityFailuresAndDurableReplay(t *testing.T) {
	var mode, reads atomic.Int32
	pool, client, request := finalizationRPCFixture(t, func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		artifactHeaders(w, r)
		switch mode.Load() {
		case 0:
			w.Header().Set("X-Amz-Version-Id", "wrong-version")
		case 1:
			w.Header().Set("Content-Length", strconv.Itoa(len(artifactBody)+1))
		case 2:
			_, _ = fmt.Fprint(w, "X"+artifactBody[1:])
			return
		case 4:
			w.Header().Del("Content-Length")
			http.Error(w, "private object diagnostic", http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprint(w, artifactBody)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for index := 0; index < 3; index++ {
		mode.Store(int32(index))
		if result, err := client.FinalizeUpload(ctx, request); result != nil || status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "OBJECT_INTEGRITY_MISMATCH" {
			t.Fatal("incorrect object accepted", index, err)
		}
	}
	mode.Store(3)
	first, err := client.FinalizeUpload(ctx, request)
	if err != nil || first.GetArtifactId() == "" || !proto.Equal(first.GetObject(), request.Object) {
		t.Fatal("correct object rejected", err)
	}
	mode.Store(4)
	before := reads.Load()
	if replay, err := client.FinalizeUpload(ctx, request); err != nil || !proto.Equal(replay, first) || reads.Load() != before {
		t.Fatal("durable replay depended on storage", err)
	}
	changed := proto.Clone(request).(*pb.FinalizeUploadRequest)
	changed.Object.VersionId = "changed"
	if result, err := client.FinalizeUpload(ctx, changed); result != nil || status.Code(err) != codes.AlreadyExists {
		t.Fatal("request rebound to another version", err)
	}
	var intents, artifacts, events int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM artifact_finalizations),(SELECT count(*) FROM artifacts),(SELECT count(*) FROM job_events WHERE type='ARTIFACT_VERIFIED')").Scan(&intents, &artifacts, &events); err != nil || intents != 1 || artifacts != 1 || events != 1 {
		t.Fatal("retries duplicated verification state", intents, artifacts, events, err)
	}
}

func TestFinalizeRPCCancelledReadCanRetrySameIntent(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	pool, client, request := finalizationRPCFixture(t, func(w http.ResponseWriter, r *http.Request) {
		artifactHeaders(w, r)
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
			_, _ = fmt.Fprint(w, artifactBody)
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failed := make(chan error, 1)
	go func() { _, err := client.FinalizeUpload(ctx, request); failed <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("verification did not begin")
	}
	cancel()
	if err := <-failed; status.Code(err) != codes.Canceled {
		t.Fatal("cancelled read returned success", err)
	}
	once.Do(func() { close(release) })
	retryCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if result, err := client.FinalizeUpload(retryCtx, request); err != nil || result.GetArtifactId() == "" {
		t.Fatal("cancelled intent could not retry", err)
	}
	var count int
	if err := pool.QueryRow(retryCtx, "SELECT count(*) FROM artifact_finalizations").Scan(&count); err != nil || count != 1 {
		t.Fatal("cancelled read duplicated intent", count, err)
	}
}
