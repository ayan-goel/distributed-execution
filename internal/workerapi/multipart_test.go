package workerapi

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/objectstore"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMultipartInitializationPreservesBackendOnUnknownBindOutcome(t *testing.T) {
	var aborted atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.URL.Query().Has("versioning"):
			_, _ = fmt.Fprint(w, `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)
		case r.Method == http.MethodPost:
			_, _ = fmt.Fprint(w, `<InitiateMultipartUploadResult><UploadId>backend-upload</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodDelete:
			aborted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprint(w, `<Error><Code>NoSuchUpload</Code></Error>`)
		}
	}))
	defer backend.Close()
	objects, err := objectstore.New(objectstore.Config{Endpoint: backend.URL, Region: "us-east-1", Bucket: "dispatch-test", AccessKey: "fixture-access", SecretKey: "fixture-secret", AllowLoopbackHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://fixture:fixture@127.0.0.1:1/fixture?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	id := store.WorkerIdentity{WorkerID: uuid.NewString(), CredentialID: uuid.NewString()}
	a := store.AttemptAuthority{JobID: uuid.NewString(), AttemptID: uuid.NewString(), WorkerID: id.WorkerID, SessionID: uuid.NewString(), Generation: 1}
	u := store.UploadRecord{UploadID: uuid.NewString(), ObjectKey: "projects/p/jobs/j/attempts/a/outputs/result", SizeBytes: (5 << 20) + 1, PartSizeBytes: 5 << 20, PartCount: 2, InitializationID: uuid.NewString()}
	r := store.UploadRequest{Authority: a, RequestID: uuid.NewString(), Kind: "OUTPUT", LogicalName: "result", SizeBytes: u.SizeBytes, SHA256: strings.Repeat("a", 64), PartCount: 2, PartSizeBytes: u.PartSizeBytes}
	// A generic database error cannot prove whether a binding committed. Preserve
	// the backend so a lost commit reply cannot leave durable metadata dangling.
	_, err = NewService(pool, store.AcquisitionPolicy{}, objects).createMultipart(context.Background(), id, r, store.UploadResult{Decision: "ACCEPTED", Upload: &u})
	if status.Code(err) != codes.Unavailable {
		t.Fatal("unknown binding outcome did not surface as retryable", err)
	}
	if aborted.Load() {
		t.Fatal("unknown binding outcome deleted a potentially committed backend")
	}
}

func TestGrantUploadPartRequiresIdentityAndBoundedFields(t *testing.T) {
	s := NewService(nil, store.AcquisitionPolicy{}, nil)
	if _, err := s.GrantUploadPart(context.Background(), nil); status.Code(err) != codes.Unauthenticated {
		t.Fatal("part grant bypassed transport identity", err)
	}
	id := store.WorkerIdentity{WorkerID: uuid.NewString(), CredentialID: uuid.NewString()}
	ctx := context.WithValue(context.Background(), identityKey{}, id)
	for _, change := range []func(*pb.GrantUploadPartRequest){
		func(r *pb.GrantUploadPartRequest) { r.Authority = nil },
		func(r *pb.GrantUploadPartRequest) { r.Authority.Generation = math.MaxUint64 },
		func(r *pb.GrantUploadPartRequest) { r.Number = 0 },
		func(r *pb.GrantUploadPartRequest) { r.Number = store.MaxUploadParts + 1 },
	} {
		r := &pb.GrantUploadPartRequest{Authority: &pb.AttemptAuthority{WorkerId: id.WorkerID, Generation: 1}, Number: 1}
		change(r)
		if _, err := s.GrantUploadPart(ctx, r); status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid part wire fields accepted", err)
		}
	}
	r := &pb.GrantUploadPartRequest{Authority: &pb.AttemptAuthority{WorkerId: uuid.NewString()}, Number: 1}
	if _, err := s.GrantUploadPart(ctx, r); status.Code(err) != codes.PermissionDenied {
		t.Fatal("cross-worker part grant accepted", err)
	}
}
