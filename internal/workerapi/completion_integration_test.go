//go:build integration

package workerapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func signWireCompletion(t *testing.T, r *pb.CompleteAttemptRequest) {
	t.Helper()
	a := r.Authority
	// Build the expected store contract independently of the RPC conversion helper.
	request := store.CompletionRequest{Authority: store.AttemptAuthority{JobID: a.JobId, AttemptID: a.AttemptId, WorkerID: a.WorkerId, SessionID: a.SessionId, Generation: int64(a.Generation)}, CompletionID: r.CompletionId, ExitCode: r.ExitCode, Stopped: r.Stopped, LogsComplete: r.LogsComplete, MetricsJSON: r.MetricsJson}
	if r.Reason != pb.FailureReason_FAILURE_REASON_UNSPECIFIED {
		request.Reason = r.Reason.String()
	}
	for _, o := range r.Outputs {
		request.Outputs = append(request.Outputs, store.CompletionOutput{Name: o.Name, ArtifactID: o.ArtifactId})
	}
	for _, gap := range r.Gaps {
		stream := "stdout"
		if gap.Stream == pb.LogStream_STDERR {
			stream = "stderr"
		}
		request.Gaps = append(request.Gaps, store.CompletionLogGap{Stream: stream, First: int64(gap.FirstSequence), Last: int64(gap.LastSequence)})
	}
	digest, err := store.CompletionDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	r.PayloadSha256 = digest
}

func completionRPCFixture(t *testing.T) (*pgxpool.Pool, pb.WorkerServiceClient, *pb.CompleteAttemptRequest, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	pool, client, finalize := finalizationRPCFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if calls.Load() > 1 {
			http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
			return
		}
		artifactHeaders(w, r)
		_, _ = fmt.Fprint(w, artifactBody)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	artifact, err := client.FinalizeUpload(ctx, finalize)
	if err != nil {
		t.Fatal(err)
	}
	r := validCompletionWire()
	r.Authority = finalize.Authority
	r.Outputs = []*pb.OutputReference{{Name: "result", ArtifactId: artifact.ArtifactId}}
	signWireCompletion(t, r)
	return pool, client, r, calls
}

func TestMTLSCompletionPublishesOnceAndReplaysWithoutStorage(t *testing.T) {
	pool, client, request, calls := completionRPCFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type outcome struct {
		reply *pb.CompleteAttemptResponse
		err   error
	}
	results := make(chan outcome, 16)
	for range 16 {
		go func() { reply, err := client.CompleteAttempt(ctx, request); results <- outcome{reply, err} }()
	}
	var first *pb.CompleteAttemptResponse
	for range 16 {
		got := <-results
		if got.err != nil || got.reply.GetDecision() != pb.Decision_ACCEPTED || got.reply.GetState() != pb.AttemptState_SUCCEEDED || len(got.reply.GetAcceptedManifestJson()) == 0 {
			t.Fatal(got.reply, got.err)
		}
		if first == nil {
			first = got.reply
		} else if !proto.Equal(first, got.reply) {
			t.Fatal("replay changed canonical bytes")
		}
	}
	var raw []byte
	var completions, events int
	var reservation string
	if err := pool.QueryRow(ctx, `SELECT c.manifest_json,(SELECT count(*) FROM attempt_completions),(SELECT count(*) FROM job_events WHERE type='ATTEMPT_COMPLETED'),r.state FROM attempt_completions c JOIN reservations r ON r.attempt_id=c.attempt_id`).Scan(&raw, &completions, &events, &reservation); err != nil || !bytes.Equal(raw, first.AcceptedManifestJson) || completions != 1 || events != 1 || reservation != "released" {
		t.Fatal("publication not atomic/replay stable", completions, events, reservation, err)
	}
	var manifest struct {
		Outputs []struct {
			ArtifactID string               `json:"artifactId"`
			Object     store.ArtifactObject `json:"object"`
		} `json:"outputs"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil || len(manifest.Outputs) != 1 || manifest.Outputs[0].ArtifactID != request.Outputs[0].ArtifactId || manifest.Outputs[0].Object.Version != "exact-version" {
		t.Fatal("manifest lost verified version", err)
	}
	changed := proto.Clone(request).(*pb.CompleteAttemptRequest)
	changed.LogsComplete = false
	signWireCompletion(t, changed)
	if _, err := client.CompleteAttempt(ctx, changed); status.Code(err) != codes.AlreadyExists {
		t.Fatal("changed evidence accepted", err)
	}
	if calls.Load() != 1 {
		t.Fatal("completion called unavailable storage", calls.Load())
	}
	if _, err := pool.Exec(ctx, "UPDATE worker_credentials SET revoked_at=clock_timestamp()"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CompleteAttempt(ctx, request); status.Code(err) != codes.Unauthenticated {
		t.Fatal("revoked certificate replay accepted", err)
	}
}

func TestMTLSCompletionRejectsMissingOutputAndSpoofedAuthority(t *testing.T) {
	pool, client, request, _ := completionRPCFixture(t)
	ctx := context.Background()
	missing := proto.Clone(request).(*pb.CompleteAttemptRequest)
	missing.Outputs = nil
	signWireCompletion(t, missing)
	if _, err := client.CompleteAttempt(ctx, missing); status.Code(err) != codes.InvalidArgument {
		t.Fatal("missing required output accepted", err)
	}
	spoofed := proto.Clone(request).(*pb.CompleteAttemptRequest)
	spoofed.Authority.WorkerId = uuid.NewString()
	signWireCompletion(t, spoofed)
	if _, err := client.CompleteAttempt(ctx, spoofed); status.Code(err) != codes.PermissionDenied {
		t.Fatal("cross-worker completion accepted", err)
	}
	foreign := proto.Clone(request).(*pb.CompleteAttemptRequest)
	foreign.Authority.AttemptId = uuid.NewString()
	signWireCompletion(t, foreign)
	if reply, err := client.CompleteAttempt(ctx, foreign); err != nil || reply.GetDecision() != pb.Decision_FENCED || reply.GetState() != pb.AttemptState_ATTEMPT_STATE_UNSPECIFIED || len(reply.GetAcceptedManifestJson()) != 0 {
		t.Fatal(reply, err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM attempt_completions").Scan(&count); err != nil || count != 0 {
		t.Fatal("rejected completion mutated state", count, err)
	}
}

func TestMTLSCompletionFencesExpiryAndAcknowledgesCancellation(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		decision  pb.Decision
	}{
		{"lease", "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second'", pb.Decision_FENCED},
		{"phase", "UPDATE attempts SET phase_deadline=clock_timestamp()-interval '1 second'", pb.Decision_STOP_REQUESTED},
		{"cancel", "UPDATE jobs SET state='CANCELLING',cancel_requested=true", pb.Decision_STOP_REQUESTED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, client, request, _ := completionRPCFixture(t)
			ctx := context.Background()
			if _, err := pool.Exec(ctx, tc.sql); err != nil {
				t.Fatal(err)
			}
			if reply, err := client.CompleteAttempt(ctx, request); err != nil || reply.GetDecision() != tc.decision || reply.GetState() != pb.AttemptState_FINALIZING || len(reply.GetAcceptedManifestJson()) != 0 {
				t.Fatal(reply, err)
			}
			if tc.name == "cancel" {
				request.Reason = pb.FailureReason_USER_CANCELLED
				signWireCompletion(t, request)
				if reply, err := client.CompleteAttempt(ctx, request); err != nil || reply.GetDecision() != pb.Decision_ACCEPTED || reply.GetState() != pb.AttemptState_CANCELLED || len(reply.GetAcceptedManifestJson()) != 0 {
					t.Fatal(reply, err)
				}
			}
		})
	}
}

func TestCompletionWithoutConfiguredStorageAndAfterDatabaseRollback(t *testing.T) {
	pool, client, request, _ := completionRPCFixture(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_completion_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.type='ATTEMPT_COMPLETED' THEN RAISE EXCEPTION 'private failure detail'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_completion_event BEFORE INSERT ON job_events FOR EACH ROW EXECUTE FUNCTION reject_completion_event()`); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CompleteAttempt(ctx, request); status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "DATABASE_UNAVAILABLE" {
		t.Fatal("database failure not redacted", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM attempt_completions").Scan(&count); err != nil || count != 0 {
		t.Fatal("failed RPC left partial publication", count, err)
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER reject_completion_event ON job_events"); err != nil {
		t.Fatal(err)
	}
	id := store.WorkerIdentity{WorkerID: request.Authority.WorkerId}
	if err := pool.QueryRow(ctx, "SELECT id::text FROM worker_credentials WHERE worker_id=$1", id.WorkerID).Scan(&id.CredentialID); err != nil {
		t.Fatal(err)
	}
	direct := NewService(pool, store.AcquisitionPolicy{}, nil)
	reply, err := direct.CompleteAttempt(context.WithValue(ctx, identityKey{}, id), request)
	if err != nil || reply.GetState() != pb.AttemptState_SUCCEEDED {
		t.Fatal("verified result depended on configured storage", reply, err)
	}
	// Discard the first response and retry over mTLS; the committed result must be
	// recoverable even though the worker did not persist that acknowledgement.
	if replay, err := client.CompleteAttempt(ctx, request); err != nil || !proto.Equal(replay, reply) {
		t.Fatal("committed response not recoverable", err)
	}
}
