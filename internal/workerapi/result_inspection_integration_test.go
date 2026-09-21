//go:build integration

package workerapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	pb "dispatch.local/dispatch/gen/dispatch/worker/v1"
	"dispatch.local/dispatch/internal/api"
	"dispatch.local/dispatch/internal/client"
	"dispatch.local/dispatch/internal/store"
)

func TestCompletedResultInspectionIsProjectScopedAndReplayConsistent(t *testing.T) {
	for _, state := range []string{"SUCCEEDED", "FAILED"} {
		t.Run(state, func(t *testing.T) {
			pool, worker, completion, _ := completionRPCFixture(t)
			ctx := context.Background()
			reader, _, err := store.IssueToken(ctx, pool, "research", store.RoleRead)
			if err != nil {
				t.Fatal(err)
			}
			submitter, _, err := store.IssueToken(ctx, pool, "research", store.RoleSubmit)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, "INSERT INTO projects(name,cpu_quota,memory_quota_mib,concurrency_quota) VALUES('other',4000,8192,4)"); err != nil {
				t.Fatal(err)
			}
			foreign, _, err := store.IssueToken(ctx, pool, "other", store.RoleRead)
			if err != nil {
				t.Fatal(err)
			}
			// No resolver or storage adapter is configured. Existing results and submission
			// recovery must remain readable without reaching either external dependency.
			server := httptest.NewServer(api.New(pool, nil, nil))
			defer server.Close()
			makeClient := func(token string) *client.Client {
				c, err := client.New(server.URL, token, true, nil)
				if err != nil {
					t.Fatal(err)
				}
				return c
			}
			c := makeClient(reader)
			before, err := c.GetJob(ctx, completion.Authority.JobId)
			if err != nil || before.State != "ACTIVE" || before.AcceptedAttemptID != nil || len(before.AcceptedManifest) != 0 && string(before.AcceptedManifest) != "null" {
				t.Fatal("active job exposed a result", before.State, err)
			}
			if state == "FAILED" {
				completion.Reason = pb.FailureReason_OUTPUT_INVALID
				signWireCompletion(t, completion)
			}
			published, err := worker.CompleteAttempt(ctx, completion)
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.GetJob(ctx, completion.Authority.JobId)
			if err != nil || got.State != state {
				t.Fatal(got.State, err)
			}
			if state == "SUCCEEDED" {
				if got.AcceptedAttemptID == nil || *got.AcceptedAttemptID != completion.Authority.AttemptId || string(got.AcceptedManifest) != string(published.AcceptedManifestJson) {
					t.Fatal("HTTP/client lost accepted identity or original manifest")
				}
			} else if got.AcceptedAttemptID != nil || string(got.AcceptedManifest) != "null" {
				t.Fatal("failed attempt diagnostic became canonical")
			}
			var key string
			if err := pool.QueryRow(ctx, "SELECT key FROM idempotency_keys WHERE response_reference=$1", got.ID).Scan(&key); err != nil {
				t.Fatal(err)
			}
			replay, err := makeClient(submitter).Submit(ctx, before.Spec, key)
			if err != nil || replay.State != got.State || string(replay.AcceptedManifest) != string(got.AcceptedManifest) || (replay.AcceptedAttemptID == nil) != (got.AcceptedAttemptID == nil) {
				t.Fatal("submission replay dropped accepted result", err)
			}
			if replay.AcceptedAttemptID != nil && *replay.AcceptedAttemptID != *got.AcceptedAttemptID {
				t.Fatal("submission replay changed accepted attempt")
			}
			_, err = makeClient(foreign).GetJob(ctx, got.ID)
			var remote *client.APIError
			if !errors.As(err, &remote) || remote.Status != 404 {
				t.Fatal("cross-project result disclosed", err)
			}
			// Ensure the JSON surface explicitly includes nulls rather than silently
			// omitting canonical-result fields for jobs without a successful completion.
			encoded, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &fields); err != nil || fields["acceptedManifest"] == nil || fields["acceptedAttemptId"] == nil {
				t.Fatal("result fields missing from client serialization", err)
			}
		})
	}
}
