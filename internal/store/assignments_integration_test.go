//go:build integration

package store

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

func TestAssignmentInventoryPagesDoNotRenewOrDuplicate(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	var expected []string
	for range 3 {
		queueAcquisitionJob(t, pool, func(j *spec.Job) { j.Spec.Resources = spec.Resources{CPUMillis: 1, MemoryMiB: 1, ScratchMiB: 1} })
		result, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
		if err != nil || result.Assignment == nil {
			t.Fatal(result, err)
		}
		expected = append(expected, result.Assignment.Authority.JobID)
	}
	sort.Strings(expected)
	cursor := ""
	for n := range 3 {
		page, err := ListAssignments(ctx, pool, id, registration.SessionID, cursor, 1)
		if err != nil || len(page.Assignments) != 1 || page.Assignments[0].Authority.JobID != expected[n] {
			t.Fatal("incorrect inventory page", page, err)
		}
		again, err := ListAssignments(ctx, pool, id, registration.SessionID, cursor, 1)
		if err != nil || len(again.Assignments) != 1 || !again.Assignments[0].LeaseExpiresAt.Equal(page.Assignments[0].LeaseExpiresAt) {
			t.Fatal("inventory renewed authority", again, err)
		}
		cursor = page.NextAfterJobID
		if (n == 2) != (cursor == "") {
			t.Fatal("pagination cursor did not terminate", n, cursor)
		}
	}
	if _, err := ListAssignments(ctx, pool, id, registration.SessionID, "not-a-uuid", 1); !errors.Is(err, ErrInvalid) {
		t.Fatal("malformed cursor accepted", err)
	}
	if _, err := ListAssignments(ctx, pool, id, registration.SessionID, "", 65); !errors.Is(err, ErrInvalid) {
		t.Fatal("unbounded page accepted", err)
	}
	if _, err := ListAssignments(ctx, pool, id, uuid.NewString(), "", 1); !errors.Is(err, ErrFenced) {
		t.Fatal("different session read inventory", err)
	}
	if err := RevokeWorkerCredential(ctx, pool, id.WorkerID, id.CredentialID); err != nil {
		t.Fatal(err)
	}
	if _, err := ListAssignments(ctx, pool, id, registration.SessionID, "", 1); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("revoked credential read inventory", err)
	}
}

func TestAssignmentInventoryOmitsStoppedAndExpiredAuthority(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	for n := range 3 {
		queueAcquisitionJob(t, pool, func(j *spec.Job) { j.Spec.Resources = spec.Resources{CPUMillis: 1, MemoryMiB: 1, ScratchMiB: 1} })
		result, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
		if err != nil || result.Assignment == nil {
			t.Fatal(result, err)
		}
		a := result.Assignment.Authority
		var statement string
		switch n {
		case 0:
			statement = "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1"
		case 1:
			statement = "UPDATE attempts SET phase_deadline=clock_timestamp()-interval '1 second' WHERE id=$1"
		case 2:
			statement = "UPDATE jobs SET cancel_requested=true,state='CANCELLING' WHERE current_attempt_id=$1"
		}
		if _, err := pool.Exec(ctx, statement, a.AttemptID); err != nil {
			t.Fatal(err)
		}
	}
	cursor := ""
	pages := 0
	for {
		page, err := ListAssignments(ctx, pool, id, registration.SessionID, cursor, 1)
		if err != nil || len(page.Assignments) != 0 {
			t.Fatal("non-actionable authority returned", page, err)
		}
		pages++
		cursor = page.NextAfterJobID
		if cursor == "" {
			break
		}
		if pages > 3 {
			t.Fatal("empty pages did not make cursor progress")
		}
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM reservations WHERE state='active'").Scan(&count); err != nil || count != 3 {
		t.Fatal("inventory released uncertain capacity", count, err)
	}
}

func TestAssignmentInventoryBoundsLargePages(t *testing.T) {
	pool, id, registration := readyAcquisitionWorker(t)
	ctx := context.Background()
	for range 4 {
		queueAcquisitionJob(t, pool, func(j *spec.Job) {
			j.Spec.Resources = spec.Resources{CPUMillis: 1, MemoryMiB: 1, ScratchMiB: 1}
			j.Spec.Env = map[string]string{}
			for n := range 100 {
				j.Spec.Env["VALUE_"+strconv.Itoa(n)] = strings.Repeat("v", 8000)
			}
		})
		result, err := AcquireWork(ctx, pool, id, AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}, AcquisitionPolicy{})
		if err != nil || result.Assignment == nil {
			t.Fatal(result, err)
		}
	}
	page, err := ListAssignments(ctx, pool, id, registration.SessionID, "", 64)
	if err != nil || len(page.Assignments) == 0 || len(page.Assignments) >= 4 || page.NextAfterJobID == "" {
		t.Fatal("large inventory did not split", len(page.Assignments), err)
	}
	seen := map[string]bool{}
	for {
		for _, assignment := range page.Assignments {
			if seen[assignment.Authority.JobID] {
				t.Fatal("byte-bounded pages repeated an assignment")
			}
			seen[assignment.Authority.JobID] = true
		}
		if page.NextAfterJobID == "" {
			break
		}
		page, err = ListAssignments(ctx, pool, id, registration.SessionID, page.NextAfterJobID, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 4 {
		t.Fatal("byte-bounded cursor skipped an assignment", len(seen))
	}
}
