//go:build integration

package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"dispatch.local/dispatch/internal/spec"
	"github.com/google/uuid"
)

func TestPriorityAgingExactThresholds(t *testing.T) {
	pool, _, _ := readyAcquisitionWorker(t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	query := "SELECT " + effectivePrioritySQL + ` FROM (VALUES($1::smallint,$2::timestamptz)) j(priority,next_eligible_at)
		CROSS JOIN (VALUES($3::timestamptz)) queue_clock(now)`
	for base := range 4 {
		for _, age := range []time.Duration{-time.Hour, 0, 10*time.Minute - time.Microsecond, 10 * time.Minute,
			20*time.Minute - time.Microsecond, 20 * time.Minute, 30*time.Minute - time.Microsecond, 30 * time.Minute, 100 * time.Hour} {
			t.Run(fmt.Sprintf("base%d-age%s", base, age), func(t *testing.T) {
				want := min(3, base+max(0, int(age/(10*time.Minute))))
				var got int
				if err := pool.QueryRow(context.Background(), query, base, now.Add(-age), now).Scan(&got); err != nil || got != want {
					t.Fatal("promotion threshold", got, want, err)
				}
			})
		}
	}
	// Extreme finite timestamps catch overflow before clamping, without waiting
	// for wall time to pass or duplicating PostgreSQL's timestamp arithmetic.
	for _, tt := range []struct {
		eligible string
		want     int
	}{{"4713-01-01 BC", 3}, {"280000-01-01 AD", 0}} {
		var got int
		if err := pool.QueryRow(context.Background(), query, 0, tt.eligible, now).Scan(&got); err != nil || got != tt.want {
			t.Fatal("extreme timestamp", tt.eligible, got, tt.want, err)
		}
	}
}

func TestPriorityAgingResetsAfterRetryTransitions(t *testing.T) {
	for _, path := range []string{"completion", "lease loss"} {
		t.Run(path, func(t *testing.T) {
			pool, worker, registration := readyAcquisitionWorker(t)
			ctx := context.Background()
			job := queueAcquisitionJob(t, pool, nil)
			var created time.Time
			if err := pool.QueryRow(ctx, `UPDATE jobs SET created_at=clock_timestamp()-interval '2 hours',
				next_eligible_at=clock_timestamp()-interval '31 minutes' WHERE id=$1 RETURNING created_at`, job.ID).Scan(&created); err != nil {
				t.Fatal(err)
			}
			request := AcquisitionRequest{SessionID: registration.SessionID, RequestID: uuid.NewString()}
			result, err := AcquireWork(ctx, pool, worker, request, AcquisitionPolicy{})
			if err != nil || result.Assignment == nil {
				t.Fatal(result, err)
			}
			queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Spec.Priority = 3 })
			if replay, err := AcquireWork(ctx, pool, worker, request, AcquisitionPolicy{}); err != nil || replay.Assignment == nil || replay.Assignment.Authority != result.Assignment.Authority || replay.Assignment.SpecHash != job.SpecHash {
				t.Fatal("aged grant replay changed identity", replay, err)
			}
			var before time.Time
			if err := pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&before); err != nil {
				t.Fatal(err)
			}
			if path == "completion" {
				completion := CompletionRequest{Authority: result.Assignment.Authority, CompletionID: uuid.NewString(), Reason: "RUNTIME_UNAVAILABLE", Stopped: true}
				signCompletion(t, &completion)
				if done, err := CompleteAttempt(ctx, pool, worker, completion); err != nil || done.State != "FAILED" {
					t.Fatal("retryable completion", done, err)
				}
			} else {
				// Expire only this grant to exercise the real reaper without sleeping
				// or claiming that a native worker/container was killed here.
				if _, err := pool.Exec(ctx, "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", result.Assignment.Authority.AttemptID); err != nil {
					t.Fatal(err)
				}
				if count, err := ReapExpiredAttempts(ctx, pool, 64); err != nil || count != 1 {
					t.Fatal("lease-loss retry", count, err)
				}
			}
			var state string
			var effective, base int
			var eligible time.Time
			var unchanged bool
			query := "SELECT j.state,j.next_eligible_at,j.priority," + effectivePrioritySQL + `,
				j.created_at=$3 AND j.spec=$4::jsonb AND j.spec_hash=$5 FROM jobs j
				CROSS JOIN (VALUES($2::timestamptz+interval '6 seconds')) queue_clock(now) WHERE j.id=$1`
			if err := pool.QueryRow(ctx, query, job.ID, before, created, job.Spec, job.SpecHash).Scan(&state, &eligible, &base, &effective, &unchanged); err != nil || state != "RETRY_WAIT" || !eligible.After(before) || effective != 0 || base != 0 || !unchanged {
				t.Fatal("retry retained promotion age or changed identity", state, eligible, base, effective, unchanged, err)
			}
		})
	}
}

func TestPriorityAgingOrdersEligibleJobs(t *testing.T) {
	for _, tt := range []struct {
		name                   string
		base, competing        int
		eligibleAge            time.Duration
		retry, oversized, wins bool
	}{
		{"fresh priority wins", 0, 3, 0, false, false, false},
		{"first promotion", 0, 1, 11 * time.Minute, false, false, true},
		{"second promotion", 0, 2, 21 * time.Minute, false, false, true},
		{"third promotion", 0, 3, 31 * time.Minute, false, false, true},
		{"higher base reaches cap sooner", 2, 3, 11 * time.Minute, false, false, true},
		{"retry age resets", 0, 3, 0, true, false, false},
		{"future retry remains blocked", 3, 0, -time.Hour, true, false, false},
		{"aged oversized job permits backfill", 0, 3, 31 * time.Minute, false, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pool, worker, registration := readyAcquisitionWorker(t)
			ctx := context.Background()
			if tt.oversized {
				if _, err := pool.Exec(ctx, "UPDATE projects SET cpu_quota=8000 WHERE name='research'"); err != nil {
					t.Fatal(err)
				}
			}
			old := queueAcquisitionJob(t, pool, func(job *spec.Job) {
				job.Spec.Priority = tt.base
				if tt.oversized {
					job.Spec.Resources.CPUMillis = 6000
				}
			})
			state := "QUEUED"
			if tt.retry {
				state = "RETRY_WAIT"
			}
			// Fixed historical timestamps model waiting without sleeping or aging
			// the worker heartbeat. Retry creation time must not count as queue age.
			if _, err := pool.Exec(ctx, `UPDATE jobs SET created_at=clock_timestamp()-interval '2 hours',
				next_eligible_at=clock_timestamp()-$2::bigint*interval '1 microsecond',state=$3 WHERE id=$1`,
				old.ID, tt.eligibleAge.Microseconds(), state); err != nil {
				t.Fatal(err)
			}
			fresh := queueAcquisitionJob(t, pool, func(job *spec.Job) { job.Spec.Priority = tt.competing })
			want := fresh.ID
			if tt.wins {
				want = old.ID
			}
			acquireSchedulerJob(t, pool, worker, registration.SessionID, want)
			var base int
			var hash string
			var unchanged bool
			if err := pool.QueryRow(ctx, "SELECT priority,spec_hash,spec=$2::jsonb FROM jobs WHERE id=$1", old.ID, old.Spec).Scan(&base, &hash, &unchanged); err != nil || base != tt.base || hash != old.SpecHash || !unchanged {
				t.Fatal("aging mutated frozen job identity or base priority", base, hash, unchanged, err)
			}
		})
	}
}
