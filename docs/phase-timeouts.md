# Phase timeout recovery

The server reaper checks the earlier of each current attempt's lease expiry and
phase deadline. It runs before listeners open and every two seconds while serving.
Each call inspects at most 64 candidates, with one transaction per attempt.
Migration `0020_active_deadlines` indexes the earlier deadline for active attempts;
rolling it back removes only that index.

After locking the job and attempt, the reaper rereads both deadlines and fresh
database time. A lease renewal or forward phase transition that committed while
the reaper waited can therefore prevent fencing from a stale candidate scan.

| Earliest expired authority | Attempt reason | Attempt state | Retry |
| --- | --- | --- | --- |
| Startup in ASSIGNED or STARTING | `STARTUP_TIMEOUT` | FAILED | No |
| Execution in RUNNING | `EXECUTION_TIMEOUT` | FAILED | No |
| Finalization in FINALIZING | `FINALIZATION_TIMEOUT` | FAILED | No |
| Lease before phase deadline | `WORKER_LOST` | LOST | Existing opt-in policy |
| Cancellation already recorded | `USER_CANCELLED` | CANCELLED | No |

Equal lease and phase deadlines count as a phase timeout. A late reaper preserves
the deadline ordering rather than turning a known timeout into retryable worker
loss. Session takeover uses the same classification. The current job schema does
not permit timeout retry reasons.

The transaction clears authoritative ownership, quarantines the reservation,
marks the host unreconciled, and appends one `ATTEMPT_TIMED_OUT` event for timeout
failures. The event payload includes the reason, next job state, cleanup status,
and eligibility timestamp. A permanent sweep-child failure applies the existing
fail-fast policy in that same transaction. Repeated reaping is a no-op; event
failure rolls back the entire transition. Late success cannot publish a result.

Expiry is not evidence of a physically stopped container. Capacity remains
quarantined with `cleanup_pending` until a replacement incarnation proves cleanup.
Durable classification and fencing are verified on real PostgreSQL, including
concurrent deadline changes and rollback/reapply.

`TestWorkerExecutionTimeoutStopsAndReconcilesItsOwnJob` also runs the actual worker
with mTLS and Docker. A 120-second sleep receives a five-second execution budget
with worker-loss retries enabled. The test observes the container running, then
stopped, and the server's nonretryable `EXECUTION_TIMEOUT` outcome with quarantined
capacity. An explicitly approved replacement incarnation removes the container
and workspace before readiness, clears cleanup pending, and releases capacity.
It verifies one attempt and no accepted completion. The timeout gate passed in
6.53 seconds; the existing active-job crash/restart gate also passed.

This is one local Docker Desktop engine with development scratch. Real startup
and finalization stalls, interrupted cleanup, independent Linux hosts, and strict
scratch still require their own release evidence.
