# Sweeps

A sweep expands a container job template across a parameter matrix. Matrix keys
are sorted, values retain their submitted order, and each child receives a stable
index. Admission creates the sweep, children, frozen dataset bindings, submission
events, and idempotency claim in one transaction. The limit is 1,000 children.

`POST /v1/sweeps` submits an embedded job template through the authenticated HTTP
API. Admission freezes its resolved image and project-owned datasets before
expansion. Identical idempotency-key retries return the same children even when
the image registry is unavailable. See [HTTP admission](http-api.md#sweep-admission).
CLI sweep commands, progress, and result export remain under development.

## Scheduling and failure policy

`maxConcurrent` counts child attempts from ASSIGNED through FINALIZING. The
scheduler checks it before reserving capacity. A blocked sweep does not prevent an
unrelated runnable job from acquiring work; a terminal attempt frees its sweep slot.
Quarantined host capacity still requires reconciliation before reuse.

`failFast: true` cancels QUEUED and RETRY_WAIT siblings when a child becomes
permanently FAILED. A failed attempt that schedules another retry does not trigger
the policy. Failure handling covers completion, lease expiry, and session recovery.
Sibling cancellations and the triggering failure commit together, preventing
acquisition of another queued child between those changes.

Running siblings continue by default. With `cancelRunningOnFailure: true`, ACTIVE
siblings become CANCELLING and receive STOP_REQUESTED on lease renewal. Their
reservations remain active until physical stop is acknowledged, or quarantined
after fencing. Already terminal or cancelling children retain their outcomes.

Each affected sibling records one `SWEEP_FAIL_FAST` event containing the sweep ID,
failed job ID, and previous/new states. Accepted completion replay does not repeat
these events. Failure paths lock all affected jobs in UUID order before attempts
so independent batch renewals cannot invert the lock order.

## Verification

Race-enabled PostgreSQL tests cover concurrent submission replay, ordered children,
quota rollback, sweep capacity/backfill and terminal release, both failure policies,
retry preservation, reaping, session recovery, completion replay, sibling event
rollback, stop acknowledgement, and observed job-lock ordering. These tests verify
store behavior; they do not establish a multi-host sweep or public CLI workflow.
