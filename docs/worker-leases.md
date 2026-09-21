# Worker authority deadlines

## Local deadline primitive (D09a)

`dispatch_worker::lease::AuthorityWindow` turns a server grant into a local deadline:

```text
lease_deadline = request_send_time + remaining_lease - 5 seconds
phase_deadline = request_send_time + remaining_phase
local_deadline = min(lease_deadline, phase_deadline)
```

Receipt time never starts a fresh lease. The constructor rejects a response already
past that deadline; every later `remaining()` check reads the clock again. Zero or
unsupported durations, arithmetic overflow, and observed backwards time fail closed.
The current wire policy permits leases up to 30 seconds and phases up to seven days,
matching the control-plane lease default and maximum execution timeout. One-second
startup/execution/finalization limits remain usable: the five-second margin applies
to lease authority, not to the configured phase duration.

Linux uses `clock_gettime(CLOCK_BOOTTIME)` so suspend time contributes to elapsed
authority. The syscall uses the already-locked libc 0.2.189 dependency and validates
its return code and timespec range. Clock failures are explicit errors, never a
fallback to wall time. Non-Linux builds use a process-relative `Instant` only for
protocol development; actual worker execution remains Linux-only.

Deadlines are in-memory values for the current boot/process. They must not be
serialized into the worker journal or recovered as authority after restart. Recovery
must obtain current grants and apply the same send-time calculation. A valid grant
also does not prove that runtime/spec validation or reconciliation has completed.

The Rust acquisition and assignment-page client now applies this calculation to
each response. Expired responses preserve only attempt identity for reconciliation;
they cannot return a live execution window. The mTLS delayed-grant integration test
checks elapsed RPC time against this boundary using the actual client.

The [live-container watchdog](worker-supervision.md) now consumes these windows,
rechecks them during runtime observation, and attempts bounded termination on loss
of authority. The periodic renewal task below now feeds this channel. Launch checks,
finalization, and reaping still need integration. Server-side fencing remains
necessary when physical termination cannot be confirmed; a frozen host cannot
prove that its workload has stopped.

## Durable batched renewal (D09b)

`store.RenewLeases` accepts 1–64 distinct attempts for one authenticated current
worker session. Every authority includes job, attempt, worker, session, and positive
generation. Invalid identifiers, duplicate attempt IDs, and mixed worker/session
payloads fail before database writes. Results follow input order and contain one
decision per attempt:

| Decision | Meaning |
| --- | --- |
| `ACCEPTED` | Current active attempt, unexpired lease and phase, no cancellation |
| `FENCED` | Unknown/mismatched authority, expired lease, or changed job ownership |
| `STOP_REQUESTED` | Cancellation requested or current phase deadline reached |
| `ALREADY_TERMINAL` | Matching attempt already has a terminal outcome |

Only accepted members extend their lease to fresh database wall time plus 30 seconds.
Renewal does not change phase deadlines, job/attempt state, reservations, or worker
readiness. Draining blocks new acquisition but permits valid existing work to renew.
Rejected members carry no lease or phase authority. A fenced session or revoked
credential rejects the whole batch, including retries.

Migration `0007_worker_lease_requests` records the original grant atomically with
lease extensions, keyed by worker/session/request UUID. The payload digest includes
an operation tag and the sorted authority set; reordering a retry is allowed, but
changing any authority conflicts. Concurrent retries serialize on that identity
even when every requested attempt is unknown. A failed transaction returns no grants
and leaves neither partial renewals nor replay history.

A retry rechecks current authority and returns at most the original grant's lease
and phase bounds. It never updates leases and cannot borrow time from a later batch
or phase transition. Original rejected members stay rejected. Missing/malformed
stored results fail closed. Each new periodic renewal must use a new request UUID;
transport retries reuse the original UUID and authority set.

The transaction locks the credential, its replay identity, then jobs by UUID and
attempts by job/attempt UUID. It does not acquire the scheduler's cluster lock or a
worker row lock. Session recovery follows the same ownership lock order, so the
post-lock session recheck remains stable for any live authority the batch can grant.
Unknown/mismatched pairs cannot lock attempts outside the requested job set.
`clock_timestamp()` is sampled after ownership locks and session revalidation;
transaction-start time cannot revive a lease that expired while blocked. Local SQL
lock and statement timeouts remain two and four seconds, respectively. No external
I/O occurs inside this transaction.

History currently persists for the session lifetime without automatic retention.
Do not prune records while their UUIDs can still renew active work: deleting one
would let an old retry look like a new operation. Safe bounded retention remains a
release requirement. The production renewal loop, supervisor, and lease
reaper remain separate work; this store slice alone does not keep containers alive
or terminate them at expiry.

## Authenticated renewal RPC (D09c)

The Go worker service implements the existing `RenewLeases` RPC through the store
transaction above. The shared mTLS boundary verifies the certificate, live database
credential, and enclosing/nested worker and session identities. The adapter rejects
empty/oversized batches and unsigned generations above PostgreSQL's signed bigint
range before conversion. Store validation checks UUIDs, positive generations, and
duplicate attempts. Existing five-second server deadlines and four-MiB message
limits apply.

Each response retains the authority tuple, decision, database timestamp, and
remaining lease/phase durations. Durations are floored to whole milliseconds before
unsigned conversion. An expired or submillisecond lease becomes `FENCED`; a
submillisecond phase becomes `STOP_REQUESTED`. Rejections expose zero durations.
Unknown decisions, invalid generations, or grants beyond the thirty-second policy
fail closed. The client must still account for request elapsed time and the local
safety margin; wire durations do not authorize a fresh full lease upon receipt.

Malformed input maps to `InvalidArgument`, conflicting replay payloads to
`AlreadyExists`, and fenced sessions to `FailedPrecondition / SESSION_FENCED`.
Certificate/credential failures map to `Unauthenticated`; mismatched claimed worker
or nested session identity maps to `PermissionDenied`. Per-attempt expiry and
cancellation are result decisions rather than transport errors.

## Rust renewal client (D09d)

`ControlClient::renew(&RenewLeasesRequest)` uses the same configured mTLS channel,
four-MiB message bound, and five-second transport/outer timeout as acquisition.
Before sending, it validates 1–64 unique attempt IDs, canonical nonzero UUIDs,
signed-range positive generations, and agreement with the enclosing worker/session.
The caller retains the request UUID and exact batch for transport retries; this
method does not retry automatically or schedule the next renewal period.

The response must contain exactly one result for each requested authority in input
order. Every full tuple must match. Missing/duplicate/substituted entries, unknown
decisions, out-of-policy durations, and rejection entries carrying durations reject
the entire response before any authority is returned. A valid response yields:

- `Renewed`: immutable attempt identity plus a conservative `AuthorityWindow`.
- `Rejected`: identity plus `Fenced`, `StopRequested`, or `AlreadyTerminal`.
- `Expired`: identity only when a grant has already exhausted local authority.

Each accepted result uses the request-send monotonic sample, the returned durations,
and the existing five-second lease margin. The client checks authority again after
validating the full batch. Server wall-clock timestamps are informational and do
not replace the local monotonic calculation. Consumers must recheck the returned
window before acting and associate it with the exact attempt identity. The API
does not mutate an older assignment grant, persist a deadline, or supervise Docker.

## Verification

Deterministic tests cover delayed receipt, exact expiry, large forward clock advances,
one-second phase deadlines, backwards samples, invalid limits, and overflow. These
tests simulate pauses through clock samples; they do not suspend the user's host.
An OS-clock test exercises the actual clock implementation for the test platform.

`make test lint smoke` verifies the normal workspace. `sh scripts/test-worker-clock.sh`
additionally verifies the production Linux branch. On Linux it uses the pinned native
toolchain; elsewhere it compiles the same module in the official Rust 1.88.0 slim
Bookworm image pinned by manifest digest. The container has no network, a read-only
root filesystem, read-only source/dependency mounts, and an ignored `.local/linux-clock`
build directory. `make integration` includes this check. The observed Linux run was
arm64 on Docker Desktop; independent-host execution and actual pause/termination
fault tests remain release work.

The race-enabled real PostgreSQL suite (`sh scripts/test-store.sh`) covers concurrent
identical requests, overlapping batches in opposite input orders, mixed decisions,
changed payload conflicts, expiry/cancellation/phase rejection, revoked credentials,
unknown authority batches, corrupt replay records, and fresh requests versus retries.
An injected history-write failure proves that both members of a renewal roll back.
Observed PostgreSQL lock waits exercise expiry and approved takeover before renewal
can acquire its ownership locks. A separate test holds the scheduler advisory lock
and worker row lock while a draining worker renews successfully. A saved-grant fixture
moves the original timestamps into the past to verify that a live newer lease cannot
revive an old request, without a thirty-second test sleep.

`sh scripts/test-schema.sh` passed fresh apply, complete rollback, and reapply with
the seventh migration. `make test lint smoke` passed after enabling local sockets
needed by existing HTTP and runtime fixture tests.

The real mTLS service integration test additionally renews an acquired attempt,
restarts the Go gRPC service, retries the same renewal identity, and verifies the
durable expiry did not change. It checks oversized/empty/duplicate batches,
generation overflow, cross-worker/session claims, changed payload conflicts,
expired leases, session takeover, and credential revocation on an established
connection. Unit tests cover signed duration flooring and invalid store outcomes.
The full race-enabled PostgreSQL/mTLS suite and `make test lint smoke` passed after
the RPC implementation.

Rust unit tests cover malformed requests/responses, reordered/substituted authority,
duplicate/oversized batches, mixed decisions, zero/out-of-policy durations, and a
grant exhausted by the local safety margin. `work_probe` now renews and retries each
real acquired attempt through the Go executable. PostgreSQL assertions verify one
batch record per request and exact agreement between original stored grants and
current expiries after retries. The mTLS delayed-grant fixture separately delays
acquisition and renewal responses: 5,100 ms of reported lease minus the five-second
margin leaves 100 ms, but delivery takes at least 200 ms. Both return no usable
execution authority in the real Rust client. The fixture never launches containers.

The race-enabled PostgreSQL/mTLS suite and `make test lint smoke` passed with this
client. Subsequent watchdog and renewal-task evidence is described below and in
`worker-supervision.md`; production launch integration and reaping remain pending.

## Periodic renewal task (D09f)

`ControlClient::maintain_leases` owns a fixed batch of 1–64 authority controllers
from one worker/session. Call it in an independent task using a cloned control
client; clones share the authenticated Tonic channel but have independent RPC
futures. Do not serialize heartbeat, runtime observation, or filesystem work behind
this loop. The future worker coordinator must group acquisitions into bounded
batches and retain the watchdog consumers until their execution/finalization ends.

The first renewal is immediate. After an accepted response, the next period starts
five seconds after that RPC began, accounting for its elapsed time. Connection and
deadline failures retry after one second. Each RPC has a five-second outer bound.
Retry retains the exact request UUID, member order, and full authority tuples;
even a member that finishes during an uncertain request remains in that retry.
After the response resolves, inactive members are omitted from the next fresh UUID.
The existing client validates all response members before returning typed grants.

Only validated grants update watchdog authority. Transient failures, sleeps, and
timeouts do not extend any window. Rejections are sticky and late grants cannot
revive a locally expired execution. Nonretryable RPC/protocol/clock errors stop
the batch with `RenewalFailed` and return the error. Once all consumers have closed
or their authority has ended, the loop returns; checking this condition may wait
for the current bounded RPC or sleep. Physical termination stays in the independently
polled watchdog and does not wait for this loop to finish.

A drop guard stops retained controllers with `ControllerLost` when a renewal
future is cancelled or unwinds, including cancellation before its first poll.
This also works when the launch coordinator retains cloned senders; merely dropping
the task's own clones would not close those channels.
An already recorded rejection/expiry is preserved. The guard cannot run after an
OS process kill or abort; server fencing and startup reconciliation remain required.
The batch API does not add members while running. Replacing a live task cancels its
authority, so coordinators must not restart it to change membership. New assignments
need their own batch; cross-batch scheduling and launch integration remain open.

Unit tests exercise stable retries across changed membership, the 64-member limit,
duplicate and cross-session rejection, expired authority during outages, stalled
RPC deadlines, malformed/fatal responses, and cancellation with retained senders.
The `renewal_probe` subprocess obtains two real assignment grants over mTLS and
runs the task. Its PostgreSQL fixture commits the first batch but loses the reply,
checks exact retry, observes a new UUID after the five-second period, then expires
database authority and verifies the loop exits on fencing. Three RPCs create exactly
two durable batch records. The fixture seeds readiness and never starts containers;
it does not prove the full agent launch, partition, or active-job restart gates.

## Explicit phase authority refresh (D09g)

`SupervisedAuthority::refresh` requests a grant from a renewal operation constructed
after the call starts. Invoke it after an accepted phase report before relying on
the new phase's server deadline. Phase acknowledgements carry no lease timing;
the refresh barrier does not create authority or extend the previously held window.

Exactly one `maintain_leases` producer must own each attempt's channel. A refresh
wakes that producer before its normal five-second period. If an older operation has
an uncertain reply, the producer first resolves its exact UUID/payload retry, then
constructs a fresh operation. Each pending batch records the refresh revisions that
preceded its construction. Only a validated, applied live grant acknowledges those
revisions; an older replay cannot satisfy a later request. Already-satisfied wake
permits are consumed without issuing surplus RPCs.

Refresh continues checking the existing local deadline and sticky stop state while
waiting. Fencing, expiry, or loss of the producer ends it with a stop reason. Cancelling
the refresh waiter can leave a pending request for the producer to satisfy; it does
not cancel the producer or revoke an otherwise live grant. The parent must keep
runtime supervision independent while awaiting the barrier. In particular, reporting
a shorter RUNNING phase requires a conservative local phase bound while the report
and fresh grant remain uncertain; that execution coordinator is still pending.

Tests cover waking the normal period without an extra RPC, exact replay before a
post-request grant, expiry without a producer, and rejection during refresh. The real
`launch_probe` now requests refresh after RUNNING. Its service fixture fences that
renewal, and the probe must reject the barrier before the watchdog confirms the
actual Docker container stopped. This is negative post-phase integration evidence;
successful finalization and production acquisition remain separate work.

## Source references

- [Linux clock_gettime and CLOCK_BOOTTIME](https://man7.org/linux/man-pages/man2/clock_gettime.2.html)
- [Rust Instant platform and suspend behavior](https://doc.rust-lang.org/std/time/struct.Instant.html)
- [PostgreSQL explicit row locking](https://www.postgresql.org/docs/current/explicit-locking.html)
- [PostgreSQL clock_timestamp versus transaction time](https://www.postgresql.org/docs/current/functions-datetime.html)
