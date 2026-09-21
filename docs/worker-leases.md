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

The future supervisor must recheck authority before starting/continuing work and
initiate bounded termination early enough for cleanup. This primitive is not a
running watchdog, renewal loop, reaper, or proof that a frozen host can stop code.
Server-side fencing remains necessary when physical termination cannot be confirmed.

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
client. Production periodic renewal, supervisor integration, reaping, and actual
container stop-at-deadline behavior remain pending.

## Source references

- [Linux clock_gettime and CLOCK_BOOTTIME](https://man7.org/linux/man-pages/man2/clock_gettime.2.html)
- [Rust Instant platform and suspend behavior](https://doc.rust-lang.org/std/time/struct.Instant.html)
- [PostgreSQL explicit row locking](https://www.postgresql.org/docs/current/explicit-locking.html)
- [PostgreSQL clock_timestamp versus transaction time](https://www.postgresql.org/docs/current/functions-datetime.html)
