# Worker sessions and recovery

## Registration (D06c)

`RegisterSession` binds a provisioned host credential to a requested session UUID.
The request UUID, protocol version, resource claims, slots, labels, and capability
set are bounded and validated. Resource claims may be lower than the operator's
ceilings; labels must exactly match the provisioned host labels. This prevents a
worker from joining another placement group through registration.

Protocol version 1 requires `docker.v1`, `cpu.hard`, `memory.hard`, `pids.hard`, and
exactly one of `scratch.soft` or `scratch.quota`. These are advertised capabilities,
not evidence that containment works; the Rust runtime must detect/enforce them and
the scheduler must honor the deployment's required scratch profile. Linux amd64
and arm64 are the supported architectures. Unsupported capabilities fail closed.

The canonical registration hash excludes the request ID and sorts capabilities.
The same request returns the existing incarnation. Another request ID can recover
the same incarnation only with identical claims. Reusing a request or session with
changed claims conflicts. Registration never refreshes heartbeat liveness on replay.
An old fenced session can never become current through replay.

New sessions start at generation 1, with monotonically increasing generations for
subsequent incarnations. Session identity, creation time, and registration hash are
immutable; a recorded fencing timestamp cannot be removed or changed. The worker
starts in REGISTERING with reconciliation incomplete. Registration alone does not
make the host eligible to execute work. Concurrent live incarnations fail with
`ErrSessionActive` unless an operator approved the specific replacement below.

## Recovery and takeover (D06d)

A different incarnation may automatically replace the current session when its
last heartbeat/registration is at least 30 seconds old and every remaining active
attempt lease has expired. These conditions are rechecked using database wall time
after acquiring the affected job, attempt, and worker locks. A recently registered
but still reconciling worker must therefore keep heartbeating during cleanup.

For a live session, the operator-only `ApproveSessionTakeover` store operation binds
approval to `(worker, old session, requested replacement session)`. A different
replacement cannot use it. Repeating the same approval is idempotent; changing its
target conflicts. Approval does not itself fence the old process. The replacement's
successful registration consumes it while changing authority atomically.

Recovery permanently fences old sessions, terminalizes their authoritative attempts,
clears job ownership, quarantines physical reservations, and records events. Lost
attempts use `WORKER_LOST`: retry only when that reason is allowed and attempts remain.
Backoff doubles to the configured cap with equal jitter in `[base/2,base]`; jitter is
derived from the immutable attempt ID, so transaction retries do not redraw delays.
Cancellation intent instead produces CANCELLED with no retry. Exhausted or excluded
retry policies produce FAILED. All uncertain executions keep `cleanup_pending`.

The replacement starts REGISTERING and must stop/remove old executions before its
heartbeat can release quarantined resources. Successful registration is never proof
of physical cleanup. Old registration requests remain fenced, and retries of the new
request cannot manufacture another session or retry event.

## Heartbeat readiness and reconciliation (D06e)

Each report carries a positive, increasing sequence within the session, a request
UUID, health flags, and at most 1,024 inventory entries. Container IDs are full
64-character lowercase hex IDs. The canonical hash sorts inventory by container
ID, so enumeration order does not change report identity. The database retains only
the latest sequence/request/hash and enforces monotonic updates.

An exact latest-report retry returns current instructions without refreshing
liveness, reapplying cleanup, or overwriting health. An older sequence is stale; a
changed request/payload at the same sequence conflicts. Heartbeats recheck both the
credential and current unfenced session inside the ownership transaction.

The server returns stop instructions for unknown, mismatched, old, expired, or
cancel-requested executions. Missing inventory never implies job completion and
never releases active reservations or renews a lease. A missing RUNNING execution
requests reconciliation; ASSIGNED work may legitimately lack a container before
delivery/startup. Server reservations remain authoritative.

Only a newer incarnation can clear older **fenced, terminal** quarantined attempts,
after a healthy report states reconciliation complete and contains no execution
requiring stop. It releases the reservations and clears `cleanup_pending` atomically
with the report. A session's own uncertain reservation cannot be freed through a
possibly stale snapshot; it requires fencing and a new incarnation. The lease-loss
path must enforce that recovery protocol when it is implemented.

Healthy, reconciled workers become READY, or DRAINING when the operator requested
drain. Incomplete reconciliation remains REGISTERING. Runtime failure or disk
pressure makes the worker QUARANTINED and blocks acquisition. State changes create
audit events; ordinary heartbeat ticks do not. These are authenticated agent reports,
not direct observations of Docker: the Rust worker must perform the actual cleanup.

## Transaction ordering

Reservation-changing paths start with transaction advisory lock `1146310734`,
distinct from migration lock `1146310733`. After that, lock affected jobs by sorted
ID, attempts, then worker/accounting rows. Registration rechecks the credential under
a shared row lock to serialize with revocation. Revocation takes only its credential
lock and never subsequently asks for the cluster lock. Lease renewal must lock its
job/attempt without subsequently taking the cluster lock.

Transactions use a two-second lock timeout and four-second statement timeout;
RPC calls have the transport's five-second deadline. No registry, runtime, object
store, or remote RPC belongs inside a database ownership transaction.

## gRPC service (D06f)

`workerapi.NewService` implements `RegisterWorker` and `Heartbeat` using the store
transitions above. `workerapi.NewServer` supplies mTLS identity and bounded RPCs.
The handlers also require that identity when invoked directly; request fields alone
cannot authorize an operation. Memory and scratch claims must be exact whole MiB,
and unsigned sequence/generation values must fit PostgreSQL's positive signed range.
Unsupported protocol/capability/resource values return `INVALID_ARGUMENT`.

Store failures map to stable gRPC status/reason pairs without exposing driver text:

| gRPC status | Reason | Meaning |
| --- | --- | --- |
| InvalidArgument | INVALID_ARGUMENT | Invalid or unsupported request/claim |
| AlreadyExists | REQUEST_CONFLICT | Replay identity reused with changed content |
| FailedPrecondition | SESSION_ACTIVE | A live incarnation needs explicit takeover |
| FailedPrecondition | SESSION_FENCED | Stop using this old incarnation |
| Aborted | STALE_HEARTBEAT | A newer report was already accepted |
| Unauthenticated | UNAUTHORIZED_WORKER | Credential missing/revoked inside the operation |
| Unavailable | DATABASE_UNAVAILABLE | No definitive mutation result; retry the same identity |

Cancellation and deadline errors retain their corresponding gRPC status. Transport
identity mismatch remains PermissionDenied. Registration retries return current
cleanup requirements without manufacturing another session or refreshing liveness.
`AcquireWork` is implemented as described in [durable acquisition](acquisition.md).
Other worker methods still return Unimplemented until their execution slices land.

The service is now wired into `dispatch-server serve --worker-listen ...` with
explicit mTLS configuration. Operator enrollment, revocation, and takeover commands
are documented in [running the control plane](running.md).

## Rust control-plane client (D06i)

`dispatch_worker::control::ControlClient` implements registration, heartbeat,
[acquisition, and paginated assignment recovery](acquisition.md) over
Tonic mTLS. Connection configuration accepts an HTTPS origin, an explicit server CA,
and a client certificate/key. Credentials in URLs, paths, queries, fragments, and
plaintext endpoints are rejected. No system-root fallback or TLS key logging is
enabled. TLS APIs were checked against the pinned Tonic 0.14.6 source.

Connection establishment (including TLS) and each RPC have a five-second local
timeout. RPCs also carry the server deadline; encoded and decoded messages are capped
at 4 MiB. Registration responses must preserve the requested worker/session and
supported protocol with a positive signed-range generation. Cleanup instructions
are bounded and must belong to the same worker; previous sessions are permitted for
reconciliation. Actual container label/authority checks remain the runtime's job.

The caller retains request payloads, session IDs, and heartbeat sequences across
retries. The [worker journal](worker-journal.md) now persists a fresh registration
request per agent incarnation and records matching acknowledgements. Reopening
exposes history but requires a new incarnation; it does not resume an old session
or restore readiness. The client never generates a replacement identity. Connection failures,
local deadlines, Unavailable, and DeadlineExceeded are retryable categories; fencing,
revocation, conflicting payloads, and stale reports require explicit handling.
There is no automatic retry loop yet. Transport errors omit configuration material,
and RPC diagnostic text is escaped for terminal output.

`rust/worker/examples/session_probe.rs` is a protocol integration fixture. It reads
a bounded protobuf registration from stdin, registers twice, sends the same heartbeat
twice, and writes the registration reply as protobuf. It always reports an unhealthy,
unreconciled runtime, so it cannot make a host schedulable or release old reservations.
`make store-test` builds all worker protocol fixtures before integration tests. The production
agent still needs to integrate durable session state and runtime discovery with
registration, reconciliation, and its supervision loop before it can advertise
readiness or execute jobs.

## Verification

Real PostgreSQL tests cover 32 identical concurrent registrations with one session
and one request record, stable replay with a new request ID, changed-claim conflict,
two racing incarnations with one winner, claimed capacity/label/protocol rejection,
revocation rechecked inside the transaction, and rollback after an injected audit
failure. Recovery tests additionally cover live-lease refusal, expired-session
replacement, exact-target approval, cancellation/retry/exhaustion, irreversible
fencing, and rollback of ownership/approval after an injected job-event failure.
A regression test observes registration blocked on a real job lock, ends the lease
after that transaction began, and verifies recovery uses fresh time after the wait.
Unit tests verify stable, capped, distributed retry jitter. Heartbeat tests cover
readiness, health/disk-pressure/drain behavior, duplicate/stale/changed reports,
old-session rejection, cleanup gating, unchanged leases/active reservations,
same-session quarantine protection, bounded inventory, canonical ordering, and
rollback of both cleanup and sequence after an injected state-audit failure.
All session migrations passed fresh apply, rollback, and reapply. A real mTLS gRPC
test now covers registration, heartbeat readiness, malformed byte/sequence bounds,
server restart with durable session recovery, live-session conflict, approved
takeover, and old-session rejection. Command-level integration also covers actual
HTTP/worker listeners, restart, and live revocation. Physical runtime cleanup remains
required.
