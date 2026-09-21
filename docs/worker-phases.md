# Attempt phase reporting

## Store contract (D08c)

`store.ReportPhase` acknowledges execution progress for an authenticated current
worker session and the full job/attempt/generation/worker/session tuple. The database
stores the container binding, exit status, phase deadline, replay identity, and job
event atomically. A phase report does not renew the lease, release reservations,
or accept a successful result.

| Current phase | Requested phase | Required evidence | Deadline |
| --- | --- | --- | --- |
| `ASSIGNED` | `STARTING` | No container or exit status yet | Preserve original startup deadline |
| `STARTING` | `RUNNING` | Full 64-character lowercase hex container ID; no exit status | Fresh database time plus execution timeout |
| `STARTING` or `RUNNING` | `FINALIZING` | Same bound container, or first binding for a fast exit; exit status 0–255 | Fresh database time plus finalization timeout |
| Same phase | Same phase | Matching evidence | Preserve deadline |

An immediately exiting process may go from `STARTING` to `FINALIZING` when runtime
inspection first sees it already stopped. Skipping startup acknowledgement and fresh
backward transitions are conflicts. Nonzero process exit still enters finalization
so logs and failure evidence can be collected; completion later decides the terminal
outcome. The job remains `ACTIVE` and physical reservations remain `active` throughout.

The startup timeout begins at assignment, not at acknowledgement. Lease and phase
expiry are checked using `clock_timestamp()` after ownership locks, parsing, and any
replay-identity wait. Expired leases return `FENCED`; expired phases or cancellation
return `STOP_REQUESTED`. Matching terminal attempts return `ALREADY_TERMINAL` with
their current state. Unknown/mismatched tuples return `FENCED` without disclosing a
state. Revoked credentials and replaced sessions reject the call before mutation.

## Replay and persistence

Migration `0008_attempt_phases` adds `attempts.container_id` and a database trigger
that prevents rebinding or clearing a known container ID. It also adds
`attempt_phase_reports`, keyed by worker/session/event UUID. The digest binds the
operation, complete authority tuple, requested phase, container ID, and optional
exit status. Accepted reports retain their replay identity; rejected reports roll
back any tentative claim. No automatic history retention exists yet.

Retry the exact event UUID and payload after an ambiguous response. Reusing an
accepted identity with changed payload conflicts. Concurrent equal reports create
one transition and one `PHASE_CHANGED` job event. A retry after later progress
returns the current state without regressing it or resetting any timeout, but it
still rechecks session, lease, phase, and cancellation. A fresh event UUID for the
same phase is accepted only when its runtime evidence matches; it records the
operation without creating another transition event or extending the deadline.

The transaction locks the credential, then the job and attempt, then claims the
event identity. The history foreign keys must not lock an attempt before its job.
This path does not take the scheduler cluster lock or a worker row lock, because it
does not change resource accounting. Session recovery must continue to lock live
jobs/attempts before replacing their session. SQL lock and statement timeouts are
two and four seconds. Event insertion failure rolls back state, evidence, deadlines,
sequence increments, and replay history together.

## Authenticated RPC (D08d)

The Go service exposes this contract through the existing `ReportPhase` RPC and
shared mTLS boundary. Only `STARTING`, `RUNNING`, and `FINALIZING` are reportable;
terminal outcome publication uses the separate completion API. The adapter rejects
generation values beyond signed bigint range before conversion and preserves the
optional exit-code pointer, including explicit zero versus missing exit evidence.
The store validates identifiers, evidence, ownership, replay, and current deadlines.

Malformed inputs map to `InvalidArgument`, changed payloads or fresh backward reports to
`AlreadyExists / REQUEST_CONFLICT`, replaced sessions to
`FailedPrecondition / SESSION_FENCED`, and revoked credentials to `Unauthenticated`.
A mismatched claimed host is `PermissionDenied`. Per-attempt fencing, cancellation,
and terminal state are explicit decision/state pairs. The response mapper rejects
unknown or inconsistent pairs; only `FENCED` for an unknown tuple may omit state.
The shared five-second server deadline and message bounds apply.

## Rust phase client (D08e)

`ControlClient::report_phase(&ReportPhaseRequest)` validates canonical authority and
event UUIDs, generation bounds, allowed phases, full container IDs, and optional exit
evidence before sending. It uses the existing configured mTLS channel and five-second
wire/outer timeout. The caller supplies stable event IDs; the method does not retry
or allocate replacement identities automatically.

The typed `PhaseStatus` contains a decision and current state, without an execution
window. Accepted replies may name the requested phase or a later active phase, but
cannot regress progress or invent a successful terminal result. Fenced replies may
name an active phase or unknown state; stop requests require active state; terminal
decisions require a terminal state. Unknown and inconsistent enums fail closed.
Transport failures remain categorized through the existing `ClientError` policy.

## Worker integration requirements

The worker must journal an assignment before acknowledging startup, inspect its own
container before reporting progress, and retain event identities across transport
retries. This API trusts authenticated runtime reports; it cannot inspect the remote
Docker daemon itself. An acknowledged phase is not a new lease grant. After changing
phases, obtain a fresh renewal batch to learn remaining authority for the new phase;
an old renewal replay remains bounded by its original phase deadline.

The [execution coordinator](worker-launch.md) now connects durable journaling,
runtime observation, phase reports, and fresh renewal. Production acquisition,
reaping, artifact publication, and completion remain required before this becomes
a complete worker execution path.

## Verification

`sh scripts/test-store.sh` passed with real PostgreSQL and the Go race detector:
concurrent startup replay, forward/fast-exit transitions, exact lease preservation,
configured phase timeout bounds, repeated-phase deadline preservation, backward
report rejection, changed event/container/exit evidence conflicts, stale authority,
expiry, cancellation, credential revocation, terminal attempts, and an injected event
failure rollback. An observed database lock wait expires a lease before the report
can acquire its job lock and proves the report cannot revive it. Concurrent reports
for two attempts with the same event UUID commit exactly one transition. Holding
scheduler and worker locks does not block a valid phase report.

`sh scripts/test-schema.sh` passed fresh apply, complete rollback, and reapply with
migration 0008. `make test lint smoke` passed. These checks prove the store boundary;
they do not prove runtime observation, process termination, or complete jobs.

The real mTLS service test additionally reports startup and running, replays an
older acknowledgement without regression, restarts the service and replays again,
and enters finalization with explicit exit code zero. PostgreSQL confirms zero is
persisted rather than treated as missing evidence. Invalid phase enums, overflow,
short/changed container IDs, missing final exit status, cross-host identity, expired
reports, replaced sessions, and live credential revocation are checked at this
boundary. Response-mapping unit tests reject malformed decision/state pairs.

The Rust work probe now sends and replays startup, running, and finalization reports
for each of three acquired attempts through the real Go executable. Database checks
verify three finalizing attempts with container bindings and explicit exit code zero,
nine report identities, and nine transition events despite repeated delivery. An old
startup replay acknowledges finalization without regressing it. These observations
are synthetic protocol fixtures and do not claim that Docker ran or exited.

Rust unit tests check malformed authority/evidence and invalid or regressing reply
states. The delayed-renewal fixture initially echoed an older requested phase instead
of retaining later progress; it now tracks monotonic phase state so the test reaches
its intended delayed renewal and verifies the real client's local expiry rejection.
`sh scripts/test-store.sh` and `make test lint smoke` passed with the Rust phase client.
