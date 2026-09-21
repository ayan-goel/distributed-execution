# Durable worker attempt journal

`rust/worker/src/journal.rs` implements the local evidence portion of spec §11.1.
The [worker startup command](worker-agent.md) uses it for durable incarnation state
and performs Docker reconciliation. The worker executable does not yet run jobs.

## Stored state and ordering

`Journal::open` binds an existing directory to one configured worker UUID. Each
attempt record contains the exact assignment/spec bytes, authority identifiers,
an optional immutable Docker container ID, observed exit/OOM evidence, up to
three exact phase-report requests, and an optional pending completion request.
UUIDs and the spec SHA-256 are validated before
writing and after reading. Replaying an identical assignment preserves all evidence;
changing its identity or execution specification returns `Conflict`.

The [execution coordinator](worker-launch.md) implements this supervisor order
through FINALIZING:

1. Persist the assignment before taking any action on it.
2. Persist a STARTING report with `prepare_phase`, then send that exact request.
3. After acknowledgement and the remaining runtime/authority checks, create or
   discover the attempt's container and persist its full 64-character ID.
4. Persist/send RUNNING after observing runtime startup.
5. Persist observed exit code and Docker's OOM flag, then persist/send FINALIZING.

These records are **intent and observed evidence**, not server acknowledgements.
The journal enforces evidence order but cannot prove the caller observed Docker or
received an RPC acknowledgement. A fast-exit workload can skip RUNNING once its
container ID and exit evidence are durable. Exit code 137 alone does not imply OOM.

Each `prepare_phase` saves a random v4 event UUID and the complete request before
returning it. Repeated calls, including after reopening or reaching a later phase,
return the original request. The caller must retry that request unchanged after an
uncertain RPC response. Container and exit evidence cannot be rebound or rewritten.

## Durable output upload evidence (D11u)

The journal now stores one upload declaration per declared output, followed by its
stable upload ID/key, exact finalization request, and validated artifact reply.
`prepare_output` and `prepare_output_finalization` generate and sync request UUIDs
before returning them. Identical retries reuse those requests; changed content,
scope, object versions, or acknowledgement identities conflict. Signed URLs and
headers are never persisted. New upload progress requires local FINALIZING evidence
and is sealed once a completion request is stored; exact retries remain readable.

The records are revalidated against the assignment and shared RPC validators on
every load. Existing record and directory byte limits still apply, with at most
64 output records. New binaries read records without the optional upload field;
older binaries reject records containing it rather than silently dropping evidence.

Tests cover reopening pending and acknowledged evidence, asynchronous journal
access, rejected mutations, and omission of signed capabilities from saved files.
The [upload delivery component](worker-transfers.md#journaled-output-delivery) now
uses these records and is tested across process death with real versioned storage.
Live execution integration remains unfinished. Recovered records do not restore a
lease or authorize publication under an old session.

## Pending completion evidence (D11o)

`persist_completion` writes the entire `CompleteAttemptRequest` through the same
file-sync/rename/directory-sync path before the caller can send it. Its asynchronous
counterpart uses the journal's existing bounded blocking executor. The caller owns
the completion UUID and digest; the journal neither generates replacements nor
reconstructs metrics/output evidence. `RecoveredAttempt::completion()` exposes the
stored request for an unchanged retry after reopening.

The request must pass the control client's payload/digest validation and match the
assignment's full authority tuple. On first persistence, exit presence/value must
match the journal. A success also requires durable FINALIZING and an observed zero
exit without an OOM flag. Failures before container creation can have no exit code.
This validates consistency with local evidence; it does not prove runtime facts
or replace the server's phase, lease, cancellation, and artifact checks.

Once pending, the request is immutable, including its ID, digest, output ordering,
log gaps, and original metrics bytes. Identical retries succeed; changed evidence
conflicts. Assignment replay retains the completion. Existing phase/container
evidence remains replayable, but new container bindings and phase reports are
blocked. Later cleanup can record a previously unknown exit without rewriting the
pending request. Reload validates the request digest independently of the frame
checksum, so recomputing a checksum cannot hide a payload/digest mismatch.

The optional protobuf field preserves readability of older records in the new
binary. An older binary rejects records containing completion evidence because its
canonical re-encoding would lose that field; do not downgrade such a state directory.
The existing 5 MiB record, attempt-count, and total-byte bounds still apply.

Finding pending evidence never permits a fresh execution, restores a lease, or
proves a reservation released. Delivery/recovery must handle cancellation after
`STOP_REQUESTED` without overwriting an uncertain request.

## Durable completion replies (D11p)

`record_completion_response` requires the exact stored request and validates the
reply through the same decision/state/manifest checks as the control client. The
reply, including original accepted manifest bytes, is synced atomically with the
request. A delayed reply for another completion is rejected. Repeated identical
acknowledgements are idempotent, and accepted/fenced/already-terminal outcomes are
immutable. A stored `STOP_REQUESTED` may progress to fenced or already-terminal;
it cannot become acceptance of that same rejected payload. Server cancellation and
expired phase intent are irreversible, so such a change is a conflict.

Recovered replies are available through `completion_response()`. They are checked
again on read and cannot exist without a valid pending request. The optional field
has the same downgrade restriction as completion requests. Existing record/total
byte bounds include the response; a failed write preserves uncertainty and uses
the journal's normal poison/reopen rules.

A reply records publication status, not physical cleanup. In particular, a failed
completion submitted with `stopped=false` still needs runtime reconciliation even
when its failure was accepted. The journal does not delete workspaces or release
local capacity. [Startup recovery](worker-agent.md#completion-recovery-d11q) now
delivers pending entries and uses resolved replies without resending. Resolved-record
retention and live execution delivery remain required. Process-death tests cover both a pending
request and a persisted accepted reply; successful manifest tests preserve the
original formatting and exact large-integer bytes through reopening.

## Evidence cannot restore a lease

Persisted assignments have `lease_duration_ms`, `phase_remaining_ms`, and
`server_time_unix_ms` set to zero. `RecoveredAttempt` is distinct from the client's
live `GrantedAssignment`; it contains no `AuthorityWindow`. Recovery must obtain
fresh server state and follow the new-session stop/reconcile policy in spec §8.
Finding a journal file never authorizes starting or adopting a container.

## Durable incarnation identity

`begin_incarnation` takes validated registration claims with empty request/session
IDs, generates fresh UUIDs, and persists the complete registration request before
returning it. It can succeed only once per open journal handle. The agent must hold
that handle and its exclusive lock for the process lifetime. Network retries use
`StoredSession::registration()` unchanged; they do not call `begin_incarnation`
again or redraw IDs. Resource/slot/label/capability bounds match the Go registration
contract, including whole-MiB capacities and exactly one scratch capability.

`.session` records the registration request, an optional accepted generation, and
the preceding locally saved session ID. `record_registration` requires the worker
and session to match the incarnation begun by this handle, protocol version 1, and
a positive signed-range generation. Repeating that acknowledgement is idempotent;
changing its generation conflicts. Registration acceptance does not restore lease
authority or readiness, and changing server cleanup instructions are not persisted
as a lasting local fact.

On process restart, `load_session` provides historical evidence only. A newly
opened journal cannot acknowledge that old incarnation. The replacement calls
`begin_incarnation` to persist a fresh request/session, then uses the server's
recovery protocol before cleaning up old containers. This applies even if the
previous registration reply was lost. The predecessor ID can refer to an unaccepted
request; it is diagnostic evidence, not proof of the server's current session.
Existing attempt records remain available throughout this transition. Never change
IDs repeatedly while retrying an uncertain registration within one process.

Session metadata is capped at 64 KiB plus its frame header. It uses the same
file-sync/rename/directory-sync commit path and poison-on-error behavior as attempts.
Map ordering inside registration protobufs is not canonical: replay retains field
values and IDs, while the server's registration hash canonicalizes map/set order.
Session reads verify framing, checksum, bounds, supported protocol, and semantics.
Missing worker identity alongside a session file is corruption, not a fresh install.
Malformed session evidence must be investigated rather than overwritten on startup.

Heartbeats and readiness are not resumed from this metadata. The startup/health
loop maintains an increasing heartbeat sequence and stable request payload for
each uncertain retry, starting a fresh sequence for each new session. Active-job
orchestration still needs integration with that loop.

## Asynchronous access during execution

`AsyncJournal::new` consumes the exclusive `Journal` and provides cloneable handles
to that same owner. The worker startup command now records its registration through
this handle and retains it throughout the health loop. Attempt assignment, container
binding, exit evidence, phase preparation, completion persistence, and reads have asynchronous counterparts
with the same durable semantics and errors as the synchronous journal.

Each handle shares a one-permit semaphore and a mutex around the journal. Operations
wait for the semaphore asynchronously **before** submitting a blocking task. At most
one journal task per worker can occupy Tokio's blocking pool; pending callers do not
occupy additional blocking threads. The mutex protects the single journal owner,
including its poison state. Each returned success still follows file sync, rename,
and directory sync. No network or Docker operation runs under this journal lock.

The blocking closure owns its permit and journal reference. Cancelling an awaiting
future does not cancel a filesystem operation that already started: that operation
retains the permit and exclusive ownership until it finishes. A cancelled call can
therefore still commit, and its absence of acknowledgement is not rollback evidence.
Callers must read/reconcile stored evidence or use its existing stable operation
identity; they must not infer permission to repeat an external launch. A blocking
panic poisons the mutex and later operations refuse to continue.

This keeps normal filesystem latency off lease/heartbeat tasks. It does not provide
a hard timeout for a kernel-stalled write or fsync. Such a stall holds only this
journal's one blocking task but may delay runtime shutdown; independent watchdogs
must still enforce authority, and process recovery follows the existing journal and
session rules. The execution coordinator must bound the number of pending callers.

Tests use a single-thread Tokio runtime with deliberately stalled blocking work:
an async timer keeps progressing, cancellation cannot release the semaphore or
exclusive journal lock, and queued work starts only after the blocked closure exits.
Concurrent cloned handles persist one STARTING event identity. Reopening retains
the exact FINALIZING report and OOM evidence while stored lease timing remains zero.
The implementation follows the pinned Tokio `spawn_blocking` contract: started
blocking tasks cannot be aborted by cancelling their awaiting async task.

## Filesystem contract

Use a dedicated local filesystem directory, outside every workload mount, owned
by the worker's effective UID with mode 0700. The caller supplies an absolute,
canonical path to an existing directory. Files must be owned regular files with
mode 0600 and exactly one hard link. Symlink leaves and unsafe modes are rejected.
Ancestor directories, the worker UID, and root are trusted installation boundaries;
do not place state beneath directories that untrusted users can rename or replace.
This is not a network-filesystem locking/durability contract.

A permanent `.lock` inode is held with nonblocking exclusive `flock` for the
journal's lifetime. Never remove that inode while a process might own it. A second
owner receives `Busy`; process death releases the kernel lock. The directory's
device/inode and permissions are checked before operations to detect replacement.
These checks do not defend against hostile processes with the worker UID or root.

Directory contents:

| File | Meaning |
| --- | --- |
| `.lock` | Permanent ownership lock |
| `.identity` | Framed configured worker UUID |
| `.session` | Registration request, accepted generation, and predecessor ID |
| `<attempt-uuid>.attempt` | Framed canonical protobuf attempt record |
| `.pending` | Uncommitted replacement; never recovery evidence |

Every frame has an 8-byte `DSPJNL01` magic/version, an 8-byte big-endian payload
length, a 32-byte SHA-256 payload checksum, then the payload. Payloads are at most
5 MiB, with the smaller session limit above. Unknown frame versions, length/checksum
errors, and invalid record semantics are rejected. Attempt records additionally
reject protobuf bytes that do not re-encode identically. SHA-256 detects
corruption; permissions and ownership supply the access boundary, not the checksum.

Updates create `.pending` exclusively, write the complete frame, sync that file,
rename it over the destination on the same filesystem, then sync the directory.
Only then does the operation report success. On reopening, the owner discards a
known private regular `.pending` file before reading committed records. An unknown
directory entry is an error, not a file to delete. A write/sync/rename error poisons
the current handle: reopen and reconcile the visible committed state before taking
further action. Do not interpret an ambiguous write error as proof nothing changed.

The default limit is 4096 committed attempts and 256 MiB of attempt frames. Limits
are configurable up to 4096 attempts and 32 GiB. The byte budget excludes small
identity/lock/session metadata, filesystem overhead, and one temporary replacement of at
most 5 MiB plus its 48-byte header. Inventory is sorted and bounded; records are
loaded individually. Budget exhaustion rejects a write without removing old data.
Accepted-completion cleanup and retention are still required before long-running
worker use; there is deliberately no automatic evidence eviction.

Specs can contain environment secrets. The state directory is private, and journal
errors and `RecoveredAttempt` Debug output omit paths/spec contents. Explicitly
reading or logging the exposed assignment still requires caller care.

The base API performs synchronous filesystem I/O; asynchronous execution uses
`AsyncJournal`'s bounded blocking pool separate from lease maintenance. Kernel fsync
latency has no strict upper bound. Successful sync relies on the filesystem,
storage hardware, and VM honoring persistence semantics.

## Verification and remaining integration

Native journal tests cover replay, conflicting evidence, fast exit, timing removal,
private modes, symlinks/hard links, corruption, budgets, and competing owners.
Private commit checkpoints inject errors after create, write, file sync, rename,
and directory sync; reopening sees the complete old or new record and removes only
the known temporary. A separate process persists an event, is killed, and its
replacement recovers the same event UUID and zero timing fields. These are process
death/error tests, not simulated power failure or physical storage fault tests.
The process-death test also leaves an unacknowledged registration on disk and
verifies that its replacement preserves the predecessor while choosing a fresh
session. It now also recovers the exact pending completion after killing its owner.
Completion tests cover concurrent identical writes, changed identity/source metrics,
authority/digest/exit/phase mismatches, OOM evidence, late cleanup observations,
and semantic corruption with a recomputed frame checksum.
Session tests check retry payloads, acknowledgement identity/generation,
changed cleanup responses, invalid claims, missing identity, and corruption.

`sh scripts/test-worker-linux.sh` runs the complete worker test suite on Linux.
On macOS it fetches locked target dependencies, checks the official protoc 34.1
archive checksum, and compiles/runs offline in the pinned Rust 1.88 image. Source
and registry mounts are read-only. Journal fixtures use an anonymous Linux volume,
removed with the container, so their filesystem calls are not macOS bind-mount
operations. `make integration` runs this gate plus the separate real Docker,
migration, and PostgreSQL/mTLS fixtures. The focused clock script remains available.

Session/registration persistence and labeled-container discovery are implemented as
components. The startup agent reconciles old-session containers before advertising
capacity and recovers pending completions concurrently. Transfer identities,
terminal cleanup/retention, and the production job acquisition/execution loop
remain required.

Persistence references: [Rust rename](https://doc.rust-lang.org/std/fs/fn.rename.html),
[File::sync_all](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all),
[Linux fsync](https://man7.org/linux/man-pages/man2/fsync.2.html), and
[Linux flock](https://man7.org/linux/man-pages/man2/flock.2.html).
