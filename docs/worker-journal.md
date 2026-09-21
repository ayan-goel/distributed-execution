# Durable worker attempt journal

`rust/worker/src/journal.rs` implements the local evidence portion of spec §11.1.
It is a library prerequisite for the worker supervisor. The worker executable does
not yet run jobs or perform startup reconciliation.

## Stored state and ordering

`Journal::open` binds an existing directory to one configured worker UUID. Each
attempt record contains the exact assignment/spec bytes, authority identifiers,
an optional immutable Docker container ID, observed exit/OOM evidence, and up to
three exact phase-report requests. UUIDs and the spec SHA-256 are validated before
writing and after reading. Replaying an identical assignment preserves all evidence;
changing its identity or execution specification returns `Conflict`.

The intended supervisor order is:

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

## Evidence cannot restore a lease

Persisted assignments have `lease_duration_ms`, `phase_remaining_ms`, and
`server_time_unix_ms` set to zero. `RecoveredAttempt` is distinct from the client's
live `GrantedAssignment`; it contains no `AuthorityWindow`. Recovery must obtain
fresh server state and follow the new-session stop/reconcile policy in spec §8.
Finding a journal file never authorizes starting or adopting a container.

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
| `<attempt-uuid>.attempt` | Framed canonical protobuf attempt record |
| `.pending` | Uncommitted replacement; never recovery evidence |

Every frame has an 8-byte `DSPJNL01` magic/version, an 8-byte big-endian payload
length, a 32-byte SHA-256 payload checksum, then the payload. Payloads are at most
5 MiB. Unknown frame versions, length/checksum errors, invalid record semantics,
and protobuf bytes that do not re-encode identically are rejected. SHA-256 detects
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
identity/lock metadata, filesystem overhead, and one temporary replacement of at
most 5 MiB plus its 48-byte header. Inventory is sorted and bounded; records are
loaded individually. Budget exhaustion rejects a write without removing old data.
Accepted-completion cleanup and retention are still required before long-running
worker use; there is deliberately no automatic evidence eviction.

Specs can contain environment secrets. The state directory is private, and journal
errors and `RecoveredAttempt` Debug output omit paths/spec contents. Explicitly
reading or logging the exposed assignment still requires caller care.

The API performs synchronous filesystem I/O. The future supervisor must execute
it through a bounded blocking pool separate from lease maintenance. Kernel fsync
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

`sh scripts/test-worker-linux.sh` runs the complete worker test suite on Linux.
On macOS it fetches locked target dependencies, checks the official protoc 34.1
archive checksum, and compiles/runs offline in the pinned Rust 1.88 image. Source
and registry mounts are read-only. Journal fixtures use an anonymous Linux volume,
removed with the container, so their filesystem calls are not macOS bind-mount
operations. `make integration` runs this gate plus the separate real Docker,
migration, and PostgreSQL/mTLS fixtures. The focused clock script remains available.

Session/registration persistence, completion and transfer identities, terminal
cleanup, labeled-container discovery, startup recovery, and the production
supervisor remain required. D10 is not complete until restarting the actual agent
stops old-session containers before advertising new capacity.

Persistence references: [Rust rename](https://doc.rust-lang.org/std/fs/fn.rename.html),
[File::sync_all](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all),
[Linux fsync](https://man7.org/linux/man-pages/man2/fsync.2.html), and
[Linux flock](https://man7.org/linux/man-pages/man2/flock.2.html).
