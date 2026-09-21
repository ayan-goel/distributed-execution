# Durable attempt upload declarations

## Store contract (D11b)

`store.CreateUpload` persists an immutable declaration for one authenticated worker
attempt before any transfer URL is signed. It accepts the full job, attempt, worker,
session, and generation tuple, a request UUID, artifact kind/name, exact byte count,
lowercase SHA-256, and `part_count=1`. It returns the durable upload ID and generated
object key only after the transaction commits with current authority.

This method performs no storage/network I/O. The [versioned storage adapter](object-storage.md)
is verified separately; RPC orchestration and exact-version finalization remain the
next integration boundary. Creating a declaration neither verifies uploaded data
nor accepts a result, renews a lease, changes phase, or releases reservations.

## Identity and replay

Migration `0009_artifact_uploads` adds `artifact_uploads`, uniquely keyed both by
upload UUID and by `(worker_id, session_id, request_id)`. The payload digest includes
the operation name, complete authority, kind, logical name, length, checksum, and
part count. The request UUID identifies the operation and is not part of its digest.

Identical concurrent retries return the same record and generate one `UPLOAD_CREATED`
event. Reusing the request UUID with changed content or another attempt conflicts.
Stored replay fields are rehashed before use, so inconsistent declaration data cannot
silently issue a capability. A different request UUID creates a different upload
identity and counts against the attempt budget, even for the same logical name.

The database generates this object key from immutable UUID columns:

```text
projects/<project>/jobs/<job>/attempts/<attempt>/uploads/<upload>
```

Workers cannot supply a shared key or put a logical filename into this path.
Composite foreign keys bind the actual project's job and the complete attempt
authority. An AFTER UPDATE trigger prevents rebinding an existing declaration;
it compares rows after PostgreSQL computes the generated key so no-op updates
remain valid. Verified
versions will reference the declaration through a separate immutable record; there
is no verified-artifact or terminal-publication state in this slice.

## Authorization, phases, and bounds

Each call, including replay, checks the live credential and current worker session.
Job and attempt locks are taken in the established order. The current session is
checked again after those locks. The method claims its replay identity only after
ownership locks, then samples `clock_timestamp()` after any resulting wait.

The decisions match the lease boundary:

| Decision | Condition |
| --- | --- |
| `ACCEPTED` | Matching current owner, live lease and phase, no cancellation |
| `FENCED` | Unknown/mismatched ownership or expired lease |
| `STOP_REQUESTED` | Cancellation or phase expiry |
| `ALREADY_TERMINAL` | Matching attempt already ended |

Fenced sessions and revoked credentials reject the call before returning upload
metadata. Rejected calls leave no new declaration or event. Changed replay payloads
conflict; malformed or undeclared artifacts are invalid requests. Timings in an
accepted result are snapshots of existing authority, not new grants. The future RPC
layer must keep signing and transfer lifetimes bounded while doing storage checks
outside this transaction, and publication must revalidate authority independently.

Artifact declarations obey these rules:

| Kind | Logical name | Allowed phase | Additional bound |
| --- | --- | --- | --- |
| `OUTPUT` | Exact name from immutable job outputs | FINALIZING | Declared output `maxBytes` |
| `LOG` | `stdout` or `stderr` | STARTING, RUNNING, FINALIZING | Global single-part limit |
| `MANIFEST` | `result` | FINALIZING | 1 MiB |

Every artifact is at most 64 MiB and uses one part. Each attempt is limited to 1024
declarations and 8 GiB of declared bytes. Count limits include empty uploads; byte
limits include all retained pending/verified declarations. The attempt lock serializes
the aggregate check and insert. A rejected over-budget request rolls back its record
and event; an exact replay consumes no additional count or bytes.

These are the initial bounded single-part limits. Multipart transfer and its larger
object policy remain required for full v0.1; oversized requests are rejected explicitly.
Do not prune active declarations to regain budget or erase replay identity. Retention
and orphan cleanup will follow authoritative references and inactive-attempt policy.

No cluster scheduler advisory lock or worker row lock is required: uploads do not
change compute accounting. The transaction uses the existing two-second lock and
four-second statement limits. The declaration, job-event sequence, and event commit
atomically; an event-write failure rolls all three back.

## Verification

Unit tests cover identity/payload hashing, malformed fields, unsupported multipart
requests, declared-output matching, phase requirements, and reserved log/manifest names.

Real PostgreSQL race tests cover 16 simultaneous identical requests, changed-payload
conflicts, a request UUID contested by two attempts, exact scope/key generation,
immutable declarations, and project/session/generation foreign-key mismatches.
Competing requests for the final count or byte budget slot yield one acceptance and
one limit error; replay remains valid at the exhausted budget. Event injection proves
rollback leaves no record or sequence change.

Expired leases/phases, cancellation, revoked credentials, and approved session takeover
prevent reusing metadata to regain authority. An observed PostgreSQL job-lock wait
expires the lease before acquisition and verifies the fresh-time check. A separate
test holds scheduler and worker locks while upload creation succeeds.

Migration tests apply, roll back, and reapply the complete schema. A populated
eight-to-nine upgrade retains an active attempt's identity, phase, and deadlines and
allows its first upload declaration afterward. `make test lint smoke` and
`make integration` cover the project; `scripts/test-store.sh` runs the database and
authenticated executable suites with the Go race detector.

Reference: [PostgreSQL generated columns and trigger ordering](https://www.postgresql.org/docs/17/ddl-generated-columns.html).
