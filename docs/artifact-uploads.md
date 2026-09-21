# Durable attempt upload declarations

## Store contract (D11b)

`store.CreateUpload` persists an immutable declaration for one authenticated worker
attempt before any transfer URL is signed. It accepts the full job, attempt, worker,
session, and generation tuple, a request UUID, artifact kind/name, exact byte count,
lowercase SHA-256, and `part_count=1`. It returns the durable upload ID and generated
object key only after the transaction commits with current authority.

This method performs no storage/network I/O. The [versioned storage adapter](object-storage.md)
is integrated with the authenticated upload RPC described below. The
[verified artifact API](verified-artifacts.md) registers exact versions separately.
Creating a declaration neither verifies uploaded data
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
remain valid. Verified versions reference the declaration through a separate
immutable artifact record. Creating a declaration alone never marks it verified.

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
accepted result are snapshots of existing authority, not new grants. The RPC layer
performs storage checks outside this transaction and rechecks authority afterward;
publication must revalidate authority independently.

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
eight-to-current upgrade retains an active attempt's identity, phase, and deadlines and
allows its first upload declaration afterward. `make test lint smoke` and
`make integration` cover the project; `scripts/test-store.sh` runs the database and
authenticated executable suites with the Go race detector.

Reference: [PostgreSQL generated columns and trigger ordering](https://www.postgresql.org/docs/17/ddl-generated-columns.html).

## Authenticated upload capabilities (D11c)

`WorkerService.CreateUpload` binds the claimed worker to the mTLS identity, rejects
unsupported kinds/parts and integer overflow, and commits the declaration above.
It then checks bucket versioning and signs a PUT outside all database transactions.
An exact declaration replay rechecks credentials, session, ownership, lease, phase,
and cancellation after signing. Only a still-accepted identical record returns a
capability. Neither check renews authority or consumes an additional upload budget.

The response contains the durable upload ID, generated key, URL, required signed
headers, and conservative expiry; `parts` is empty for this single-part profile.
Send a PUT with the declared bytes and all required headers. URLs last 30 seconds.
An exact retry can return a newly signed URL for the same upload; URL bytes and
expiry are not the durable replay identity. Signing failure retains the declaration
so retries reuse its ID and event rather than accumulating pending uploads.

Already-issued URLs may remain usable briefly after cancellation or expiry. They
write only their attempt-specific key. Every accepted object reference must pin an
exact version, and finalization/publication must independently fence ownership.
The second check is a transaction-time authorization decision, not a promise that
ownership cannot change while the response travels to the worker.

| Failure | gRPC code / stable reason |
| --- | --- |
| Missing configured backend | `FailedPrecondition / OBJECT_STORAGE_NOT_CONFIGURED` |
| Expired or mismatched attempt | `FailedPrecondition / UPLOAD_FENCED` |
| Cancelled or phase expired | `FailedPrecondition / UPLOAD_STOP_REQUESTED` |
| Terminal attempt | `FailedPrecondition / UPLOAD_ALREADY_TERMINAL` |
| Revoked credential | `Unauthenticated / UNAUTHORIZED_WORKER` |
| Old session | `FailedPrecondition / SESSION_FENCED` |
| Declaration budget exhausted | `ResourceExhausted / UPLOAD_LIMIT_EXCEEDED` |
| Changed replay | `AlreadyExists / REQUEST_CONFLICT` |
| Disabled bucket versioning | `FailedPrecondition / OBJECT_VERSIONING_REQUIRED` |
| Backend unavailable | `Unavailable / OBJECT_STORAGE_UNAVAILABLE` |

Malformed declarations remain `InvalidArgument`. The existing five-second worker
RPC deadline bounds signing, its concurrency queue, and both database operations.
Signed URLs and raw storage diagnostics never appear in error messages.

Unit tests cover missing identity, wire overflow, kinds/parts, explicit configuration,
and refusal of ambient AWS credentials. Real PostgreSQL/mTLS tests check signature
scope, headers, replay identity, unchanged lease/phase, and signing failures. While
a controlled HTTP versioning response is blocked, database mutations revoke the
credential, expire the lease/phase, or cancel the job; each commits without waiting
for storage and prevents the response from returning a grant.

The full integration gate also starts PostgreSQL and SeaweedFS together. A real mTLS
response uploads declared bytes, which the storage adapter verifies by exact version.
Changing checksum or key fails. Cancellation prevents new grants, while reuse of an
existing URL creates a distinct version and leaves the original bytes intact. This
is extended by the [finalization gate](verified-artifacts.md) to prove durable
verified-artifact registration. Result publication remains pending.
