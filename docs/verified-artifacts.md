# Verified artifact registration

## Transaction contract (D11d)

`store.FinalizeUpload` accepts an authenticated worker, the complete attempt authority,
a request UUID, an existing upload UUID, and an exact object key/version/byte count/
SHA-256. The key, size, and hash must match the immutable upload declaration. Versions
are nonempty printable ASCII, at most 1024 bytes, and cannot be the replaceable `null`
version. The current bounded profile remains single-part and at most 64 MiB.

The operation has three stages:

1. A short transaction validates credentials/session/ownership, binds the request
   identity to the candidate version, checks fresh database time, and commits.
2. A trusted verifier reads and validates that exact object with no open database
   transaction. It receives the already matched key, version, length, and checksum.
3. Another short transaction repeats authorization, expiry, cancellation, phase,
   and replay checks before inserting the verified artifact and its event together.

The store has no public operation that simply marks client-supplied bytes verified.
The caller must supply the trusted verification function; the production RPC must
connect it to the exact-version storage adapter. A nil verifier is rejected.
Storage failures retain the candidate request for retry but create no artifact.
Expiry, cancellation, revocation, or takeover during verification prevents registration.

Neither stage changes compute reservations, lease deadlines, attempt phase, job
state, or the accepted result. An artifact is verified data, not a successful job.
`CompleteAttempt` must later validate its manifest and current authority independently.

## Durable identity and limits

Migration 0010 adds `artifact_finalizations` and `artifacts`:

- Finalizations uniquely bind `(worker_id, session_id, request_id)` to an upload and
  candidate version. The digest includes the complete authority and exact object.
  Changed payloads conflict even after an earlier verification failed.
- Artifacts select one version per upload. Foreign keys bind the verified record to
  the exact upload/version in its finalization request. Ownership, key, size, kind,
  and logical name are obtained through the immutable upload declaration.
- Both records reject mutation. Artifact membership means verified; the separate
  upload and finalization records retain pending intent. Dataset artifacts and
  retention tombstones will be added with their corresponding lifecycle behavior.
- Each attempt may retain at most 1024 finalization requests, independently of its
  upload count/byte budgets. This prevents unbounded request history targeting one
  upload. The attempt lock serializes the aggregate check; identical retries cost
  no extra slot. Active-work records must not be pruned to evade limits.

Concurrent identical requests return the same artifact ID and produce one
`ARTIFACT_VERIFIED` event. A new request ID for the same already-verified object also
returns that artifact without re-reading storage, subject to the request budget.
Different candidate versions may be verified concurrently, but only the first
committed version is selected; competitors conflict and cannot rebind the upload.

An exact retry after successful registration uses the durable artifact as evidence
of completed verification. It does not recheck object availability. Object retention
must preserve referenced versions; a later missing object is a storage/retention
failure and must never fall back to the key's latest version.

All replies, including already-verified retries, require current authority. Rejected
attempts return no artifact. The existing lease decisions apply: `FENCED`,
`STOP_REQUESTED`, and `ALREADY_TERMINAL`. Credential/session errors and request
conflicts use the existing store error types. Budget overflow uses `ErrUploadLimit`.

## Locking and verification evidence

Both transactions use credential, job, then attempt locks; request identity is
claimed afterward to avoid foreign-key lock inversion. Database wall time is sampled
after ownership/replay waits. No scheduler advisory lock or worker accounting lock
is needed. The two-second lock and four-second statement bounds remain in force.

Unit tests exercise request identity and object validation. Real PostgreSQL tests
exercise 16 concurrent verifications, competing versions, event failure rollback,
immutable records, exact-version foreign keys, budget races, storage failure/retry,
and changes to lease, phase, cancellation, credentials, or session during verification.
These store tests deliberately use a controlled verifier to isolate state races;
they do not claim to verify real object bytes. The storage adapter has its separate
SeaweedFS compatibility gate. RPC integration of finalization remains the next slice.

Fresh/rollback/reapply migration tests cover the complete schema. Populated upgrade
tests retain active ownership from schema eight and an existing upload from schema
nine, then apply the current schema and complete the corresponding operation.
