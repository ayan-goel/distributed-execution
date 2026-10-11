# Multipart output transfers

Specification §13.3 requires multipart outputs above a configurable threshold.
The storage lifecycle is implemented and verified against the local versioned
backend. Part plans, initialization identities, and completion intent/version
are durable. The authenticated worker API coordinates multipart initialization,
individual part grants, completion recovery, and verified publication. Paired
Rust client contracts and durable part evidence are implemented; automatic delivery, larger
object limits, and the submitted-job demonstration remain required. Public
uploads retain the 64 MiB cap; R02 is not complete.

## Storage lifecycle

`internal/objectstore` provides:

| Operation | Contract |
| --- | --- |
| `BeginMultipart` | Check versioning and bounded size/part plan, request SHA-256 part checksums, return backend upload identity |
| `BeginIdentifiedMultipart` | Bind the durable initialization UUID to server-owned object metadata during creation |
| `PresignPart` | Bind one key/upload/part number, exact length, and SHA-256 to a short-lived PUT capability |
| `CompleteMultipart` | Require consecutive ordered parts with ETags/checksums, check versioning, return the exact non-null version from storage |
| `AbortMultipart` | Abort only the supplied key/upload, then inspect at most one remaining part; repeated missing-upload responses establish absence at the time of the check |
| `RecoverMultipartVersion` | Inspect at most 16 listed versions/markers, HEAD exact versions, require one initialization/size match, and return its immutable version |

Plans obey the existing configured object cap. Parts are 5–64 MiB except the
last, which can be smaller; part counts are capped at 10,000. Calls share the
adapter's concurrency permits and 30-second operation budget. Invalid plans,
part indices, checksums, and completion lists fail before network access.
Abort validates stable identity independently of current admission size limits,
so lowering a limit cannot prevent cleanup of older uploads.

The storage SDK handles errors embedded in HTTP 200 completion replies. Missing
upload state returns `ErrMultipartGone`, not success: the upload may have been
completed or aborted. Other transport/backend errors expose stable categories
without signed URLs or raw diagnostics. The adapter never substitutes the key's
latest version after an uncertain completion.

Identified uploads can recover after a completion reply is lost: enumerate the
generated key's versions, then inspect each exact version's server-bound
initialization metadata. Recovery requires exactly one match of the declared
size. It refuses truncated inventories, duplicate version IDs, null/substituted
versions, and ambiguous matches. No match returns `ErrMultipartGone`, which does
not distinguish an aborted upload from an unfinished or deleted one. The caller
still needs durable completion intent and full-byte `Verify` before publication.
Recovery shares the 30-second/concurrency budget and requires backend permission
for `s3:ListBucketVersions` and exact-version metadata reads. The 16-entry bound
keeps abnormal version histories from consuming unbounded finalization work.

Multipart completion alone does not verify or accept a result. Call `Verify`
against the returned exact version to check declared size and full-object
SHA-256, then pass through the existing fenced artifact/completion transactions.
Multipart ETags and composite part checksums are not full-object SHA-256 evidence.

In-flight part uploads can race abort. The coordinator must stop transfers and
retry cleanup after capabilities expire if needed. A single successful abort is
not a claim that no in-flight write can still finish remotely.

## Durable declaration and initialization metadata

Migration `0021` adds immutable part sizing to output declarations and a separate
multipart identity row. Internal declarations permit up to 8 GiB, with 5–64 MiB
parts and at most 10,000 parts; the existing 8 GiB aggregate attempt budget still
applies. Single-part payloads retain their original canonical hash and size cap.
This metadata limit does not enable larger public uploads or downloads.

`CreateUpload` persists the plan and a stable initialization UUID.
`BindMultipartUpload` saves one backend upload ID after fresh attempt-authority
checks. Concurrent identical declarations return the same identity; competing
backend IDs have one winner, and a repeat of the winner is idempotent. Binding
and its audit event commit together. Backend IDs are omitted from public events.

The coordinator creates the declaration before storage initialization, performs
storage I/O outside the transaction, and binds afterward. A confirmed losing or
fenced initializer aborts its unused backend upload with a separate bounded
context. A generic binding error can hide a successful database commit, so it
preserves the backend for replay or reconciliation. Ambiguous storage creation
responses and automatic orphan cleanup remain unresolved.
The initialization UUID alone is not proof of a completed storage version.
Only the winning backend identity may receive part grants or completion calls;
unused initializers must be aborted. Recovery rejects multiple completed versions
carrying the same initialization UUID instead of choosing one.

The migration preserves existing single-part replay and attempt deadlines. Its
downgrade refuses to discard any multipart declaration or identity.

## Durable completion and publication gate

Migration `0022` stores one completion intent per multipart upload: a stable
request ID, immutable ordered part numbers/ETags/SHA-256 values, and a set-once
exact completed version. The internal `CompleteMultipartUpload` path commits
this intent before invoking storage outside database transactions. After storage
returns, a second transaction rechecks credentials, session, cancellation, lease,
and phase using fresh database time before saving the version and audit event.
Storage failure or an event rollback leaves the prepared intent available for
retry. Downgrade refuses to discard prepared or stored intents.

A stored replay returns its version without another storage call. Concurrent
prepared requests may both call storage; the callback must tolerate concurrent
completion and use identified exact-version recovery for lost replies or
`NoSuchUpload`. Only one version/event can be recorded. Changed request IDs or
part evidence conflict. Binding the storage version does not verify bytes or
accept a job result.

`FinalizeUpload` and its database guard require multipart artifact verification
to use this stored version. The existing trusted full-byte verifier and fenced
artifact transaction still apply. Another version with matching bytes cannot
substitute for the completed version. These internal paths currently retain the
64 MiB artifact limit; larger size policy and automatic Rust delivery remain
pending.

## Durable part grants

Migration `0023` binds one SHA-256 to each upload/part number before a capability
is signed. `PrepareMultipartPart` checks the active worker/session/attempt and
finalization phase with fresh database time after locks. Repeated requests reuse
the same content identity; a changed hash conflicts. Part numbers must fit the
immutable declaration. A completed upload cannot acquire another part grant.
Completion intent must match every previously declared part hash in both Go and
SQL. The migration backfills part hashes from existing completion intents and
refuses to discard any bound part evidence on downgrade. This is the metadata
prerequisite for bounded part RPCs.

## Authenticated worker API

`CreateUpload` accepts an OUTPUT plan with `part_count` and `part_size_bytes`.
Initialization returns the stable public upload ID, object key, and plan; it
returns no inline part capabilities or backend upload ID. Equal retries return
the winning binding after a fresh authority check.

`GrantUploadPart` accepts the attempt authority, public upload ID, part number,
and lowercase SHA-256. It persists the content identity before signing one
30-second PUT capability with its required headers and expiry. It rechecks
authority after signing, so cancellation or fencing during storage I/O prevents
delivery. Changing the checksum for an already bound part conflicts.

Multipart `FinalizeUpload` takes the expected key, full size and SHA-256 with an
empty version, plus ordered part numbers, ETags, and checksums. It binds the
expected object and part evidence to durable completion intent, completes or
recovers the identified storage version, then verifies every byte before fenced
artifact publication. A replay returns the same artifact and exact version.
Single-part requests retain their existing version-bearing contract.

Rust's control client validates these requests and responses, including plan
consistency, consecutive parts, checksums, capability bounds, and returned exact
versions. The current worker transfer loop still requests single-part uploads.

## Rust journal

`prepare_output_plan` persists immutable part sizing with the declaration.
`prepare_output_part` saves a part checksum before any grant or PUT, and
`record_output_part` records its ETag once after a successful transfer. Part
evidence is sorted by number, so independent acknowledgements can arrive out of
order. Changed checksums, ETags, plans, or authority conflict. Empty ETags identify
prepared parts whose PUT outcome still needs resolving with the same bytes.

`prepare_multipart_finalization` requires every part's checksum and ETag, then
seals one ordered versionless request for the coordinator's exact-version
completion path. Reopening retains the same request ID and payload. The verified
reply subsequently saves the returned exact version and artifact ID. Preparing
or acknowledging new parts after finalization or attempt completion is sealed
fails; equal historical retries remain readable.

Only stable evidence is journaled; signed capabilities are never persisted.
Existing single-part journal records remain readable with an empty part list.
The execution loop still uses single-part transfers until multipart HTTP delivery
and plan selection are connected.

## Remaining R02 implementation

1. Reconcile unknown storage creation/binding outcomes and abandoned initializers
   through R03 cleanup without deleting the winning backend identity.
2. Wire journaled Rust part delivery with configurable threshold/part sizing,
   bounded concurrency, source hashing, same-content retries, cancellation and
   lease/finalization deadlines. Extend object-size policy together with
   verification/download bounds; changing only the upload cap is insufficient.
3. Run a submitted job exceeding the old 64 MiB cap, retrieve its exact result,
   and inject lost responses, interrupted transfers, and stale completion.
   Connect abandoned-upload cleanup to R03 retention.

Each step is a verified slice of the same release feature. None is satisfied by
the low-level storage gate alone.

## Observed evidence

The real mTLS worker test initializes a two-part output, uploads both signed
checksum-bound parts, rejects changed checksum replay, and verifies the exact
completed version and stable finalization replay. Cancellation prevents another
part capability. A focused regression preserves the backend on generic binding
errors; five Go-to-Rust-to-Go fixtures preserve initialization, part grant, and
completion fields.

The real storage gate creates two multipart versions of one object, checks each
with full SHA-256, rejects altered part content, and confirms a wrong full hash
fails even for an existing version. It uploads an abandoned part, lowers the
configured object cap, aborts twice, rejects the old part capability, and verifies
that completed versions survive. Each test owns a fresh bucket inside the
disposable backend.

The multipart gate also discards each successful completion response after the
backend commits, reconstructs the client, and recovers its exact version. After
overwrite, recovery still selects the original identified version and full-byte
verification succeeds. Unit cases cover missing, ambiguous, truncated,
wrong-size, null, and substituted-version inventories.

Unit tests cover signed scope/length/checksum, invalid plans, completion order,
versioning loss, null versions, embedded errors, missing uploads, and remaining
or truncated parts after abort. Run:

```sh
sh scripts/test-objectstore.sh .tools/go/bin/go test -race -tags integration \
  ./internal/objectstore -run 'TestRealMultipart|TestRealVersioned' -count=1 -v
```

Sources: [S3 multipart lifecycle](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html),
[completion and embedded errors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html),
[abort and in-flight uploads](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html),
and [part limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html).
Recovery uses [version listing](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)
and [exact-version metadata reads](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html).
