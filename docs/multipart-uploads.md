# Multipart output transfers

Specification §13.3 requires multipart outputs above a configurable threshold.
The storage lifecycle is implemented and verified against the local versioned
backend. Part plans and initialization identities are durable. Completion
recovery, worker RPCs, Rust delivery, larger object limits,
and the submitted-job demonstration remain required. The public upload path
still accepts one part and at most 64 MiB; R02 is not complete.

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

The future coordinator must create the declaration before storage initialization,
perform storage I/O outside the transaction, and bind afterward. A losing or
fenced initializer must abort its unused backend upload. This slice does not
recover ambiguous storage responses or implement automatic orphan cleanup.
The initialization UUID alone is not proof of a completed storage version.
Only the winning backend identity may receive part grants or completion calls;
unused initializers must be aborted. Recovery rejects multiple completed versions
carrying the same initialization UUID instead of choosing one.

The migration preserves existing single-part replay and attempt deadlines. Its
downgrade refuses to discard any multipart declaration or identity.

## Remaining R02 implementation

1. Connect durable initialization metadata and exact-version recovery to storage
   calls, retain completion intent and the exact completed version, and handle
   ambiguous create/complete responses without issuing a new accepted identity.
   Perform storage I/O outside ownership transactions and
   recheck authority afterward. Preserve upgrade/rollback fixtures.
2. Add bounded part-grant/completion RPCs and paired Go/Rust protocol validation.
   Avoid returning thousands of capabilities in one message. Bind part evidence
   to durable declarations and reject changed replay payloads or fenced owners.
3. Wire journaled Rust part delivery with configurable threshold/part sizing,
   bounded concurrency, source hashing, same-content retries, cancellation and
   lease/finalization deadlines. Extend object-size policy together with
   verification/download bounds; changing only the upload cap is insufficient.
4. Run a submitted job exceeding the old 64 MiB cap, retrieve its exact result,
   and inject lost responses, interrupted transfers, and stale completion.
   Connect abandoned-upload cleanup to R03 retention.

Each step is a verified slice of the same release feature. None is satisfied by
the low-level storage gate alone.

## Observed evidence

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
