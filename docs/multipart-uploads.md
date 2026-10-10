# Multipart output transfers

Specification §13.3 requires multipart outputs above a configurable threshold.
The storage lifecycle is implemented and verified against the local versioned
backend. Durable coordination, worker RPCs, Rust delivery, larger object limits,
and the submitted-job demonstration remain required. The public upload path
still accepts one part and at most 64 MiB; R02 is not complete.

## Storage lifecycle

`internal/objectstore` provides:

| Operation | Contract |
| --- | --- |
| `BeginMultipart` | Check versioning and bounded size/part plan, request SHA-256 part checksums, return backend upload identity |
| `PresignPart` | Bind one key/upload/part number, exact length, and SHA-256 to a short-lived PUT capability |
| `CompleteMultipart` | Require consecutive ordered parts with ETags/checksums, check versioning, return the exact non-null version from storage |
| `AbortMultipart` | Abort only the supplied key/upload, then inspect at most one remaining part; repeated missing-upload responses establish absence at the time of the check |

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

Multipart completion alone does not verify or accept a result. Call `Verify`
against the returned exact version to check declared size and full-object
SHA-256, then pass through the existing fenced artifact/completion transactions.
Multipart ETags and composite part checksums are not full-object SHA-256 evidence.

In-flight part uploads can race abort. The coordinator must stop transfers and
retry cleanup after capabilities expire if needed. A single successful abort is
not a claim that no in-flight write can still finish remotely.

## Remaining R02 implementation

1. Persist immutable part plans and backend upload identity under the existing
   attempt-scoped declaration. Serialize concurrent initialization, retain
   completion intent and the exact completed version, and recover ambiguous
   create/complete responses without issuing a new accepted identity or guessing
   the latest object. Perform storage I/O outside ownership transactions and
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
