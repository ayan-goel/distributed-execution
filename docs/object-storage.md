# Versioned object storage

## Supported development profile (D11a)

`internal/objectstore` implements bounded single-part uploads, exact-version
verification, and scoped downloads. It uses the official AWS SDK for Go v2 S3 module
`v1.113.1`, core `v1.47.0`, and explicit credentials provider `v1.20.5`. The API is
S3-compatible; it does not require an AWS account. The
[durable upload declaration store](artifact-uploads.md) is verified separately.
Server/RPC integration, exact-version artifact records, and publication remain pending.

The verified local backend is SeaweedFS 4.47, pinned as:

```text
chrislusf/seaweedfs:4.47@sha256:ce9e796f1fe6f06968f4c04bdaf8f678dad9c8acdfef3d244133d71bfa6bf882
```

Run its isolated compatibility gate with:

```sh
sh scripts/test-objectstore.sh
```

`make integration` includes the same gate. It starts one disposable Docker container
with a 512-MiB data tmpfs, one CPU, a 1-GiB memory limit, and a 256-PID limit. Only S3
port 8333 is published, on a dynamically allocated host loopback port. Credentials
are generated for each test and passed explicitly to that local backend and the
test client. The script neither reads ambient AWS credentials nor uses cloud APIs.
Cleanup removes only its own container and its ephemeral data.

This is Docker Desktop arm64 compatibility evidence, not a production storage
durability, backup, replication, or independent-host claim. Other S3 backends must
pass the same version/signature/integrity checks before they are supported.

## Configuration and request bounds

`New(Config)` requires an explicit endpoint origin, region, bucket, access key, and
secret key. A session token is optional. There is no default credentials chain,
instance metadata lookup, or inferred cloud endpoint. Static credentials remain on
the control plane. Workers will receive only scoped transfer grants.

Endpoints use HTTPS. Plain HTTP requires `AllowLoopbackHTTP` and a loopback IP or
`localhost`; URL userinfo, path prefixes, query strings, and fragments are refused.
The profile uses path-style bucket addressing, lowercase DNS-style bucket names
without dots, and opaque object keys made of slash-separated ASCII letters, digits,
hyphens, and underscores. Keys are at most 1024 bytes. Logical output names and
filesystem paths belong in artifact metadata, not unvalidated storage keys.

The SDK transport stays on the configured origin, refuses redirects, ignores proxy
environment variables, and bounds response headers and XML/error bodies to 64 KiB.
Object bodies are bounded by the configured object cap plus one byte. All operations,
including waiting for a concurrency slot, have a 30-second maximum and honor earlier
caller deadlines. SDK automatic retries are disabled so the orchestration layer can
retry within current attempt/finalization authority.

`MaxObjectBytes` defaults to 64 MiB and can be lowered. The current single-part
implementation rejects larger limits and objects; multipart support remains required
for the complete v0.1 release. `MaxConcurrent` defaults to four and permits 1–64
operations. Verification hashes streamed bytes rather than retaining entire objects
in memory. SDK errors cross the boundary as stable categories, without URLs or
credential identifiers. Do not log configuration structs or capability URLs;
normal string formatting is redacted, but fields remain available to authorized
configuration/transfer code.

## Transfer and integrity contract

`CheckVersioning` requires bucket status `Enabled`. `PresignUpload` repeats that
check before issuing a capability and binds the exact key, declared length, and
SHA-256 checksum. Grant lifetimes are whole seconds from one second through five
minutes. Reported expiry is conservatively rounded to the signature's second
precision. Clients must send the returned method and required headers with the
exact declared body length. SHA-256 input uses lowercase hexadecimal; the S3 checksum
is signed in the protocol's base64 representation.

A presigned URL is reusable until expiry. Repeated uploads may create newer versions
of the same key. Neither its expiry nor bucket versioning makes the upload write-once.
Pending artifact metadata must retain its unique upload identity; accepted references
must pin the exact verified version.

`Verify(Object)` requires a nonempty, non-`null` opaque version identifier and checks
that the response names that exact version and byte count. It computes SHA-256 over
the streamed version bytes and compares the result with the declaration. ETag and
uploader-controlled metadata are not accepted as proof. Missing/deleted versions
fail; verification never falls back to the key's latest version.

`PresignDownload` signs the exact version identifier. Its caller must first authorize
the project/attempt and resolve a verified reference from durable metadata. The
storage adapter itself neither authorizes an attempt nor accepts a job result.

Storage I/O must occur outside PostgreSQL state transactions. Subsequent artifact
verification/publication transactions must recheck current ownership, lease, phase,
cancellation, and declared-output requirements before attaching this immutable
version. A successful upload or hash check alone cannot publish a stale result.

## Verification evidence

Unit tests exercise configuration and input bounds, required versioning checks,
signed size/checksum/key and exact-version downloads, incorrect bytes/versions/sizes,
misleading ETags, redirect refusal, cancellation, and bounded concurrent verification.

The real backend gate starts with an unversioned bucket and requires rejection, then
enables versioning. It uploads and verifies content, reuses its grant, overwrites
the same key, and checks the original version remains unchanged and downloadable.
Empty files and files at the configured byte limit are uploaded and verified too.
Tampered checksum, byte count, key, and expired upload grants are rejected. Verification
rejects an incorrect version's content and a specifically deleted original version.
Suspending versioning prevents new upload grants. These tests establish storage
compatibility; pending records, worker transfer plumbing, fenced publication,
multipart uploads, retention, and terminal completion remain pending.

## Primary references

- [SeaweedFS 4.47 release](https://github.com/seaweedfs/seaweedfs/releases/tag/4.47)
- [SeaweedFS mini setup and explicit credentials](https://github.com/seaweedfs/seaweedfs/wiki/Quick-Start-with-weed-mini)
- [SeaweedFS object versioning](https://github.com/seaweedfs/seaweedfs/wiki/S3-Object-Versioning)
- [AWS SDK v2 custom endpoints and path-style S3](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-endpoints.html)
- [S3 PutObject checksum and version fields](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
- [AWS SDK v2 checksum behavior](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/s3-checksums.html)

The checked-in implementation also follows the downloaded, pinned SDK's
`options.go`, `api_op_PutObject.go`, `api_op_GetObject.go`, and presigning API. The
real gate verifies the behavior required by this profile instead of inferring
compatibility from the backend's advertised S3 support.
