# Accepted artifact downloads (D11j)

`GET /v1/jobs/{id}/artifacts` returns accepted output metadata and short-lived,
version-pinned download capabilities. Read, submit, and operator tokens may use it
within their project. Missing or foreign jobs return 404; unauthenticated or revoked
tokens receive 401. Job IDs and logical names never grant access by themselves.

## Response contract

The response contains `jobId`, current `state`, nullable `acceptedAttemptId`, and an
`artifacts` array. Jobs without accepted success return an empty array, including
failed/cancelled jobs with diagnostic uploads. A successful job may also have no
outputs if its specification declares none. Each accepted output contains:

- `name` and `artifactId` identifying the verified logical output.
- `object`: exact `key`, `version`, `sizeBytes`, and lowercase `sha256`.
- `downloadUrl`, the signed `method` (GET), and `requiredHeaders` (header arrays).
- `expiresAt`, an RFC3339 UTC timestamp for the conservatively rounded expiry.

At most 64 outputs are returned, matching the job/completion contract. Grants last
60 seconds. The endpoint reuses the server's explicitly configured S3-compatible
adapter and does not require an AWS account. The API issues all grants before
writing any response so a signing failure cannot expose a partial success page.

Metadata comes only from the job's accepted immutable completion manifest. It does
not enumerate attempt uploads or accept caller-supplied object keys/versions. The
manifest holds the verified version even if the same key now has a newer object.
The storage signature binds the version query parameter; removing or changing that
parameter invalidates the grant. Missing or deleted accepted versions fail to
transfer rather than falling back to current bytes.

## Authorization and credential handling

URLs are bearer capabilities: do not log them, share them publicly, or store them
as permanent download locations. Fetch a fresh page after expiry. Project tokens
are sent only to Dispatch, never to the object-store URL. Static storage credentials
stay on the server. Download consumers must use the supplied method/headers, reject
redirects, and verify the expected version, length, and SHA-256 before accepting a
local file. The CLI command below performs those checks.

Authorization is checked before reading job metadata and again after signing. This
rejects a token revoked or a project disabled while waiting for signer capacity.
No database transaction is held during signing. Revocation cannot recall a URL
already returned; that capability lasts until its expiry. Responses carry
`Cache-Control: no-store` and the existing API security headers.

If accepted outputs exist but storage is unconfigured, the endpoint returns
503 `OBJECT_STORAGE_NOT_CONFIGURED` with `retryable=false`. Signing failures return
503 `OBJECT_STORAGE_UNAVAILABLE` with `retryable=true`, without backend diagnostics
or URLs. Empty artifact lists require no signer. Job inspection continues to expose
accepted metadata even when download signing is unavailable.

## Verification

The PostgreSQL/mTLS/HTTP tests check pending and failed outputs are absent, accepted
versions and sizes remain exact, grants expire within 60 seconds, read-only tokens
can download within their project, foreign jobs remain 404, and revoked tokens
cannot mint grants. A controlled signer revokes the token during signing, proves
revocation can commit without a held authorization lock, and verifies no capability
is returned. Injected signing errors remain redacted.

The combined fixture uses real SeaweedFS and PostgreSQL. It uploads through a scoped
worker grant, overwrites the same key with different bytes, verifies the original
version, completes the job over mTLS, and retrieves download metadata through the
HTTP server. The returned capability downloads the original accepted bytes and
version. Removing the signed version parameter is rejected by the storage backend.
Run it with:

```sh
sh scripts/test-objectstore.sh sh scripts/test-store.sh
```

Native tests, lint, protocol round-trips, and executable smoke checks also pass.
This establishes authenticated output retrieval through the HTTP boundary and CLI.
Rust transfer orchestration, multipart objects, and the complete
multi-host workload-to-download release gate remain separate required work.

## Verified local download client (D11k)

`client.DownloadArtifact` resolves a named output through the authenticated artifact
endpoint, then transfers it with a separate HTTP client. Metadata is bounded to
4 MiB and 64 entries. It validates job/accepted-attempt/artifact identities, unique
output names, object key/version/hash/size, GET method, expiry, and agreement between
the signed URL's key/version and accepted metadata. The current single-part profile
supports outputs through 64 MiB, including empty files. Multipart support remains
required for larger v0.1 outputs.

Storage traffic carries no project bearer token or cookies and ignores proxy
environment variables. HTTPS is required except explicit development mode on a
literal loopback IP; `localhost` HTTP is not accepted by this client profile. It
refuses redirects and automatic decompression, permits only matching Host and
bounded S3 signature headers, and caps response headers at 64 KiB. The transfer has
a two-minute total timeout and honors caller cancellation. Failed transfers require
a new call to obtain a fresh grant; no retry reuses an uncertain local file.

A destination must name a new file in an existing directory controlled by the
invoking user. The client opens that directory once, streams into a random 0600
partial file relative to it, checks the returned version, counts at most the expected
bytes plus one, and verifies SHA-256. Only after verification and file sync does it
publish through an atomic hard link that cannot overwrite an existing file or
symlink. It removes the temporary name and syncs the directory. Filesystems must
support hard links and directory sync; no unsafe overwrite fallback is provided.
A parent-directory rename cannot redirect publication into its replacement.

Normal integrity, transport, and cancellation failures clean up temporary data and
leave the destination absent. Existing or concurrently created destinations remain
untouched. If cleanup or directory sync fails after verified publication, the error
explicitly says that the verified output was already published. The client returns
a receipt with identities, destination, version, size, and hash, never the signed
URL or storage headers.

Client tests cover metadata rejection before storage access, wrong hashes/versions,
short/oversized and chunked bodies, empty files, redirects, cancellation, private
permissions, existing files/symlinks, competing destination creation, and replacement
of the parent directory during transfer. The combined PostgreSQL/SeaweedFS fixture
also uses this client to publish the originally accepted version to a local file.

## CLI command (D11l)

After a job has succeeded, download a named declared output to a new local file:

```sh
bin/dispatch artifacts download JOB_UUID result --output result.json
bin/dispatch artifacts download JOB_UUID metrics --output metrics.json --json
```

The output name must exist in that job's accepted manifest. `--output` is required;
the parent directory must already exist. The command never chooses local paths from
server metadata, overwrites a destination, or writes unverified bytes to stdout.
Use the existing `DISPATCH_URL` and `DISPATCH_TOKEN` settings. Development HTTP
requires `DISPATCH_DEV_INSECURE=1` and literal loopback IPs for both origins.

Success exits 0 and prints a quoted filename, byte count, and checksum. `--json`
prints one receipt with `jobId`, `attemptId`, `artifactId`, `name`, `path`, `version`,
`sizeBytes`, and `sha256`. Failures exit 2 with diagnostics on stderr and no success
receipt. If writing stdout fails after file publication, the verified file remains;
inspect it before retrying because the command will not overwrite it.

CLI tests cover JSON/text receipts, terminal-safe filenames, missing/extra arguments,
and corrupt transfer failure without publication. The combined real-storage gate
builds a fresh `dispatch` executable, downloads the accepted original version to a
local file, checks its JSON receipt and bytes, and verifies a second invocation
cannot overwrite that file.
