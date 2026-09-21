# Rust artifact transfers

`ControlClient::create_upload`, `TransferClient::put`, and
`ControlClient::finalize_upload` implement the single-part worker data path:

1. Submit the exact artifact declaration under the current attempt authority.
2. Upload the already-open file with the returned signed capability.
3. Retain the returned immutable version, size, checksum, and object key.
4. Submit that exact object to server verification and retain its artifact ID.

The low-level methods do not generate IDs, automatically repeat PUTs, renew
authority, or complete an attempt. The journal and delivery path below now manage
durable output evidence. Production execution integration remains required; the
actual worker startup command still does not acquire work.

## Journaled output delivery

First call `AsyncJournal::prepare_output` with a declared output's name, size, and
hash. Then `upload::deliver_output` advances that saved declaration:

- Without a saved storage version, require an open source file, obtain a grant,
  sync its stable scope, upload, and sync the exact version/finalization request.
- With a saved finalization request, retry only that request. No local source or
  additional PUT is required, including after reopening the journal.
- With a saved artifact acknowledgement, return it without storage or control RPCs.

The reply is synced before delivery returns. Missing bytes before a storage version
is saved produce an explicit `MissingSource` error with no network mutation.
Network operations hold no journal lock. Callers own bounded retries, live
authority checks, and cancellation at lease/phase deadlines; this function does
not restore authority or run a background retry loop.

A PUT may commit without returning its version, or the process may die before that
version is synced. In those cases, the journal cannot recover unknown bytes or
select the latest object version. Retrying while still authorized needs the exact
source bytes and can leave an unreferenced version for later cleanup. Once a version
is saved, retrying never repeats PUT or selects another version.

## Control-plane validation

Create requests require canonical authority/request UUIDs, a positive bounded
generation, a valid logical name/kind, lowercase SHA-256, and one part of at most
64 MiB. The server additionally validates job declarations and live ownership.
Grant validation binds the key to the requested job, attempt, and returned upload
UUID within the server's project/job/attempt/upload path. It bounds URLs and headers
and rejects multipart responses. The worker cannot independently infer the project
UUID from this RPC's authority; the authenticated server owns that relationship.

Finalization requests require that same key scope and a nonempty, non-null version
of at most 1024 printable ASCII bytes. The reply must contain a canonical artifact
UUID and match the requested object in every field. Both RPCs use five-second local
and wire deadlines. A verified artifact is still subject to the final completion
transaction's authority, cancellation, and required-output checks.

## Storage channel

The HTTP client is independent of worker mTLS. It uses normal TLS certificate and
hostname verification for HTTPS and never receives the worker's client key.
Plain HTTP requires an explicit development option and a literal loopback IP.
Redirects, ambient proxies, automatic retries, response decompression, and referer
forwarding are disabled. URLs reject user information, fragments, control bytes,
and backslashes, and their paths must end in the exact scoped object key.

Required headers are limited to host, content length, content type, and the S3
checksum/content-hash fields. Host must match the URL; content length must match
the declaration. Case-equivalent duplicate headers and other headers, including
Authorization, are rejected. Signed capabilities and raw transport diagnostics
never appear in transfer errors or Debug output.

Each client and its clones share four transfer permits. Queue wait and HTTP work
have one budget bounded by both grant expiry and 30 seconds; connections have a
five-second timeout. HTTP/1 responses permit at most 64 headers, and response
bodies are not consumed. Callers must also cancel work at the attempt's local
authority/finalization deadline. Dropping a future stops local work but cannot
prove that an upload did not commit remotely.

The file is rewound and checked for regular-file type and exact declared length.
The body streams in chunks of at most 64 KiB while recomputing SHA-256. Before
releasing the final chunk, it checks EOF and the complete digest. Empty files take
an explicit integrity path because HTTP may never poll a zero-length body. A
successful response is insufficient unless local body verification completed and
exactly one valid `x-amz-version-id` was returned. ETags are never used as versions
or checksums. The server then reads and verifies the selected immutable version.

## Verification and dependencies

Loopback tests cover exact bytes, multiple chunks, empty files, changed/truncated/
grown files, expired grants, unsafe destinations/headers, redirects, stalled
responses, and missing/null/duplicate version headers.

The real versioned-storage integration suite invokes `upload_probe` with fixture
bytes and an existing FINALIZING assignment. It first checks that missing local
bytes fail before any upload RPC. With bytes available, the first committed grant
reply is lost; Rust retries its journaled declaration unchanged. After finalization
commits, the server withholds the reply while the parent kills the Rust process.
The test deletes the source, reopens the journal in another process, and recovers
the same artifact by retrying the exact saved request. A further reopen succeeds
with upload RPCs unavailable. Database counts and object-version listing require
one declaration, one artifact, and exactly one stored version.

This fixture seeds runtime observations and retains its existing server session to
isolate delivery recovery. It is not the production worker startup path: a full
worker restart registers a fresh incarnation and fences unfinished old attempts.
Recovered upload evidence alone must never authorize execution or publication.

Reqwest 0.13.5 is pinned with defaults disabled and streaming/rustls support enabled.
It is MIT OR Apache-2.0 licensed and declares Rust 1.85 compatibility, below this
project's pinned Rust 1.88 toolchain. Rustls 0.23.40 was already a transitive
dependency; it is now directly pinned to select the existing ring backend without
adding a second crypto provider. The lockfile records the HTTP/platform trust
dependencies. See the [client builder API](https://docs.rs/reqwest/0.13.5/reqwest/struct.ClientBuilder.html)
and [streaming body API](https://docs.rs/reqwest/0.13.5/reqwest/struct.Body.html).

Multipart, authority-aware live finalization, and the agent's acquisition/execution/
publication loop remain part of the v0.1 work.
