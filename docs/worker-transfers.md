# Rust artifact transfers

`ControlClient::create_upload`, `TransferClient::put`, and
`ControlClient::finalize_upload` implement the single-part worker data path:

1. Submit the exact artifact declaration under the current attempt authority.
2. Upload the already-open file with the returned signed capability.
3. Retain the returned immutable version, size, checksum, and object key.
4. Submit that exact object to server verification and retain its artifact ID.

The caller owns operation IDs and must persist evidence before retrying uncertain
operations. These methods do not generate IDs, automatically repeat PUTs, renew
authority, or complete an attempt. Upload intent/version journaling and production
execution integration remain required. Until those are connected, the actual
worker startup command still does not acquire work.

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
bytes and an existing FINALIZING assignment. Rust requests a grant over real mTLS,
uploads to isolated SeaweedFS, and calls finalization. The server commits then
drops the first create/finalize replies; identical retries and an additional
finalization replay leave one durable upload and one exact-version artifact.
This proves the transfer components, not crash-safe live execution integration.

Reqwest 0.13.5 is pinned with defaults disabled and streaming/rustls support enabled.
It is MIT OR Apache-2.0 licensed and declares Rust 1.85 compatibility, below this
project's pinned Rust 1.88 toolchain. Rustls 0.23.40 was already a transitive
dependency; it is now directly pinned to select the existing ring backend without
adding a second crypto provider. The lockfile records the HTTP/platform trust
dependencies. See the [client builder API](https://docs.rs/reqwest/0.13.5/reqwest/struct.ClientBuilder.html)
and [streaming body API](https://docs.rs/reqwest/0.13.5/reqwest/struct.Body.html).

Multipart, durable transfer intent/version recovery, and the agent's live
acquisition/execution/publication loop remain part of the v0.1 work.
