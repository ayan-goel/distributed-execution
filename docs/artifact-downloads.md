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
local file. The dedicated CLI transfer command remains to implement.

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
This establishes authenticated output retrieval at the HTTP boundary. Rust transfer
orchestration, the artifact download CLI, multipart objects, and the complete
multi-host workload-to-download release gate remain separate required work.
