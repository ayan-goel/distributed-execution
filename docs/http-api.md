# HTTP API implementation

## Implemented endpoints (D05d)

| Endpoint | Permission | Behavior |
| --- | --- | --- |
| `GET /healthz` | none | Database readiness, no tenant data |
| `POST /v1/jobs` | submit | Validate, resolve image, atomically queue a job |
| `POST /v1/sweeps` | submit | Resolve a template and atomically queue up to 1,000 ordered children |
| `POST /v1/sweeps/{id}/retry` | submit | Create fresh linked jobs for a terminal sweep's failed/cancelled children |
| `GET /v1/sweeps/{id}` | read | Sweep-wide progress and a bounded ordered page of children/accepted metrics |
| `GET /v1/jobs/{id}` | read | Return project-scoped job state and accepted result metadata |
| `POST /v1/jobs/{id}/cancel` | submit | Idempotently record project-scoped cancellation intent |
| `GET /v1/jobs/{id}/artifacts` | read | Accepted outputs with exact-version, 60-second download grants |
| `GET /v1/attempts/{id}/logs` | read | Registered log ranges, frozen completion gaps, a stream cursor, and exact-version, 60-second download grants |
| `POST /v1/datasets/uploads` | submit | Reserve a project-owned dataset key and return a 60-second upload grant |
| `POST /v1/datasets/uploads/{id}/complete` | submit | Verify one exact object version and register the immutable dataset |

Send `Authorization: Bearer <project-token>`. Submission requires `Idempotency-Key`
(1–128 characters) and one bounded JSON/YAML Job document. New submissions return
201 with a job ID and Location header. Repeated identical submissions return 200
with the existing job; changed payloads return 409. The request hash is checked
before contacting the registry, allowing lost-response recovery during an outage.

Job documents accept optional `spec.priority` (integer 0–3, default 0). Sweep
children inherit `spec.jobTemplate.spec.priority`. Nonzero changes are part of the
request hash; changing priority with the same key returns 409. Invalid priorities
return 422 before creating jobs or keys. Priority orders jobs within an eligible
project, gains one level per 10 eligible waiting minutes up to 3, and never bypasses
admission limits. See the [priority contract](contracts.md#job-priority-d18b).

Project mismatches return 403 for submission; looking up another project's UUID
returns 404. Read-only tokens cannot submit. Registry resolution happens outside
database transactions. Unavailable dependencies return retryable 503 responses
without leaking their internal errors or credentials.

Error responses have `error.code`, `error.message`, `error.retryable`, and
`error.requestId`. Every response also carries `X-Request-ID`. Request bodies are
limited to 1 MiB (sweep retry requires an empty body), request contexts to 15
seconds, and in-flight handlers to 256.
An initial global limit of 200 requests per one-second window bounds API pressure;
429 responses include Retry-After. This global cap does not claim per-project fairness.
Responses disable caching and MIME sniffing; TLS responses set HSTS.

## Sweep admission

`POST /v1/sweeps` accepts a bounded JSON/YAML Sweep document with the full Job
document embedded at `spec.jobTemplate`, plus `matrix`, `maxConcurrent`, `failFast`,
and `cancelRunningOnFailure`. Both metadata projects must match the token's
project. The server rejects `jobTemplateFile`; clients must embed local templates.

Use a project-scoped `Idempotency-Key`. New requests return 201 and replays return
200 with the same sweep ID and ordered `childIds`, resolved `spec`, `specHash`,
`projectId`, `maxConcurrent`, and `createdAt`. Both include a Location header.
The request hash normalizes retry-reason order and map keys while preserving
matrix value order. Image resolution and project-owned dataset lookup happen
after replay detection, outside the bounded insertion transaction. Failed
validation or resolution creates no sweep or child jobs.

Sweeps have a separate idempotency namespace from jobs. Conflicts return 409,
oversized documents 413, invalid matrices or policies 422, and missing datasets
404. The same role checks, response headers, and dependency errors as job admission
apply. The CLI resolves local template references and uses this endpoint through
`dispatch sweep submit`.

## Sweep retry

`POST /v1/sweeps/{id}/retry` requires a submit token and `Idempotency-Key` of
1–128 visible ASCII characters without spaces. Send no body or query parameters;
even `{}` or a whitespace body returns 400. The only retry policy is to create
fresh jobs for failed/cancelled children once every source child is terminal.
A still-running or entirely successful source returns 409. Invalid, unknown,
or other-project source IDs return 404; read-only tokens return 403.

New retries return 201; replays return 200. Both return `id`, `projectId`,
`parentSweepId`, `specHash`, `maxConcurrent`, `createdAt`, and ordered `children`.
Each child has its new `id`, dense zero-based `index`, and immediate `parentJobId`.
The Location header points to `/v1/sweeps/{newId}`, which supports ordinary
inspection and accepted-results export. The new hash binds the selected parents;
it is not the original full-grid hash.

Keys are scoped to the authenticated project and source sweep. Reusing the key
after a lost response returns the exact saved identity/mapping, even after quotas
change. A new key creates another retry, subject to current project quotas.
Retrying a retry uses its ID as the source and preserves immediate parent links.
Neither fresh retries nor replay contact image resolution or object storage.
Original jobs, successful results, and history remain unchanged; new jobs copy
frozen execution specs, input bindings, priority, and sweep policies.

Authentication and submit authorization still run before replay. Disabled
projects and revoked tokens return 401; a durable store-level replay does not
bypass these HTTP checks. `Client.RetrySweep` validates the requested source,
canonical IDs, hash/time, bounds, present dense indices, unique mappings, and
disjoint new/parent identities. It makes one explicit request and returns API
errors without creating a new retry automatically. `dispatch sweep retry ID
--idempotency-key KEY --json` records the recovery key before admission and emits
the validated mapping. Without `--json`, it prints the new sweep ID and child count.

## Sweep inspection

`GET /v1/sweeps/{id}` returns `sweep` (identity, name, state, policy, spec hash,
creation time, and `progress`), `children`, `hasMore`, and `nextCursor`. Progress
counts cover the whole sweep: `total`, `queued`, `retryWait`, `active`, `cancelling`,
`succeeded`, `failed`, and `cancelled`. Aggregate state reflects logical job states;
terminal jobs can still have quarantined physical cleanup pending.

Each child has `id`, `index`, `state`, matrix `parameters`, nullable
`currentAttemptId`/`acceptedAttemptId`, and accepted successful `metrics`. Failed
attempt diagnostics are excluded. Metrics are JSON numbers with preserved decimal
precision. The immutable template is available from the submission response;
inspection does not repeat it on every page.

Use `limit=1..100` (default 50) and pass the previous `nextCursor` unchanged to
continue. Child JSON also has a 2 MiB limit, so wide matrices may return fewer
rows than requested. Children retain stable index order. Counts and children
share one snapshot per request; progress can advance between pages. `hasMore`
is false and `nextCursor` is empty at the end. Cursors bind to one sweep and carry
position only; project authorization is checked on every request.

Unknown/repeated query parameters and malformed, empty, out-of-range, or
wrong-sweep cursors return 400. Invalid/unknown/foreign sweep IDs return 404.
Read and submit tokens can inspect their project's sweeps. `dispatch sweep get`
uses this endpoint. `dispatch sweep export` follows its pages for CSV/JSON results.

## Accepted result inspection (D11i)

Job responses add `acceptedAttemptId` and `acceptedManifest`. Both are null until
a successful completion commits. After success they identify the accepted attempt
and its immutable result manifest, including the pinned specification, exact output
versions, metrics, log completeness/gaps, and attempt history. Failed/cancelled
attempt diagnostics and unfinished uploads are never exposed as canonical results.

The read follows the job's accepted-completion pointer within the same scoped SQL
query. It returns the completion's stored JSON bytes instead of reconstructing a
result from current uploads or converting metrics through floating point. Replaying
the original submission returns the same current job/result view as inspection,
without contacting the registry. Reading metadata requires neither object-store
access nor issuing download capabilities.

`dispatch jobs get JOB_ID --json` includes both fields; ordinary text output remains
the job ID and state. The Go client retains the manifest as raw JSON so integers
above 2^53 survive inspection and CLI serialization. The existing 4-MiB response
limit remains in place. [Artifact downloads](artifact-downloads.md) provide the
separate authorized output metadata and signed transfer links.

An integration test completes a verified output over mTLS, then reads it through
the real HTTP server and Go client. It checks successful and failed jobs, matching
submission replay, missing results before completion, and 404 for another project's
token. Client and CLI tests verify the accepted identity and exact metric text.

Jobs with inputs resolve registered dataset names within the authenticated
project and pin the resulting immutable dataset IDs in the submission
transaction. Missing or other-project names return 404; an identical
submission replay reads the committed job without resolving datasets again.
`POST /v1/datasets/uploads` accepts one JSON object with `requestId`
(UUID), `name`, `sizeBytes`, and lowercase hex `sha256`. The server allocates the
object key; callers cannot choose one. The response includes `uploadId`,
`objectKey`, `uploadUrl`, `method`, `requiredHeaders`, `expiresAt`, and `replayed`.
Send the exact declared bytes using the returned method and headers. Identical
requests replay with a fresh grant and HTTP 200; a new declaration returns 201,
and changed metadata for the same project/request ID returns 409. A failed
signer leaves the declaration replayable. This endpoint does not register a
dataset. Send `POST /v1/datasets/uploads/{id}/complete` with `version` (the
object store's upload response version ID) and a `manifest` containing
`format: "tar.v1"` and sorted file entries with `path`, `sizeBytes`, and
`sha256`. Completion streams and hashes that exact object version against the
original upload declaration, then freezes its name, version, and manifest.
It returns 201 for a new registration and 200 for an identical replay;
changed versions or manifests return 409. A corrupt version returns 422 and
leaves no dataset registration. Both endpoints require submit permission and
are scoped to the token's project. Archive entries are independently checked
against the manifest during worker staging.
The Go client has typed calls for both metadata endpoints, validates upload
grant scope, and transfers only bytes matching the declared size and SHA-256.
`dispatch dataset upload DIRECTORY --name NAME` now builds a deterministic
archive and completes registration through these endpoints. Its recovery
options reuse the request ID and exact uploaded version after an uncertain
response. A live CLI-to-Docker integration test verifies dataset-backed job
execution and output download; broader dataset fault coverage remains pending.
Job listing, events, and worker administration remain required. The
[job-listing database foundation](job-listing.md) is implemented; its public
HTTP and CLI interfaces are still pending.

## Evidence

Race-enabled HTTP tests use real PostgreSQL and cover 100 concurrent duplicate
submissions, read/submit roles, cross-project access, registry-outage replay,
conflicting keys, oversized/malformed input, structured errors, and no partial jobs
after resolver failure. Resolver integration separately uses a real local registry.
The executable listener and CLI also have separate gates recorded in
[the implementation ledger](implementation.md). Handler tests alone do not prove
TLS deployment or a complete workload-to-download workflow.
