# HTTP API implementation

## Implemented endpoints (D05d)

| Endpoint | Permission | Behavior |
| --- | --- | --- |
| `GET /healthz` | none | Database readiness, no tenant data |
| `POST /v1/jobs` | submit | Validate, resolve image, atomically queue a job |
| `GET /v1/jobs/{id}` | read | Return project-scoped job state and accepted result metadata |
| `GET /v1/jobs/{id}/artifacts` | read | Accepted outputs with exact-version, 60-second download grants |

Send `Authorization: Bearer <project-token>`. Submission requires `Idempotency-Key`
(1–128 characters) and one bounded JSON/YAML Job document. New submissions return
201 with a job ID and Location header. Repeated identical submissions return 200
with the existing job; changed payloads return 409. The request hash is checked
before contacting the registry, allowing lost-response recovery during an outage.

Project mismatches return 403 for submission; looking up another project's UUID
returns 404. Read-only tokens cannot submit. Registry resolution happens outside
database transactions. Unavailable dependencies return retryable 503 responses
without leaking their internal errors or credentials.

Error responses have `error.code`, `error.message`, `error.retryable`, and
`error.requestId`. Every response also carries `X-Request-ID`. Request bodies are
limited to 1 MiB, request contexts to 15 seconds, and in-flight handlers to 256.
An initial global limit of 200 requests per one-second window bounds API pressure;
429 responses include Retry-After. This global cap does not claim per-project fairness.
Responses disable caching and MIME sniffing; TLS responses set HSTS.

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

Dataset admission currently returns an explicit 501 rather than queuing unresolved
inputs. Job listing, cancellation, full attempt history, logs, events, sweeps,
datasets, and worker administration remain required endpoints in later slices.

## Evidence

Race-enabled HTTP tests use real PostgreSQL and cover 100 concurrent duplicate
submissions, read/submit roles, cross-project access, registry-outage replay,
conflicting keys, oversized/malformed input, structured errors, and no partial jobs
after resolver failure. Resolver integration separately uses a real local registry.
The executable listener and CLI also have separate gates recorded in
[the implementation ledger](implementation.md). Handler tests alone do not prove
TLS deployment or a complete workload-to-download workflow.
