# HTTP API implementation

## Implemented endpoints (D05d)

| Endpoint | Permission | Behavior |
| --- | --- | --- |
| `GET /healthz` | none | Database readiness, no tenant data |
| `POST /v1/jobs` | submit | Validate, resolve image, atomically queue a job |
| `GET /v1/jobs/{id}` | read | Return a job only within the token's project |

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

Dataset admission currently returns an explicit 501 rather than queuing unresolved
inputs. Job listing, cancellation, attempt history, logs, artifacts, events, sweeps,
datasets, and worker administration remain required endpoints in later slices.

## Evidence

Race-enabled HTTP tests use real PostgreSQL and cover 100 concurrent duplicate
submissions, read/submit roles, cross-project access, registry-outage replay,
conflicting keys, oversized/malformed input, structured errors, and no partial jobs
after resolver failure. Resolver integration separately uses a real local registry.
The executable listener and CLI are the next slice; handler tests do not prove TLS
deployment or a complete executable workflow.
