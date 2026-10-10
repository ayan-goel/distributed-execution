# Submission contracts

## Job parsing (D02a)

`internal/spec` accepts one JSON or YAML document of at most 1 MiB. The example
in `schema/examples/job.yaml` matches spec §4; its registry and dataset are placeholders.
The same typed representation is used by future HTTP admission and the CLI.

Unknown fields (including wrong capitalization), duplicate keys, nulls, YAML aliases,
non-string environment values, multiple documents, and nesting over 32 levels are
rejected. No implicit shell is added. The parser reserves `DISPATCH_` environment
names for execution identity. Project authorization and quota checks are admission
responsibilities; structural validation is not authorization.

Names/project/dataset identifiers have 1–128 ASCII letters, digits, dots, underscores,
or hyphens, beginning with a letter or digit. Environment names use shell identifier
syntax. Maps have at most 128 entries; values and individual arguments are at most
8192 bytes. Commands contain a nonempty executable and at most 256 combined arguments.

### Resource and deadline ceilings

These are defensive platform ceilings, not promised machine capacity. Actual
project quotas and eligible worker capacity can be much smaller.

| Field | Accepted range |
| --- | --- |
| Priority | integer 0–3; optional, defaults to 0 |
| CPU millis | 1–1,024,000 |
| Memory MiB | 1–16,777,216 |
| Scratch MiB | 1–1,073,741,824 |
| Startup/finalization | 1–3600 seconds each |
| Execution | 1–604800 seconds |
| Termination grace | 0–300 seconds |
| Attempts including initial | 1–10 |
| Initial/maximum backoff | 1–3600 seconds; maximum ≥ initial |
| Inputs/outputs | at most 64 each |
| Per-output maximum size | 1 byte–1 TiB |

Input paths must be clean descendants of `/inputs`; outputs must be clean descendants
of `/outputs`. Overlapping paths and duplicate output names fail validation. These
lexical checks complement, but do not replace, runtime symlink-safe file collection.
Only `disabled` workload networking is currently admitted. Omission defaults to
disabled. Other resource/time/retry fields are explicit rather than guessed.

### Job priority (D18b)

Set `spec.priority` in a Job document, including a sweep's embedded or local job
template. Larger effective priorities run first within the next eligible project
selected by round-robin. [Queue aging](acquisition.md#priority-aging-d18c) adds one
level per 10 eligible waiting minutes, capped at 3. Priority does not bypass project
access, quotas, placement, backoff, resource fit, or sweep concurrency, and does not
preempt running jobs.

Priority is stored in the frozen job specification and scheduler column. Sweep
children inherit their template's value; failed/cancelled-only retries copy their
source values. Nonzero priority changes request identity, so changing it under an
existing idempotency key conflicts. Omitted and explicit-zero priority normalize
to the same bytes and hash as existing default jobs.

Go and Rust validate the range. Use updated server and worker binaries together
for nonzero priorities: an older strict worker decoder rejects the new field.
Documents without it remain valid; this is not a rolling-upgrade compatibility claim.

### Job status diagnostics (D18f)

`GET /v1/jobs/{id}` adds a project-scoped `queueDiagnostics` object:

```json
{
  "asOf": "2026-10-10T12:00:00Z",
  "attemptCounter": 1,
  "observations": [{
    "sequence": 42,
    "workerId": "00000000-0000-0000-0000-000000000002",
    "sessionId": "00000000-0000-0000-0000-000000000003",
    "requestId": "00000000-0000-0000-0000-000000000004",
    "attemptCounter": 0,
    "reason": "NO_RESOURCE_FIT",
    "observedAt": "2026-10-10T11:58:00Z"
  }]
}
```

This is a historical worker-specific check before a later attempt; it does not
declare the current job blocked on every host. `observations` is always an array,
including `[]`, with at most 16 entries ordered by descending positive sequence.
Reason codes are PLACEMENT_MISMATCH, NO_RESOURCE_FIT, PROJECT_QUOTA,
SWEEP_CONCURRENCY, PROJECT_DISABLED, and RETRY_BACKOFF. Missing observations do not
prove absence of blockers. Clock rollback can put `observedAt` after `asOf`.

`dispatch jobs get ID` renders historical checks; `--json` preserves the object.
Mutation response shapes are unchanged. New clients accept older servers that omit
the field; a present field is strictly validated before display. See
[queue diagnostics](queue-diagnostics.md#public-job-status-d18f) for bounds, sampling,
counter semantics, authorization, and compatibility.

### Canonical identity

Validated jobs serialize to compact JSON with sorted map keys. Retry reasons are
treated as a set; their canonical order is sorted. Argument and input/output order
remain unchanged. SHA-256 of these bytes is the normalized request identity.
Admission must separately resolve image tags and datasets to immutable versions
before storing the execution specification and its final hash. A request hash alone
does not prove that an image or dataset was pinned.

The initial supported retry reasons are `WORKER_LOST`, `RUNTIME_UNAVAILABLE`, and
`TRANSFER_FAILED`. Application exit-code retry extensions are not yet admitted.

### Implementation references

- [Official YAML parser](https://pkg.go.dev/go.yaml.in/yaml/v3): parse into a bounded
  node tree, reject ambiguous syntax, then convert to strict typed JSON. This avoids
  YAML's automatic scalar-to-string coercion for environment parameters.
- [Go JSON encoding](https://pkg.go.dev/encoding/json#Marshal): map key ordering is
  deterministic. Exact field-name checks supplement the decoder's case folding.

## Sweep expansion (D02b)

The server contract embeds `spec.jobTemplate` as a complete Job, never a local path.
The CLI will resolve the spec's `jobTemplateFile` form on the client before sending
this request. Sweep/template projects must agree. Matrix keys override environment
values; sorted parameter names and original value order determine child indices.
Each child owns its maps and slices, so modifying one cannot change another.

Expansion accepts 1–32 dimensions, at most 1000 children, at most 16 MiB of expanded
canonical specifications, and a concurrency cap of 1–1000. Count multiplication is
checked before allocation. Empty dimensions and reserved environment names fail.
Repeated values remain separate children. `cancelRunningOnFailure` requires
`failFast`. This slice validates policy; execution-time enforcement belongs to D17.

## Image resolution (D05c)

Admission resolves approved image tags to repository@sha256 references using the
manifest bytes returned by an OCI/Docker registry. Supported media types are OCI
images/indexes and Docker schema-2 images/manifest lists. The computed digest must
match both the descriptor and any digest supplied by the caller. A mutable tag can
move later without altering an admitted job's pinned reference.

Resolution has a 10-second deadline and 4-MiB response limit. Registry, bearer-auth,
and redirect destinations are checked against operator configuration. Docker Hub's
registry/auth endpoints are allowed when its canonical `index.docker.io` registry is
approved. Other auth hosts require explicit configuration. Plain HTTP is allowed
only for explicitly enabled loopback development registries. Host Docker credentials
are not loaded implicitly; private registry provisioning remains an operator feature.

Tests include a real local registry, moving a tag between two manifests and fetching
the first pinned digest afterward. Unit cases reject wrong digests, unapproved hosts,
insecure requests, auth/redirect escapes, malformed manifests, and cancelled lookups.
Reference: [go-containerregistry remote API](https://pkg.go.dev/github.com/google/go-containerregistry/pkg/v1/remote).
