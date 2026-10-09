# Sweeps

A sweep expands a container job template across a parameter matrix. Matrix keys
are sorted, values retain their submitted order, and each child receives a stable
index. Admission creates the sweep, children, frozen dataset bindings, submission
events, and idempotency claim in one transaction. The limit is 1,000 children.

`POST /v1/sweeps` submits an embedded job template through the authenticated HTTP
API. Admission freezes its resolved image and project-owned datasets before
expansion. Identical idempotency-key retries return the same children even when
the image registry is unavailable. See [HTTP admission](http-api.md#sweep-admission).
CLI/HTTP progress inspection and CSV/JSON result export are available.

## CLI submission

```sh
bin/dispatch sweep validate schema/examples/sweep.yaml --json
bin/dispatch sweep submit schema/examples/sweep.yaml --idempotency-key masking-grid-1 --json
```

Validation is offline. Submission uses the standard `DISPATCH_URL` and
`DISPATCH_TOKEN` settings. Adapt the example's job template to an approved image
and register its named datasets before submitting it.

Local files accept either an embedded `spec.jobTemplate` or `spec.jobTemplateFile`,
never both. Relative paths resolve beside the sweep file; absolute paths are also
supported. Each document is parsed strictly and limited to 1 MiB. The resolved
request must also fit 1 MiB. Only the embedded template is transmitted.

The CLI records the idempotency key on stderr before sending the request. Reuse
that key and unchanged files after an uncertain response. JSON results stay on
stdout; the text form prints the sweep ID and number of children. The client
checks the resolved template, spec hash, child count, unique child IDs, and cap
before reporting success. Admission does not mean the children have finished.

## Progress and accepted metrics

`GET /v1/sweeps/{id}` reads summary counts and a child page from one
read-only database snapshot. It returns counts for queued, retrying, active,
cancelling, succeeded, failed, and cancelled children. Aggregate state remains
ACTIVE while any child is unfinished; after every child is terminal, failure
takes precedence over cancellation, followed by success. An untouched sweep is
QUEUED.

Children are ordered by immutable index and include matrix parameters, state,
current/accepted attempt IDs, and final metrics from the accepted successful
completion only. Failed-attempt diagnostic metrics never become canonical sweep
results. Numeric values retain their decimal precision.

Pages contain at most 100 children and 2 MiB of child JSON. Wide parameters may
shorten a page; continuation resumes after its last included index. Summary counts
cover the entire sweep. Separate page requests may observe later child states;
they retain stable membership and index order. Use `limit` and the returned
`nextCursor` for continuation; see [HTTP inspection](http-api.md#sweep-inspection).

The Go client exposes `GetSweep(ctx, id, cursor, limit)`. It checks summary counts,
ordered child identities, pagination, and accepted-only finite scalar metrics
before returning a typed page. Decimal metric tokens retain their precision.

Inspect one page from the terminal:

```sh
bin/dispatch sweep get SWEEP_ID
bin/dispatch sweep get SWEEP_ID --limit 100 --json
bin/dispatch sweep get SWEEP_ID --cursor PREVIOUS_NEXT_CURSOR --json
```

Text output shows sweep-wide counts, each returned child's parameters/metrics,
and a `nextCursor` when more children remain. JSON returns the full HTTP page.
The default limit is 50; use 1–100 and pass the previous cursor unchanged. The
command reads one page per invocation. Parameters are escaped for terminal safety.

## Result export

```sh
bin/dispatch sweep export SWEEP_ID > results.json
bin/dispatch sweep export SWEEP_ID --format csv > results.csv
```

Export follows all pages and verifies stable identity, ordered membership, and
completion of the traversal before writing stdout. JSON includes `sweepId`,
`projectId`, `specHash`, and every child with observed state, parameters, attempt
references, and accepted metrics. A failed later request returns an error without
writing an incomplete export. Collection has a 32 MiB JSON size budget; inspect
bounded pages directly when a result exceeds this limit. This measures encoded
result size, rather than exact heap usage or final CSV size.

CSV columns are `index`, `jobId`, `state`, `acceptedAttemptId`, `parameters`, then
sorted `metric.NAME` columns. Missing metrics are blank. `parameters` is a JSON
object inside a CSV cell, preserving exact strings and preventing spreadsheet
formula interpretation. Metric values retain their decimal text, though a
spreadsheet may coerce them when opening the file. CSV allows at most 256 distinct
metric columns across the sweep; use JSON for wider metric sets.

Pages can observe different points in time while jobs run, so an active export
can contain unfinished children. Export after the sweep is terminal for final
comparison. Failed-attempt diagnostics never enter canonical metric columns.

Declare an output named `metrics` to ingest numeric results automatically:

```yaml
outputs:
  - name: metrics
    path: /outputs/metrics.json
    required: true
    maxBytes: 65536
```

The logical name selects ingestion; another declared path under `/outputs` is
also supported. The worker reads at most 64 KiB from the collected file handle,
checks its size/checksum, and attaches the original bytes only after artifact
publication succeeds. Completion and uncertain retries bind those same bytes.
The payload must be an object of finite numeric scalars with at most 256 bounded
names. Duplicate names, nonnumeric values, nonfinite values, and nonzero numeric
underflow are rejected.

Invalid or oversized metric content fails the job with `OUTPUT_INVALID` after a
zero exit. When the source passes declared output-size/safety limits and storage
is available, the original remains in the attempt's diagnostic artifact catalog,
excluded from canonical metric references. A file exceeding its declared
`maxBytes` is rejected during collection before upload. Existing
execution failures retain precedence, and a later storage outage cannot turn
known invalid metrics into a transfer-retryable failure. An undeclared file is
never read just because its filename is `metrics.json`.

## Scheduling and failure policy

`maxConcurrent` counts child attempts from ASSIGNED through FINALIZING. The
scheduler checks it before reserving capacity. A blocked sweep does not prevent an
unrelated runnable job from acquiring work; a terminal attempt frees its sweep slot.
Quarantined host capacity still requires reconciliation before reuse.

`failFast: true` cancels QUEUED and RETRY_WAIT siblings when a child becomes
permanently FAILED. A failed attempt that schedules another retry does not trigger
the policy. Failure handling covers completion, lease expiry, and session recovery.
Sibling cancellations and the triggering failure commit together, preventing
acquisition of another queued child between those changes.

Running siblings continue by default. With `cancelRunningOnFailure: true`, ACTIVE
siblings become CANCELLING and receive STOP_REQUESTED on lease renewal. Their
reservations remain active until physical stop is acknowledged, or quarantined
after fencing. Already terminal or cancelling children retain their outcomes.

Each affected sibling records one `SWEEP_FAIL_FAST` event containing the sweep ID,
failed job ID, and previous/new states. Accepted completion replay does not repeat
these events. Failure paths lock all affected jobs in UUID order before attempts
so independent batch renewals cannot invert the lock order.

## Verification

Race-enabled PostgreSQL tests cover CLI admission replay, concurrent submission,
ordered children, quota rollback, sweep capacity/backfill and terminal release,
both failure policies, retry preservation, reaping, session recovery, completion
replay, sibling event rollback, stop acknowledgement, and observed job-lock ordering.
Progress tests cover accepted-only numeric precision, byte/row pagination, and
cancellation between snapshot reads. Real multi-host sweep execution remains open.

The real-stack 27-child test runs three enrolled native worker processes sharing
Docker Desktop, with a sweep cap of two. It checks same-key replay, successful
execution, released reservations, exact accepted metrics, CLI pagination,
matching JSON/CSV exports, and the downloaded metric artifact. Independent Linux
hosts and live sweep failure demonstrations remain separate release gates.
