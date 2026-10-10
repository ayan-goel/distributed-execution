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

The internal `store.RetrySweep` operation creates a new sweep containing only
failed/cancelled children. All source children must be terminal, with at least
one failed/cancelled child. Successful jobs and the original history stay intact.
New children have fresh IDs, dense indices, zero attempts, and immediate parent
links protected by migration 0016. Frozen job specs, dataset bindings, resources,
priority, and sweep failure/concurrency policies are copied without resolving
image or dataset names again.

An arbitrary failed subset need not form a rectangular parameter matrix. The
stored retry spec preserves the original matrix description and adds a
server-owned `retry` selector containing the immediate source sweep and ordered
source job IDs. Its hash includes this selection; another retry records its
immediate parents while immutable links retain the earlier ancestry.

One transaction creates the sweep, jobs, input bindings, submission events, and
idempotency claim. Reusing the same project/source/key returns the same ordered
mapping even after project quotas or enablement change. Fresh retries check
current enablement and per-job CPU/memory quotas. Callers must authorize submit
access before invoking the store operation. The authenticated
`POST /v1/sweeps/{id}/retry` endpoint enforces this and accepts an empty body plus
an idempotency key; see [the API contract](http-api.md#sweep-retry). The Go client
validates the returned source, bounds, dense indices, and distinct parent/new
identities before the CLI prints the mapping.

## Retry failed or cancelled children

After every child reaches a terminal state, create a linked sweep for the
failed/cancelled subset while keeping successful results:

```sh
bin/dispatch sweep retry SWEEP_ID --idempotency-key retry-grid-1 --json
```

The CLI prints the recovery key to stderr before contacting the server. It
generates a key if omitted. After an uncertain reply, repeat the same source ID
and key to recover the same retry; changing the key creates another retry.
JSON output includes the new sweep ID, immediate source sweep ID, and ordered
new-job/parent-job mappings. Text output prints the new ID and child count.
Use the new ID with `sweep get` and `sweep export`, or as the source of another
retry after it finishes. There is no option to rerun successful jobs or change
the frozen job configuration. The source must contain at least one failed or
cancelled child, and the token needs submit permission.

## Verification

Race-enabled PostgreSQL tests cover CLI admission replay, concurrent submission,
ordered children, quota rollback, sweep capacity/backfill and terminal release,
both failure policies, retry preservation, reaping, session recovery, completion
replay, sibling event rollback, stop acknowledgement, and observed job-lock ordering.
Progress tests cover accepted-only numeric precision, byte/row pagination, and
cancellation between snapshot reads.

The real-stack 27-child test runs three enrolled native worker processes sharing
Docker Desktop, with a sweep cap of two. It checks same-key replay, successful
execution, released reservations, exact accepted metrics, CLI pagination,
matching JSON/CSV exports, and the downloaded metric artifact.

Live fail-fast tests accept a permanent application failure after another child
has reported RUNNING. The default policy lets that sibling finish and cancels 25
queued children; `cancelRunningOnFailure` stops it and cancels all 26 siblings.
Both cases lose the committed failure's reply and verify exact accepted replay,
one event per cancelled sibling, released reservations, exclusion of diagnostic
metrics from exports, and physical container cleanup before fixture teardown.

A worker-loss scenario kills a native worker while its designated child's Docker
container is still running. The production lease detector waits for the issued
lease to expire, then another worker accepts a fresh attempt. All 27 children
succeed with 28 attempts, one LOST record, and 27 accepted completions. Even with
both fail-fast flags enabled, this retry-eligible loss cancels no siblings. The
lost reservation stays quarantined; its container is removed by fixture teardown,
not claimed as reconciled by this test. Export and CLI download verify the
replacement's accepted metrics and immutable artifact.

A separate three-child scenario captures an actual successful worker completion
after both declared outputs are uploaded and verified. It kills that worker in
FINALIZING, waits for natural lease expiry, and lets another worker finish the
replacement. Replaying the exact old request through the authenticated mTLS
listener returns `ALREADY_TERMINAL/LOST` without a canonical manifest. The accepted
replacement manifest and completion history remain unchanged, and sweep export
retains the replacement's accepted ID and metrics.
The result file contains the attempt ID, so the CLI download proves it received
replacement bytes distinct from the old worker's still-verified diagnostic object.

A dataset-backed three-child scenario now verifies the separate sweep retry
command. Natural worker loss exhausts the first child's attempt policy and
fail-fast cancels the unstarted third child; a held real completion then lets
the second child succeed. `sweep retry` creates only two new linked jobs, and
same-key replay preserves their identities. Both finish across the remaining
workers. CLI downloads contain the frozen uploaded input and each fresh job and
attempt ID. Original sweep/jobs/attempts/events/completions and its export remain
unchanged; the lost worker's reservation remains quarantined.

Those local scenarios share one Docker daemon. The recovery and delayed-completion
gates now also pass with two independent Debian ARM64 VMs using strict scratch,
separate kernels, Docker daemons, quota filesystems, and enrolled identities.
The recovery gate requires accepted children from both workers before killing an
agent, then verifies all 27 results and the replacement's downloaded artifact.
See [independent-workers.md](independent-workers.md) for reproduction and boundaries.
Reconciliation after these worker kills and the broader fault matrix remain
separate gates; fixture teardown does not establish recovery of old capacity.
