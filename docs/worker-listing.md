# Inspecting workers

List hosts authorized for your token's project:

```sh
bin/dispatch workers list
bin/dispatch workers list --limit 20 --json
bin/dispatch workers list --cursor PREVIOUS_NEXT_CURSOR --json
```

Read, submit, and operator tokens can list workers. Each command returns one
page; use `nextCursor` while `hasMore` is true. Success exits 0, including an empty
fleet. Argument, API, transport, and output errors exit 2. Listing changes no
worker or job state. See [worker maintenance](worker-administration.md) to drain.

## Reading the result

Each worker includes ID, name, recorded state, labels, drain intent, runtime health,
reconciliation status, disk pressure, and `lastHeartbeatAt`. A null heartbeat means
none has been recorded; human output prints `never`. Timestamps are UTC. No worker
credentials, certificates, session identifiers, or foreign job identities appear.

`capacity`, `reserved`, and `available` each contain `cpuMillis`, `memoryMiB`,
`scratchMiB`, and `slots`:

- Capacity is the host's current advertised capacity, initially its provisioned
  ceiling before registration.
- Reserved includes active **and quarantined** reservations across every project
  sharing the host. Cleanup must prove quarantined capacity reusable before release.
- Available is `max(0, capacity - reserved)` for each resource. Reservations can
  exceed reduced advertised capacity; they remain visible instead of being clipped.

These are recorded observations. `READY`, healthy flags, or available headroom do
not promise assignability: a heartbeat/session may be stale, or placement, project
quotas, credentials, capabilities, and drain policy may block a particular job.
Use the page's database `asOf` and each worker's heartbeat time to assess freshness.

## HTTP contract

`GET /v1/workers?limit=50&cursor=OPAQUE` returns 200 with
`{project,projectId,asOf,workers,hasMore,nextCursor}`. Scope always comes from the
authenticated token. The limit defaults to 50 and accepts 1–100. The optional
cursor is a bounded, canonical, project-bound position; it grants no access.
Unknown, duplicate, empty, malformed, or out-of-range query options return 400
`INVALID_ARGUMENT`. Raw queries are limited to 2 KiB and cursors to 256 bytes.
Missing, invalid, or revoked credentials receive 401.

Workers are ordered by UUID ascending. The final page has `hasMore=false` and an
empty cursor. Continuation compares the position's value, so removing the previous
worker or its membership does not strand later workers. Each page uses a read-only
repeatable snapshot with bounded database waits and no scheduler locks. Pages
across separate requests are live observations: membership and health may change.

Actual escaped JSON responses are bounded to 1 MiB. Large label sets can produce
fewer workers than the requested limit; the cursor never skips an unreturned row.
A stored row outside admission bounds that cannot fit fails with 503 instead of
returning an empty page that cannot advance.

## Verification boundary

PostgreSQL/race tests exercise assignment, lease expiry/quarantine, release,
reduced claims, shared-host reservations, foreign scope, large escaped labels,
and continuation after membership removal. Real CLI/HTTP/PostgreSQL checks cover
all project roles, empty fleets, pagination, credential-field exclusion, and drain
visibility. Client/CLI checks reject malformed evidence and propagate output errors.
These checks establish listing behavior; independent Linux-host execution and the
remaining v0.1 release gates are tracked in the [ledger](implementation.md).
