# Job listing

Find submitted work through `GET /v1/jobs` or `dispatch jobs list`. All reads are
scoped to the authenticated token's project and require read permission (also
included in submit/operator roles).

```sh
dispatch jobs list --project research --state QUEUED --label cohort=alpha --json
dispatch jobs list --project research --state QUEUED --label cohort=alpha --cursor CURSOR --limit 100 --json
```

Each command returns one page. The JSON envelope contains `project`, `projectId`,
`jobs`, `hasMore`, and `nextCursor`; terminal pages have an empty cursor. Human
output includes job IDs, state, name, submitted priority, creation time, and a
continuation cursor when needed. Output failures exit 2.

## HTTP filters and cursors

Query parameters are optional `project`, `state`, `limit`, `cursor`, and repeated
`label=key=value`. The project defaults to the token's project; an explicit other
project returns 403. The default limit is 50, with values from 1–100. Labels split
at the first equals sign, so values can contain further equals signs. Percent
encode label values as usual. CLI `--label KEY=VALUE` handles URL encoding.

Unknown parameters, duplicate scalar parameters/label keys, empty explicit scalar
values, invalid filters, and malformed/mismatched cursors return 400
`INVALID_ARGUMENT`. Unauthenticated or revoked tokens return 401. Errors retain
the standard machine code, message, retryability, and request ID envelope.

Treat cursors as opaque. They bind the project UUID and canonical state/label
filter hash to a creation-time/UUID position. Filter order is irrelevant and the
limit can change between pages. Repeat the same filters on continuation. Cursor
validation rejects ambiguous or altered query identity; unsigned positions never
grant access, and the database independently applies authenticated project scope.

The encoded raw query, including percent escapes and cursor, is limited to
12 KiB before parsing, within the executable server's existing 16 KiB header
allowance. Individual label bounds match admission, but the aggregate URL budget
can limit combinations or heavily escaped long values. The client rejects
oversized queries before sending them. Cursors are limited to 512 bytes.

## Selection and continuation

`store.ListJobs` requires a canonical project UUID and a limit from 1–100. An
optional state selects one of the seven job states. Metadata label filters use
exact string equality, with all supplied pairs required. An empty value matches
an existing empty value, never a missing key. Label filters follow admission's
128-entry, 128-character identifier-key, and 8192-byte value bounds; NUL and
invalid UTF-8 are rejected.

Summaries contain job/project IDs, name, state, metadata labels, submitted priority,
and UTC creation time. They omit full specs, attempts, grants, results, and blocker
history. Priority is the submitted value, rather than its age-adjusted scheduler
priority. Empty results contain an empty jobs array.

Ordering is creation time descending, then UUID descending. Continuation uses
the last returned tuple, so tied timestamps remain traversable and deletion of
the anchor does not break the next page. Newer submissions appear on a fresh first
page. Project scope remains an independent SQL condition; a position grants no
access. Each page is a live database statement snapshot. Changes in state-filter
membership between requests can change what later pages contain; a scan is not
a frozen historical snapshot.

## Bounds and schema

Pages stop at either the requested row count or an 8 MiB encoded JSON budget,
reserving 1024 bytes for the public envelope and bounded cursor. Counting
uses standard Go JSON encoding, including HTML escaping. Admission bounds keep
one summary below 6.1 MiB even when every value byte needs six-byte escaping.
An unreturned row remains eligible for the next page. `HasMore` and an internal
`Next` position are supplied only when another matching row was observed.

The API checks the complete encoded envelope before writing headers. Only the
listing client opts into the 8 MiB response allowance; the generic allowance
remains 4 MiB. The client rejects incomplete/null/duplicate/unknown object fields,
invalid IDs/times/enums/labels/priority, mixed project scope, filter mismatches,
incorrect row ordering, and unchanged continuation cursors. It checks each returned page;
it does not establish a frozen snapshot across requests.

Migration `0019_job_listing` adds an index on
`jobs(project_id, created_at DESC, id DESC)` and removes only that index on
rollback. Its transactional build can block writes on populated installations;
it is not an online index build. Listing uses a read-only transaction with a
2-second lock timeout and a 4-second statement timeout. The index supports ordered
project traversal, but sparse state/label filtering can still scan many rows;
no throughput or latency benchmark is claimed.

Integration tests cover project isolation, combined filters, tied timestamps,
insertion/deletion during traversal, empty results, escaped wide-label byte
pagination, invalid inputs, and populated index rollback/reapply without changing
active authority, reservations, queue diagnostics, or scheduler state.
Public tests add authenticated HTTP/CLI traversal, filter-scoped cursor rejection,
revocation, a real response above 4 MiB, strict client decoding, CLI output errors,
and the query boundary through a net/http listener with a 16 KiB header limit.
