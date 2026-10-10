# Job listing foundation

The store now supports filtered, paginated job summaries. HTTP and CLI listing
remain to be connected; this foundation does not yet implement `dispatch jobs list`.

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
reserving 1024 bytes for the future public envelope and bounded cursor. Counting
uses standard Go JSON encoding, including HTML escaping. Admission bounds keep
one summary below 6.1 MiB even when every value byte needs six-byte escaping.
An unreturned row remains eligible for the next page. `HasMore` and an internal
`Next` position are supplied only when another matching row was observed.

The future HTTP/client listing path must honor this budget explicitly: the
existing generic client response limit is 4 MiB. No public cursor or response
allowance changes are included in this store slice.

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
