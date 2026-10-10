# Job events

`GET /v1/jobs/JOB_UUID/events` reads durable state events using the project's
read or submit token. Invalid, unknown, and foreign job IDs return 404; missing
or invalid credentials return 401. Reads do not alter lifecycle or event history.

Query parameters:

| Parameter | Meaning |
| --- | --- |
| `limit` | Maximum rows, 1–100; default 50 |
| `cursor` | Opaque continuation returned by an earlier page |

The response is `{jobId,events,hasMore,nextCursor}`. Events are ordered by
increasing per-job `sequence`, with nullable `attemptId`, `type`, an object
`payload`, and UTC `createdAt`. Payload fields depend on event type. Submission,
assignment, phase changes, uploads, completion, loss, and cancellation use the
existing transactional event writers. Heartbeats are not a separate event stream.
Sequences order transitions even if timestamps tie or the database clock changes.

Responses contain at most 1 MiB of encoded JSON, including escaping and the
envelope, so wide payloads can return fewer rows than the requested limit. Events
are never split. `hasMore` means another row existed in that request's snapshot;
it does not predict future events. Each request uses one read-only snapshot.

`nextCursor` is always present, including on empty pages and at the current end
of history. Save it and request it again to poll for later events. An empty page
keeps the previous position. Page size may change when continuing. This endpoint
returns JSON pages; server-sent events are not implemented.

Cursors bind their version, project, job, and last observed sequence. They carry
position only; every query checks authenticated project ownership independently.
They remain usable when the last returned row is deleted. A cursor is not a
retention guarantee: deleted events cannot be recovered, and gaps may remain.

Malformed, empty, repeated, unknown, or out-of-range query fields and malformed
or wrong-scope cursors return 400 `INVALID_ARGUMENT`. Raw query strings are
limited to 2 KiB and cursors to 256 bytes. Database errors return retryable 503
without publishing a partial page. Callers must treat payload strings as untrusted
content when displaying them.

## Verification

Real HTTP/PostgreSQL tests read submission and cancellation events, resume after
an empty page, change page size, continue after anchor deletion, and reject
unauthorized reads. Large payloads that expand during JSON escaping verify the
encoded byte limit and complete traversal without skips or duplicates. Unit tests
cover scope, canonical encoding, integer overflow, and malformed queries. This
slice adds a read path; it does not add new worker transitions or a retention job.
