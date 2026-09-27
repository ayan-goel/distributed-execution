# Log segment catalog and read cursor (D15a–b)

`RegisterLogSegment` records a verified, immutable LOG object for one attempt and
one stream. The catalog supports an authorized HTTP cursor. Docker capture,
spool/upload integration, and CLI follow are not yet implemented. The
[binary segment format](log-format.md) is defined separately.

The worker declares a `stdout` or `stderr` LOG upload of 1 byte to 1 MiB,
transfers it to versioned object storage, and calls `FinalizeUpload` to verify the
exact object version and checksum. It then registers the artifact UUID with the
attempt authority, stream, inclusive sequence range, and any dropped ranges
inside that interval. Each stream starts at sequence 1 and its segments must
advance contiguously. Gaps are explicit; a segment containing only dropped
records cannot be registered. The payload format and worker capture timestamps
will be defined with the spool implementation.

The mTLS worker identity and current session must match the attempt. The store
checks fresh lease and phase authority, the verified artifact's exact attempt,
kind and logical name, and its size. The attempt lock serializes concurrent
registrations. A request UUID with identical content replays as accepted, even
after terminalization in the same session; changed content conflicts. Reusing
one verified artifact or skipping a sequence range also conflicts. Expired,
cancelled, and fenced attempts cannot add new segments.

Completion includes only registered log artifacts in its frozen manifest. It
rejects `logsComplete=true` while any LOG upload is unverified or unregistered,
or any registered segment reports a gap. Known registered gaps must appear in
the completion's gap claims, including when logs are marked incomplete.
The completeness flag remains the worker's claim about its full captured stream;
the catalog cannot infer bytes that never reached an upload declaration. Worker
backpressure, bounded loss reporting, and reconnectable CLI follow remain D15
work.

`GET /v1/attempts/{id}/logs?stream=stdout` (or `stderr`) requires project read
permission. It returns registered ranges, their internal gaps, server
registration time, exact object metadata, and 60-second download grants. The
optional `limit` is 1–100
(default 50). `nextCursor` encodes the attempt, stream, and last delivered
sequence; clients pass it back as `cursor` when polling, including after an
empty page. `hasMore` signals another immediately available page. Cross-project
attempts return 404, and a cursor for another attempt or stream is rejected.
The server checks token revocation again after signing and before returning
bearer grants. Object bytes remain binary; a future CLI must render them safely.
The nullable `completion` field repeats the frozen `logsComplete` claim and all
known gaps on every page, including empty follow polls. It exposes tail loss
beyond the last registered object. Completion is read before segments so a
page never pairs a newly visible completion with an earlier segment snapshot;
if completion commits during the read, the next poll sees it.
