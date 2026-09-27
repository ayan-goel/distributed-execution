# Log segment catalog (D15a)

`RegisterLogSegment` records a verified, immutable LOG object for one attempt and
one stream. This is the durable metadata boundary for future log retrieval; the
worker spool, record encoding, HTTP cursor, and CLI follow command are not yet
implemented.

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
backpressure, bounded loss reporting, public access control, and reconnectable
stream cursors remain D15 work.
