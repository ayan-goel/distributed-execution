# Durable log transfer evidence (D15g)

The worker journal stores each stdout/stderr segment as a separate transfer.
Its identity is the attempt, stream, and first sequence. A matching retry
returns the original `CreateUpload` request; changed bytes, range, gaps, size,
or checksum conflict. New ranges must follow a registered predecessor, so the
server catalog never sees a skipped or out-of-order same-stream segment.

The journal records the upload ID and scoped object key, then the exact object
version observed after PUT, the `FinalizeUpload` request and verified reply,
and finally the `RegisterLogSegment` request and accepted result. These writes
precede each next RPC. An uncertain reply is resolved with the saved request
and version; it must not cause a second PUT under a new version. Signed upload
URLs and headers are never persisted. Recovery still needs a current worker
session and live attempt authority before sending anything.

The per-attempt record admits at most 1024 historical log transfers and each
declared segment is at most 1 MiB. This is a journal/catalog bound, separate
from the 256 MiB private spool cap. A capture loop that reaches either limit
must continue draining the container stream and report known loss as gaps.

This slice stores retry evidence only. Docker streaming, transfer scheduling,
spool cleanup, and CLI rendering are still pending. Tests reopen the journal
between upload verification and registration, check exact retry IDs, reject
changed object versions and out-of-order ranges, and ensure signed URLs do not
appear on disk.
