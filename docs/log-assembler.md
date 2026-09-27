# Segment assembly and spool loss (D15j)

`LogAssembler` consumes bounded, raw queue chunks and writes separate stdout
and stderr `DSPLOG01` segments. It flushes on the 1 MiB format limit, the
1024-gap format limit, an explicit final flush, or when its caller checks the
two-second flush interval. Each sealed segment is checksummed before the
private spool publishes it.

The pending queue owns each spool entry until the caller confirms registration.
`read_front` opens only a verified private file; `acknowledge_front` removes it
after delivery. A failed removal retains the pending identity and poisons the
spool, so the worker cannot silently claim that local space was freed. This
queue stays bounded by the spool's byte and file caps.

When the spool is full, the assembler discards that candidate but does not
advance the last stored sequence. The next stored segment begins at the
missing range and records it as a gap. If no later segment is stored, the
capture follower's final per-stream sequence counters reveal the lost tail.
Tests cover queue plus spool loss, independent streams, and ack cleanup.

This component is not yet attached to the attempt runner. Completion gap
aggregation, delivery scheduling, and Docker follower lifecycle are still
required.
