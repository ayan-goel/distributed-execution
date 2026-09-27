# Live Docker log handoff (D15i)

The Docker runtime now has a `follow_logs` operation that requests both streams
with `follow=true` and the full available tail. It passes raw bytes into a
nonblocking channel. Each nonempty chunk has one stream-specific sequence and
a worker capture timestamp. Frames larger than 64 KiB are split; 256 queued
chunks bound the payload queue to 16 MiB per follower.

When the channel is full, the runtime keeps draining Docker and increments
the stream sequence without retaining that chunk. The consumer can infer an
internal gap from the next delivered sequence. Shared final counters expose a
tail gap when no later chunk arrives. Tests cover saturation, recovery after a
drop, tail loss, binary bytes, frame splitting, and the queue limit.

The follower is a runtime primitive. Attempt supervision does not start it yet,
and no segment is spooled or uploaded from this queue. The consumer must drain
concurrently, flush segments, and merge known queue/spool loss into completion
evidence before the log path is usable.
