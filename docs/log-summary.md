# Completion log summary (D15k)

The worker now has a conservative log-summary builder for completion. It
collects gaps from registered segments, then marks every captured sequence
after the last registered range as an unregistered tail gap. It coalesces
adjacent gaps, keeps stdout/stderr independent, and rejects a claim requiring
more than the completion contract's 1024 gaps instead of hiding known loss.

`logsComplete` is true only when capture reached a clean end, every declared
segment is registered, and no gap remains. A stopped container by itself is
not proof of complete logs. The summary consumes journal evidence and final
per-stream capture counters; it does not infer bytes lost before the follower
attached or after a host crash.

The attempt runner does not invoke this builder yet. It must pass a trustworthy
capture-finished flag and the final counters into completion sealing.
