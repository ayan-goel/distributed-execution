# Live log collector (D15q)

`collect_capture` polls a Docker-output producer and the bounded log receiver
concurrently. It writes queued stdout and stderr chunks into private per-stream
segments, checks the two-second flush interval, then drains remaining chunks
after the producer ends. Its result carries the assembler and both final
sequence counts. A producer error marks capture incomplete, while a full queue
preserves missing sequence counts instead of blocking Docker's stdout pipe.

The Docker runtime now completes an attach handshake before starting the
container, then launches this collector. The attempt runner joins it through
exit and uses its counters in the frozen completion claim. A recovered
container that was already running cannot prove its earlier Docker history, so
its logs remain incomplete. The worker still uploads segments during
finalization, so the CLI cannot follow them live. The assembler is shared for
short, local-only locks: a future publisher can inspect sealed segments while
capture continues without holding the lock during transfers.
