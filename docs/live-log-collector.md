# Live log collector (D15q)

`collect_capture` polls a Docker-output producer and the bounded log receiver
concurrently. It writes queued stdout and stderr chunks into private per-stream
segments, checks the two-second flush interval, then drains remaining chunks
after the producer ends. Its result carries the assembler and both final
sequence counts. A producer error marks capture incomplete, while a full queue
preserves missing sequence counts instead of blocking Docker's stdout pipe.

The attempt runner now invokes this collector after launch, supervises it
through exit, and uses its counters in the frozen completion claim. It still
uploads segments during finalization, so the CLI cannot follow them live.
Starting the follower before the container runs remains necessary to rule out
an early Docker log-rotation gap. Until then, the agent reports incomplete
logs even if the follower ended cleanly and every observed chunk was registered.
