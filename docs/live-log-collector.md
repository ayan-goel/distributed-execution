# Live log collector (D15q)

`collect_capture` polls a Docker-output producer and the bounded log receiver
concurrently. It writes queued stdout and stderr chunks into private per-stream
segments, checks the two-second flush interval, then drains remaining chunks
after the producer ends. Its result carries the assembler and both final
sequence counts. A producer error marks capture incomplete, while a full queue
preserves missing sequence counts instead of blocking Docker's stdout pipe.

The attempt runner does not invoke this collector yet. The next slice must
start it after container launch, bound its shutdown and log delivery by lease
authority, and include its counters in the frozen completion claim.
