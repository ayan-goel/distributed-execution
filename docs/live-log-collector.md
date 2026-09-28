# Live log collector (D15q–D15v)

`collect_capture` polls a Docker-output producer and the bounded log receiver
concurrently. It writes queued stdout and stderr chunks into private per-stream
segments, checks the two-second flush interval, then drains remaining chunks
after the producer ends. Its result carries the assembler and both final
sequence counts. A producer error marks capture incomplete, while a full queue
preserves missing sequence counts instead of blocking Docker's stdout pipe.

The Docker runtime now completes an attach handshake before starting the
container, then launches this collector. The runtime returns the running
collector task together with a handle to its shared assembler. The attempt
runner joins the task through exit and uses its counters in the frozen
completion claim. A recovered container cannot prove Docker output from before
attachment, so its logs remain incomplete. A separate publisher polls sealed
segments and registers their verified object versions while the attempt has
current lease authority. It locks the assembler only for local reads and
acknowledgements, never across storage or control-plane requests. When execution
ends, the worker stops the publisher. Finalization retries remaining segments
from its durable journal before freezing the completion log summary.
