# Bounded post-exit log publication (D15p)

After Docker reports an exit, the worker reads up to 1 MiB of combined stdout
and stderr. It gives each stream its own sequence numbers, seals binary
segments in the private attempt spool, and journals each upload identity before
transferring bytes. It records the exact object version, registers the segment
in the catalog, then removes the local copy. Completion claims use only
registered evidence; failed delivery or truncated capture marks logs incomplete.

This path makes short completed jobs readable through `dispatch logs JOB_ID`.
It does not stream logs while a job runs. The follower and bounded queue need
to be connected to the attempt runner before D15's live and noisy-job gates
can pass. An incomplete flag without a numeric gap means the post-exit Docker
snapshot could not establish how many bytes were omitted.
