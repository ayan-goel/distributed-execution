# Bounded post-exit log publication (D15p)

After Docker reports an exit, the worker reads up to 1 MiB of combined stdout
and stderr. It gives each stream its own sequence numbers, seals binary
segments in the private attempt spool, and journals each upload identity before
transferring bytes. It records the exact object version, registers the segment
in the catalog, then removes the local copy. Completion claims use only
registered evidence; failed delivery or truncated capture marks logs incomplete.

This snapshot path remains available to the launch probe. The ordinary agent
now captures through a bounded Docker follower while the job runs and uploads
the resulting segments after exit. An incomplete flag without a numeric gap
means the Docker source could not establish how many bytes were omitted.
