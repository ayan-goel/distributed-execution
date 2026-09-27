# Private bounded log spool (D15e)

The Rust `LogSpool` stores sealed binary segments under the attempt workspace's
private `logs/` child. Docker mounts only the workspace's `inputs/`, `outputs/`,
and `scratch/` children, so the workload cannot reach the spool. The directory
is owned by the worker with mode 0700; immutable segment files use mode 0600,
no symlinks, and one link after publication.

The production ceiling is 256 MiB and 1024 retained segments per attempt.
`store` returns `Saturated` without writing when either cap is reached. It
writes one segment to a private pending file, syncs its bytes, publishes with a
non-overwriting hard link, removes the pending alias, then syncs the directory.
Only fully published objects count against the cap. A failed publication
poisons the spool rather than guessing which bytes are durable. It never
deletes a preexisting final object or pending alias on collision.

The caller may open a tracked segment for upload and remove it after the server
has verified and registered that exact object. Removal frees both bytes and a
segment slot. Saturation is a signal to the future Docker capture loop: it must
continue draining output and record dropped sequence ranges. This module does
not yet consume Docker frames, upload files, or journal transfer identities.
After agent restart, existing abandoned attempt workspaces are cleaned by the
worker recovery path; unuploaded bytes may be lost, as the spec permits.

Filesystem tests cover the cap, 1024-file limit, modes, exact bytes, identity
binding, range collision, safe removal, symlink rejection, and publication
failure cleanup. The [binary format](log-format.md) and [catalog](log-catalog.md)
remain separate trust boundaries.
