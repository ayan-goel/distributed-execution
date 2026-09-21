# Worker output collection

`outputs::collect_outputs` collects declared files after the runtime confirms that
the container has exited. It is synchronous filesystem work: callers must use a
blocking task so hashing cannot stall heartbeat or lease renewal. The production
agent does not yet acquire and execute jobs; the real Docker execution fixture
connects this component to `launch::execute` after FINALIZING.

The collector uses the validated `ExecutionSpec`, which limits outputs to 64
distinct names and non-overlapping paths under `/outputs`. It does not enumerate
other workload files. Missing optional files are skipped; missing required files,
unsafe paths, and oversized files return explicit errors. A present optional file
must pass the same safety and size checks as a required file.

## Filesystem boundary

The prepared workspace and its ancestry belong to the trusted worker host. The
collector opens the workspace without following its final symlink and rechecks
ownership and write permissions. It then opens `outputs` and each declared path
component relative to held directory descriptors with `O_NOFOLLOW`. Intermediate
components must be directories; final files must be regular and have exactly one
hard link. FIFO opens are nonblocking so validation can reject them without waiting
for a writer. Symlinks, directories used as files, and other special files fail.

Each accepted file is read through its open descriptor in 64 KiB chunks, with one
extra byte allowed beyond the initial length to detect growth. Size, modification
time, and change time must match after hashing; the bytes read must equal the
observed length. The collector returns the logical name, byte count, SHA-256, and
rewound file handle. Renaming or replacing a path cannot redirect that handle to a
different inode.

An open file handle is not an immutable snapshot. A trusted host process can still
alter its contents. Uploads must bind the declared size and checksum, and server
verification must read the exact immutable object version before publication.
This component does not upload, register artifacts, authorize publication, or
release capacity. Hashing a failed job's file does not make that job successful.

## Current transfer limits

Collection enforces each declaration's byte limit and a configurable policy limit.
The current defaults and maximum policy values are 64 MiB per file and 8 GiB total,
matching the initial single-part storage path. Zero-byte files are valid. This is
an intermediate transfer limit; multipart support for larger declared artifacts
remains required for v0.1.

## Verification

Native and Linux tests cover exact hashes including empty files, required/optional
absence, declaration and policy limits, aggregate limits across files, ignored
undeclared files, path replacement after collection, and rejection of root/parent/
leaf symlinks, hard links, directories, and FIFOs.

The PostgreSQL/mTLS/Docker finalization tests run a real container that writes `abc`
to `/outputs/result` and exits 7. Both normal and uncertain RUNNING acknowledgement
cases verify the collected name, three-byte length, and known SHA-256 alongside
durable exit evidence and FINALIZING replay. They stop before artifact transfer.
