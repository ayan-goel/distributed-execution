# Binary log segment format (D15d)

Each immutable LOG object is one stream from one attempt. Its uncompressed
`DSPLOG01` format has a 48-byte header:

| Offset | Bytes | Meaning |
| --- | ---: | --- |
| 0 | 8 | ASCII magic `DSPLOG01` |
| 8 | 36 | Canonical lowercase attempt UUID in ASCII |
| 44 | 1 | Stream: 1 stdout, 2 stderr |
| 45 | 3 | Reserved zero bytes |

Records follow without padding. Each contains an unsigned 64-bit sequence,
signed 64-bit worker capture time in Unix nanoseconds, unsigned 32-bit payload
length, then that many raw payload bytes. Numeric fields are big-endian. Payloads
are nonempty; the object is at most 1 MiB. Sequence numbers are positive,
strictly increasing, and at most the signed 64-bit maximum used by PostgreSQL.
They may skip values; the registered catalog range declares those gaps. Worker
wall-clock timestamps are evidence of capture time, not a total ordering across
stdout and stderr.

The Rust writer rejects invalid records and a record that would exceed the cap
without advancing its sequence state. The Go decoder rejects malformed headers,
truncated or oversized records, duplicate/decreasing sequences, and negative
timestamps. It returns payload bytes unchanged. A CLI must check object bytes
against the catalog's attempt, stream, range, gaps, size, and SHA-256 before
rendering, then escape terminal control characters by default. Object integrity
is also checked during upload verification.

Both implementations assert the same fixed binary vector with NUL, invalid
UTF-8, an ANSI escape sequence, two timestamps, and a sequence gap in
`rust/worker/tests/log_format.rs` and `internal/logformat/logformat_test.go`.
The writer and decoder are format boundaries; Docker capture, bounded spooling,
transfer, and CLI rendering remain separate D15 work. A future incompatible
format must use a new magic value.
