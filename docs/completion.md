# Completion identity and publication

## Payload contract (D11f)

`store.CompletionRequest` carries the complete attempt authority, a completion UUID,
claimed payload SHA-256, optional exit status, failure reason, confirmed-stop flag,
verified output references, log completeness/gaps, and optional original metrics
JSON bytes. The transaction and RPC integration are subsequent slices.

Success uses an empty failure reason, exit status zero, and a confirmed stop.
Application failure requires a nonzero exit status. Other worker reasons are
`RUNTIME_UNAVAILABLE`, `TRANSFER_FAILED`, `OOM`, `STARTUP_TIMEOUT`,
`EXECUTION_TIMEOUT`, `OUTPUT_INVALID`, `USER_CANCELLED`, and `FINALIZATION_TIMEOUT`.
`WORKER_LOST` is reserved for the server's fencing/recovery path. Transaction-time
rules must additionally validate phase, cancellation, and recorded exit evidence.

Output references have unique declared names and unique artifact UUIDs, with at
most 64 entries. Log gaps use `stdout` or `stderr`, inclusive positive int64 sequence
ranges, and at most 1024 entries. Overlap within a stream is invalid. A complete-log
claim cannot contain gaps; incomplete logs may have no known ranges when loss is
unquantified. These claims are metadata, not proof that segments were uploaded.

## Digest encoding

`CompletionDigest` validates and normalizes the payload, then computes lowercase
SHA-256 over `dispatch.worker.v1.CompleteAttempt\n` followed by compact UTF-8 JSON.
The prefix ends with a literal newline. Completion UUID and claimed digest are
excluded from the payload but the UUID must still be valid.

JSON field order is fixed:

1. `authority`: `jobId`, `attemptId`, `generation`, `workerId`, `sessionId`.
2. `exitCode` (integer or null), `reason`, `stopped`.
3. `outputs`, sorted by name, each containing `name`, `artifactId`.
4. `logsComplete`, then `gaps`, sorted by stream/first sequence, each containing
   `stream`, `first`, `last`.
5. `metrics`, with lexically sorted keys, then `metricsSourceSha256`.

Empty lists serialize as `[]`; absent metrics serialize as `{}` with an empty source
hash. The source hash binds the exact original metrics JSON bytes, including
whitespace, so publication can match it to a verified metrics output. Other values
use Go `encoding/json` compact encoding. Numbers retain their validated JSON text,
except all numeric zero representations become `0`. The fixed digest test in
`internal/store/completion_contract_test.go` is a cross-language implementation vector.
Rust completion support must reproduce this before sending live requests.

## Bounded metrics

Metrics must be one UTF-8 JSON object of at most 64 KiB and 256 unique names. Names
match `[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}`. Values are JSON numeric scalars within
the finite float64 export range; nonzero underflow is rejected. Numeric text is
bounded to 1024 bytes. Duplicate keys, nested values, booleans, strings, nulls,
trailing documents, overflow, and invalid UTF-8 are rejected.

Validated number text preserves exact large integers in storage instead of silently
rounding them through float64. True zero is normalized, including extreme exponent
spellings, to keep downstream storage bounded. The original source bytes remain
bound by their checksum. Publication must require their corresponding verified
artifact rather than accepting an unrelated metrics claim.

## Verification

Unit tests cover reordered sets, changed evidence, a fixed digest vector, malformed
identifiers, conflicting log claims/ranges, failure evidence, metric count/byte limits,
duplicate fields, overflow/underflow, and bounded zero normalization. These are
payload checks; they do not prove ownership, verified outputs, or publication.
