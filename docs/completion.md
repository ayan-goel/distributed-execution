# Completion identity and publication

## Payload contract (D11f)

`store.CompletionRequest` carries the complete attempt authority, a completion UUID,
claimed payload SHA-256, optional exit status, failure reason, confirmed-stop flag,
verified output references, log completeness/gaps, and optional original metrics
JSON bytes. The transaction and authenticated worker RPC are implemented below.

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
The Rust `control::completion_digest` helper reproduces this contract and is checked
against Go by the cross-language fixture below. Completion delivery/journaling remain
separate integration work.

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
payload checks; publication has the additional real-database gate below.

## Atomic publication (D11g)

`store.CompleteAttempt` validates the supplied digest and authenticated worker, then
takes the cluster transition lock, credential lock, job/attempt locks, and worker/
project accounting locks in the established order. It checks the current session,
ownership, cancellation, phase, and fresh database time. No external I/O occurs.

For success the attempt must be FINALIZING with the previously recorded zero exit
status and a confirmed stop. Required declared outputs must all reference verified
artifacts belonging to this attempt, with matching names and allowed sizes. Every
provided optional/partial output is checked too. Failure may omit required outputs,
but cannot attach unverified or foreign data. Completion cannot change recorded exit
evidence; application/OOM/output/finalization failures require FINALIZING, while
startup/execution timeout reasons are restricted to their relevant phases.

Metrics bytes require a declared, verified output named `metrics`. Its exact size
and SHA-256 must match the original metrics JSON; a referenced metrics output also
requires those bytes. Accepted values retain exact numeric text in both the frozen
manifest and PostgreSQL JSONB. The original artifact is retained by a foreign key.

All verified LOG artifacts are included in the frozen manifest. A complete-log claim
is rejected if any declared LOG upload remains unverified. Completeness otherwise
remains the authenticated worker's claim: the log spool and segment sequence catalog
are still pending. Known gaps and unquantified incompleteness remain explicit in the
manifest instead of being silently labeled complete.

Migration 0011 adds immutable completion records and artifact references. A completion
is unique per attempt, and its UUID is unique per worker/session. Artifact foreign
keys bind the exact attempt/name/kind and verified object. Deferred constraints bind
the completion's terminal state to its attempt, and require a successful job's
canonical JSONB manifest to equal the successful completion's immutable manifest.

The manifest is versioned JSON, at most 2 MiB, containing the complete normalized
job specification/hash, project/authority, terminal evidence, exact output/log object
versions, metrics/source identity, log gaps, and attempt history. Its byte representation
is stored for stable response replay. Failure/cancellation manifests are retained as
attempt diagnostics; only successful completion fills the job's canonical result.

The transaction writes the completion and protected references, then samples database
time again before terminal mutations. Expiry during a reference-lock wait rolls back
the entire transaction. The attempt state, job state/pointer/result, reservation,
completion identity, and one `ATTEMPT_COMPLETED` event commit together.

## Failure, capacity, and cancellation

A confirmed stop releases the reservation only with terminalization. Uncertain stop
on a failure sets `cleanup_pending` and retains a quarantined reservation. Existing
recovery/reconciliation must confirm cleanup before that capacity is reusable.

Worker-reported failures use the immutable job retry policy and existing bounded
deterministic backoff. Retryable failures enter `RETRY_WAIT` while the old attempt
remains terminal; retry delay is measured from the final publication-time check.
Nonretryable/exhausted failure produces a FAILED job with no canonical manifest.
Cancellation acknowledgements never retry.

Committed cancellation intent prevents success and unrelated failure publication.
A `USER_CANCELLED` acknowledgement can resolve that intent only with a confirmed
stop and live lease/phase. Otherwise the worker receives stop/fencing instructions;
expiry/reaping must resolve uncertain cleanup. The public cancellation endpoint is
still a separate slice. These tests exercise database intent ordering, not that API.

## Completion uncertainty and replay

After a successful commit, the same completion UUID/digest returns its stored terminal
outcome even after lease expiry or session replacement. This is a historical read,
not renewed execution authority. It still requires a live credential for the original
worker. Replays of failed/cancelled completions return their terminal state without
a canonical manifest; old failures cannot change a replacement attempt's ownership.

Changed payload or completion UUID for an already completed attempt conflicts, as
does reusing a worker/session completion UUID across attempts. A terminal attempt
without an accepted completion returns `ALREADY_TERMINAL`. Expired/current-owner
mismatches return fencing, and cancellation/phase expiry return stop intent.

## Publication verification

Real PostgreSQL tests cover 16 concurrent identical completions; required/unverified
outputs; digest and exit mismatches; both cancellation-intent ordering cases; stop
acknowledgement; retryable/nonretryable failure; released/quarantined capacity; pending
logs; exact metric source binding and integers above 2^53; event/manifest failure
rollback; and a confirmed reference-lock wait that outlives the lease.

Accepted replay is checked after expiry, session replacement, and replacement-attempt
acquisition; revoked credentials and cross-attempt UUID reuse fail. A populated
schema-ten-to-eleven upgrade preserves verified outputs and permits completion.
Store fixtures use the separately verified artifact boundary. The real Rust
workload-to-completion path remains to integrate. Public result metadata is now
available through [job inspection](http-api.md#accepted-result-inspection-d11i);
authorized artifact download links are described in [artifact downloads](artifact-downloads.md).

## Authenticated completion RPC (D11h)

`CompleteAttempt` binds the authority to the provisioned mTLS worker identity,
preserves optional exit status, and rejects unsigned values outside PostgreSQL's
int64 range before conversion. Output/gap counts and metrics bytes are bounded
before allocation; the store validates UUIDs, enums, evidence, and the digest before
opening a transaction. The unspecified failure reason represents the empty success
reason and therefore requires observed exit zero and a confirmed stop. Unknown
failure reasons and the server-only `WORKER_LOST` reason are invalid.

Replies carry `ACCEPTED` with SUCCEEDED, FAILED, or CANCELLED; `ALREADY_TERMINAL`
with a known terminal state; `STOP_REQUESTED` with an active state; or `FENCED`
with an active state or no state for an unknown authority tuple. Only accepted
SUCCEEDED responses contain the canonical manifest, as its original stored bytes.
Invalid internal decision/state/manifest combinations fail with
`Internal/INVALID_COMPLETION_RESULT` rather than acknowledging ambiguous success.

Changed accepted evidence returns `AlreadyExists/REQUEST_CONFLICT`. Malformed or
unverified evidence returns `InvalidArgument/INVALID_ARGUMENT`. Credential failures
remain unauthenticated, cross-worker claims are permission denied, and database
failures expose only the stable `DATABASE_UNAVAILABLE` reason. Completion performs
no storage calls and works with storage unconfigured once artifact verification is
durable. An object-store outage does not prevent completion or accepted replay.

Unit tests exercise optional presence, unknown enums, nil entries, int64 boundaries,
field limits, and malformed internal publication responses. The mTLS/PostgreSQL
tests cover concurrent identical calls, byte-identical durable replies, exact output
versions, missing outputs, spoofed authority, lease/phase expiry, cancellation
acknowledgement, credential revocation, and event-failure rollback with redacted
errors. A discarded acknowledgement is recovered on retry. Storage is deliberately
unavailable after initial artifact verification to detect accidental completion I/O.

## Rust payload parity (D11m)

The worker validates complete authority and completion identity, integer ranges,
failure/exit/stop evidence, output uniqueness/count, and log gap ranges/count before
constructing normalized JSON. Sorting uses separate output/gap views and leaves the
caller's durable evidence unchanged. The helper excludes completion ID and claimed
digest from the hash while still validating the completion UUID.

Metrics use the existing pinned `serde_json` crate's `raw_value` feature. A custom
map visitor rejects duplicate decoded keys, and raw numeric JSON retains exact large
integers and exponent spelling. Parsing through float64 only checks the agreed
finite export range; it never supplies serialized metric values. Nonzero underflow
is invalid, while true zero normalizes to `0`. The exact original metrics bytes
remain bound by a separate SHA-256. No dependency version or lockfile changed.

`make protocol-test` now has Go encode framed completion protobuf fixtures, Rust
decode and hash/reject each request, and Go compare against its own store contract.
Cases cover the fixed golden vector, empty and reordered data, counters above 2^53,
int64 limits, numeric precision/exponents/subnormals/zero, byte/count limits,
duplicate keys, malformed metrics, conflicting outputs/gaps, failure reasons, and
missing exit evidence. This establishes payload compatibility, not worker delivery,
artifact transfer, or completion-journal recovery.

## Rust completion RPC (D11n)

`ControlClient::complete_attempt` sends caller-owned evidence through the existing
mTLS channel with a five-second local and wire deadline. Before sending, it checks
the request's canonical digest against `payload_sha256`; it does not generate an
identity, repair a digest, or retry with changed evidence. Transport failures retain
the existing retryable classification. The caller must persist the request before
delivery and decide when to retry.

Acknowledgements validate decision/state combinations. Accepted state must match
the submitted reason: SUCCEEDED for success, CANCELLED for user cancellation, and
FAILED for other worker failures. An already-terminal response does not acknowledge
this completion. Only accepted success can contain a manifest; all other responses
must have empty manifest bytes.

Accepted manifests must be JSON objects bounded to 2 MiB. The client checks version,
authority, successful exit/outcome, confirmed cleanup, and the complete output
name/artifact-ID set against the request. It retains the original bytes and does
not deserialize metrics into floating-point values. Full manifest/schema and object
integrity validation remain server responsibilities. The checked manifest fields
reject duplicates, while unknown fields remain forward-compatible. Serde's alternate
positional-array struct encoding is explicitly rejected at the manifest root.

Unit tests cover malformed/oversized manifests, mismatched ownership and outputs,
digest mismatch, inconsistent decisions, and byte preservation. The Go integration
test runs the actual Rust `completion_probe` against PostgreSQL and the mTLS server.
It commits the first call and replaces its acknowledgement with `Unavailable`, then
requires unchanged retries to recover the stored result. Success, failure, and
cancellation each produce one completion/event and release one stopped reservation.
Changed evidence returns a conflict; a spoofed worker is denied before the handler;
an unknown attempt is fenced. No storage request occurs after artifact verification.

The probe is a protocol fixture, not a production delivery loop. Durable completion
journaling, recovery after process restart, output transfers, and connecting these
operations to the worker's execution loop remain required.
