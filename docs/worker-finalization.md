# Worker output finalization

`finalization::prepare_completion` connects an observed Docker exit to safe output
collection, journaled upload delivery, and a durable completion request. Call it
with the `FinalizingAttempt` returned by `launch::execute`, the same prepared
workspace and journal, and independently running lease renewal.

## Publication sequence

1. Match the workspace and durable assignment/exit evidence to the live attempt.
2. Collect declared outputs on a blocking I/O task, enforcing the collector's
   required-file, path, checksum, and size checks.
3. Persist each output declaration, obtain a scoped grant, PUT its verified bytes,
   and register the exact immutable version through `upload::deliver_output`.
4. Reference only acknowledged artifact IDs in the completion. Use the observed
   OOM flag for `OOM`, another nonzero exit for `APPLICATION_EXIT`, and otherwise
   the output-publication outcome. Persist the full request and digest before
   returning; zero exit alone does not establish success.
5. Call `completion::deliver_pending` to deliver that saved request and sync its
   reply. A lost reply retries the same completion; it does not collect or upload
   again. The server alone accepts the result and releases its reservation.

The coordinator currently sets `logs_complete=false` and supplies no metrics.
Segmented log delivery and verified metrics-file extraction are still required.
An existing durable completion is returned unchanged, never replaced by a new ID.

## Authority and failures

The local finalization deadline starts before the FINALIZING report and survives
the returned attempt handle. Collection, journal writes, uploads, transient RPC
retries, and their one-second backoff share that original budget and live lease
checks. New grants cannot restart it. Fencing, cancellation, lost renewal, or
expiry cancels further preparation. Storage or server operations already in flight
may have committed; durable identities and server fencing resolve that uncertainty.

Each output has at most three delivery rounds, including grant and artifact RPC
retries, with one second between transient failures. Retryable control errors,
transport/deadline failures, expired grants, and HTTP 408/429/5xx can retry while
authority remains live. A saved version still skips PUT on subsequent rounds.
Unknown versions and other HTTP failures stop that output without another PUT.

Missing, unreadable, unsafe, changing, or oversized declared files, and transfer
integrity failures, yield `OUTPUT_INVALID`. Exhausted transient delivery errors or
other storage failures yield `TRANSFER_FAILED`. These produce a durable stopped
failure completion with any previously acknowledged output references. Collection
validates all declared files before uploading, so a collection failure publishes
none. An observed OOM or application exit remains the primary failure reason;
a later storage outage cannot make an OOM eligible for transfer-failure retries.

Malformed grants, journal conflicts/corruption, invalid configuration, worker
authentication failures, and control-plane rejection remain coordinator errors.
They must not be converted into permission to publish a new completion. Expiry or
cancellation can still interrupt failure preparation before it is sealed.

Collection runs off the async executor so hashing cannot stall renewal. A cancelled
blocking read may finish, but performs no network or journal mutations. As with
other journal operations, a blocking write already started can finish after its
waiting future is cancelled. Preserve the journal and workspace until reconciliation.

Completion delivery is deliberately separate from live preparation. Acceptance
can make renewal report the attempt terminal before its completion acknowledgement
arrives. Replaying the saved terminal evidence remains valid after authority ends;
it grants no permission to launch, upload, or accept a stale result. The caller
owns bounded completion retries and must interpret the returned decision.

Cancellation supersession, phase-expiry recovery, metrics/log publication,
multipart files above 64 MiB, and local cleanup remain separate work.
No coordinator return value releases local capacity or deletes containers/files.

## Verified scope

The real Docker/PostgreSQL/mTLS/SeaweedFS fixture acquires a CPU job with a cached
image and no inputs, executes it, and publishes its three-byte output. Success and
nonzero-exit scenarios both lose committed phase, grant, artifact, and completion
replies. Completion retry follows an explicit renewal rejection after acceptance.
Assertions check exact request replay, durable acknowledgement, one upload/artifact/
completion/event, released reservation, and verified immutable storage content.
Only success has a canonical accepted manifest.

Additional Docker scenarios omit a required output, exceed its byte limit, or
exit nonzero without producing it. A storage-outage fixture permits grant issuance
but returns HTTP 503 for every PUT; the worker stops after three delivery rounds
(a lost grant reply and two failed PUTs). Failed completion replay releases the
reservation, records the expected reason, and creates no accepted result/artifact.

Unit fixtures check fencing before I/O, cancellation during pending finalization,
and a fixed phase deadline despite continuous fresh grants. These component paths
are exercised together by `launch_probe`; the production agent's acquisition,
staging, capacity management, and execution loop still need to call them.
