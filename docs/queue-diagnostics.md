# Queue diagnostics implementation

Dispatch must explain queued-job blockers, including large jobs bypassed by
backfill (spec §9.2), and expose a blocker summary on job status (spec §15).
These observations describe a specific worker's placement check at a specific time;
they cannot prove that no other worker can execute the job.

## Ordered slices

1. Bounded durable history: retain the newest 16 observations per job, with server
   time, reason, worker/session/request identity, and attempt counter. Enforce the
   row bound in PostgreSQL with a fixed-slot ring. Reads must be project-scoped.
   Verify constraints, replay, concurrent writes, rollback, and populated migration
   upgrade/rollback before committing this foundation.
2. Scheduler recording: sample blocked candidates during fresh ready-worker
   acquisitions, including jobs skipped by backfill. Bound each acquisition's
   writes and resampling frequency; replay and failed transactions must not append
   history. Preserve lock order, project rotation, resource limits, and lease time.
3. Visible status: expose a bounded historical summary through job GET and CLI,
   with observation age/context. Verify project isolation, strict client decoding,
   and real CLI-to-PostgreSQL behavior before committing the public interface.

Each slice runs focused PostgreSQL/race checks and relevant repository gates.
Scheduler integration also runs the combined real-worker/Docker/storage suite.
All three slices are implemented and verified locally.
Independent Linux-host validation remains a separate D18 acceptance gate.

## Bounds and interpretation

Sixteen observations are an initial diagnostic retention policy, not a permanent
audit trail. Slot replacement evicts the oldest retained observation. Sequence IDs
order observations despite wall-clock corrections and may contain gaps after
rollback. Diagnostics grant no execution authority and never change frozen specs,
base priority, ownership, reservations, or project fairness.

Recorder-level replay applies only while an observation is retained. The same
retained request cannot change its reason. Scheduler integration must check durable
acquisition replay first, so evicted observations cannot turn old polls into new
samples. The internal recorder requires the cluster transition lock and prior caller
authorization; stored worker/session provenance is not an authentication mechanism.
Only queued, uncancelled jobs can gain a new observation.

Sequence IDs may restart after a development rollback drops and recreates the
diagnostic table; they are not externally durable identifiers across that rollback.

## Scheduler sampling (D18e)

Fresh acquisitions from initially ready workers now record up to 16 blocked jobs
per request, including backfilled jobs. Candidate selection captures one database
wall-time sample for eligibility, reasons, aging, and observation timestamps.
Reason precedence is disabled project, retry backoff, placement, physical resources,
project quota, then sweep concurrency. Only authorized, queued, uncancelled jobs
enter the diagnostic pool. Disabled/backoff jobs do not enter winner selection;
existing assignment ordering and no-work outcomes remain unchanged.

The latest sequence determines each job's previous observation. Its timestamp
controls a 30-second cooldown shared across all workers, sessions, reasons, and
attempts. Cooldown filtering happens before the 16-job limit; unsampled jobs come
first, then oldest observations, with creation time and UUID breaking ties. This
reduces polling churn and gives previously unsampled jobs coverage. Clock rollback
can delay resampling. Another worker or a changed blocker does not bypass cooldown,
so retained history may show only one worker's context during an interval.

The full selected/sampled union locks jobs in UUID order before worker/accounting
rows. History and acquisition response share one transaction; failures roll both
back. Durable replay returns before sampling, even after its observation is evicted.
Recording precedes fresh readiness/time checks and lease issuance, so observation
lock waits do not consume an already-issued lease.

The row bound and per-request write bound do not bound queue scans or scheduling
latency. Missing or old observations do not imply an absence of blockers, and a
worker-specific observation does not establish global unschedulability. Focused
PostgreSQL/race checks, repository build/lint/smoke checks, and the full local
worker/Docker/storage runtime gate passed.
See the [implementation ledger](implementation.md#d18e-record-scheduler-blocker-observations)
for evidence and the fixture startup correction encountered during verification.

## Public job status (D18f)

`GET /v1/jobs/{id}` includes `queueDiagnostics` for the authorized owning project.
Its `asOf` is a database wall-time sample during the read, `attemptCounter` is the
job's current counter, and `observations` contains the newest 0–16 retained checks
in descending sequence order. Lifecycle, accepted manifest, counter, and history
come from one SQL statement snapshot. Reads acquire no execution authority and
do not change queue state, history, or leases. The time sample is not a global
snapshot of other workers or an availability guarantee.

Each observation contains `sequence`, `workerId`, `sessionId`, `requestId`,
`attemptCounter`, `reason`, and `observedAt`. The counter is the job's counter at
the check, not the number of an allocated attempt. History remains visible after
assignment, retries, and terminal outcomes; a retained check can belong to an older
counter. Neither a matching counter nor a recent timestamp makes it a current
global blocker decision. Sequence order survives clock rollback, which can put a
retained timestamp after `asOf`. No global `blocked` flag is returned.

`dispatch jobs get JOB_ID` labels these checks as historical and shows their
absolute server timestamps, reason, worker ID, and attempt counter. Empty history
prints “No queue observations recorded.” `--json` preserves the full object,
including session/request provenance. UUIDs are identifiers, not credentials or
execution capabilities. Foreign and absent jobs both return NOT_FOUND.

Submission/replay and cancellation mutation responses omit this additive field.
The client accepts its absence for older servers, but rejects explicit null or
incomplete objects when present. Diagnostic objects require exact, non-null fields;
unknown/duplicate fields, invalid times/UUIDs/reasons, out-of-range counters,
non-descending sequences, duplicate identities, and more than 16 checks fail
validation before CLI rendering. Existing job-field decoding remains unchanged.

Store/client, real CLI-to-HTTP-to-PostgreSQL, repository build/lint/smoke, and all
affected PostgreSQL package gates passed. Exact evidence is in the
[implementation ledger](implementation.md#d18f-expose-historical-blockers-in-job-status).
