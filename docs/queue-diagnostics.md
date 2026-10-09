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

The foundation is not yet connected to acquisitions or public job status. Sequence
IDs may restart after a development rollback drops and recreates the diagnostic
table; they are not externally durable identifiers across that rollback.
