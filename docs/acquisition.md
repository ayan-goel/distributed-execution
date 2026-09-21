# Durable job acquisition

## Store transaction (D07a)

`store.AcquireWork` accepts an authenticated worker identity, a session UUID, and a
stable request UUID. It returns an assignment, a durable no-work reason, or a replay
decision. The transaction rechecks the credential, current session, and fencing;
possession of a request UUID never replaces authorization.

New assignments require a READY, healthy, reconciled, non-draining host without disk
pressure and with a heartbeat less than 15 seconds old. Docker, hard CPU/memory/PID
limits, and scratch quota capabilities are required. `AcquisitionPolicy{}` requires
strict scratch quotas. `AllowSoftScratch` explicitly enables the development profile;
it does not claim a hard disk limit. The executable policy flag is not wired yet.

Selection considers enabled authorized projects, QUEUED/RETRY_WAIT state, elapsed
backoff, cancellation, placement labels (including architecture), free CPU/memory/
scratch/slots, and shared project CPU/memory/concurrency quotas. Physical capacity
counts active **and quarantined** reservations. Project quotas count active attempts,
so fenced historical work does not prevent retry elsewhere. Actual cleanup still
controls reuse of the original host's capacity.

Fitting candidates precede blocked jobs, permitting backfill. Within those groups,
the current baseline orders by priority, creation time, and UUID. A no-work result
reports the highest-ranked candidate's blocker, or QUEUE_EMPTY when there are no
eligible visible candidates. This is not yet the final scheduling policy: durable
project round-robin, aging, and per-job blocker history remain D18 requirements.
Sweep concurrency joins this eligibility check with D17's sweep metadata.

The transaction holds the existing cluster transition lock, locks its selected job,
then worker and project accounting. It rechecks capacity/quotas/readiness and reads
fresh database wall time after those locks. No runtime or network call occurs inside
the transaction. For an assignment it atomically:

1. Increments the job's attempt counter and fencing generation.
2. Creates an ASSIGNED attempt with a 30-second lease and startup phase deadline.
3. Reserves CPU, memory, scratch, and one execution slot.
4. Moves the job to ACTIVE with its current attempt pointer.
5. Records the ASSIGNED event and acquisition request's response reference.

Commit failures return no assignment. Database ownership constraints independently
reject mismatched reservations and multiple authoritative active attempts. Only
input-free, digest-pinned admitted jobs are supported until dataset staging lands.
Returned canonical job bytes are reconstructed and checked against the stored hash.

## Replay

The `(worker, session, request)` identity stores either an attempt reference or a
no-work reason. Repeating a no-work request keeps that result even if new jobs arrive;
the worker must use a fresh UUID after its polling delay. Repeating an assignment
never allocates capacity again and never extends its lease or phase deadline.

Assignment replay locks the job, attempt, and worker, then reads fresh wall time.
Current active authority returns the same assignment with a new server-time sample.
Expired or displaced ownership returns FENCED; cancellation or phase expiry returns
STOP_REQUESTED; a terminal attempt returns ALREADY_TERMINAL. Revoked credentials and
old sessions are rejected before returning authority. The future wire adapter must
derive remaining durations from the deadlines and server-time sample; the agent must
also subtract elapsed RPC time and its local safety margin.

## Verification and remaining integration

Real PostgreSQL integration tests cover concurrent duplicate requests, fresh request
capacity races, stable empty-queue replay, non-renewing assignment replay, credential
revocation, worker health/drain, labels, shared project quotas across two registered
hosts, independent CPU/memory/scratch/slot ceilings, strict/development scratch policy,
backfill, cancellation, session takeover, and injected event failure rollback.
An expiry test observes a replay blocked on a real job lock, expires its lease after
that transaction started, and confirms the replay rejects authority after release.

This slice implements the store boundary. AcquireWork/ListAssignments gRPC handlers,
the Rust acquisition loop, Docker execution, renewal/reaper, sweep limits, and final
fair scheduling remain required. Two registered database identities are not evidence
for the release gate requiring execution on two independent Linux hosts.
