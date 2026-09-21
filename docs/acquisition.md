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
old sessions are rejected before returning authority. The wire adapter derives
remaining durations from the deadlines and server-time sample; the agent must also
subtract elapsed RPC time and its local safety margin.

## Acquisition RPC (D07b)

The executable worker service now implements `AcquireWork` behind the existing mTLS
listener. The handler requires verified transport identity even on direct invocation,
binds the request to that worker, and passes the session/request UUIDs to the store.
The service constructor receives an explicit acquisition policy; the executable uses
the default strict scratch policy until its development-profile flag is added.

An assignment carries the same authority, canonical spec/hash, pinned image, argument
vector, and byte-based resource limits. Remaining lease and phase durations are
floored to milliseconds from the store's fresh time sample. Expired or submillisecond
authority returns FENCED/STOP_REQUESTED rather than underflowing an unsigned duration.
No-work and rejection outcomes must map to known, nonzero wire enums; ambiguous or
unknown internal results fail with Internal/INVALID_ACQUISITION_RESULT.

## Assignment recovery pages (D07c)

`ListAssignments` is available through the authenticated worker service. Each page
checks the credential and current unfenced session, locks candidate jobs in UUID
order, then their attempts and worker accounting, and reuses the same fresh-time
authority checks as acquisition replay. It returns only actionable current attempts;
expired, cancelled, phase-expired, or terminal authority cannot grant execution.
Reading inventory changes no lease, reservation, job outcome, or event history.

Requests accept a canonical `after_job_id` and page size (default 32, maximum 64).
The response cursor advances over scanned candidates, including those no longer
actionable, so an empty page can still require another request. The database holds
at most 65 candidate job/attempt locks per page. Canonical specs share a conservative
byte budget that allows for duplicated argv/image fields; the adapter additionally
enforces the exact 4-MiB protobuf limit. A large individual assignment can use a page
alone. An oversized encoded page fails with ResourceExhausted/ASSIGNMENT_PAGE_TOO_LARGE.

Recovery must pause new acquisitions while following cursors, process pages without
retaining every spec, and continue until the cursor is blank. The cursor grants no
cross-host/session visibility. Each page can reflect newer cancellation or expiry;
this is not a frozen snapshot and an omitted assignment is not proof of completion
or successful container cleanup.

## Rust acquisition and recovery client (D07d)

`ControlClient::acquire` and `list_assignments` use the existing mTLS channel and
five-second RPC deadline. Callers retain the acquisition request UUID across retries;
the client never silently converts an uncertain response into a new request.
Both operations sample the local monotonic clock before sending and after receiving,
then construct [conservative authority windows](worker-leases.md). A delayed response
cannot start a fresh lease. An expired grant returns its identity separately, without
execution authority, so reconciliation can still account for it.

Responses must bind to the requested worker/session and carry canonical nonzero UUIDs,
a positive signed-range generation, bounded resources/argv/spec bytes, a pinned image,
and a lowercase SHA-256 hash. Unknown or missing outcomes fail closed. Recovery checks
page counts, strict job ordering, unique jobs, and forward cursor progress; expired
items retain their identities without blocking traversal of the remaining page.

The client additionally [parses and validates execution settings](execution-spec.md),
checks the SHA-256 of the original JSON bytes, and verifies agreement with duplicated
wire fields. It rechecks authority after parsing. Staging inputs and enforcing runtime
settings remain prerequisites before Docker execution; inputs are rejected until D16
staging support. A returned window must be rechecked before acting; it is not a running
watchdog or a durable journal entry.

`rust/worker/examples/work_probe.rs` exercises acquisition, exact replay, and incremental
single-item recovery pages against the Go service. It pauses acquisitions during the
scan and retains only IDs to check traversal. The production journal and agent loop
are still pending; the fixture does not create containers or advertise runtime health.

## Verification and remaining integration

Real PostgreSQL integration tests cover concurrent duplicate requests, fresh request
capacity races, stable empty-queue replay, non-renewing assignment replay, credential
revocation, worker health/drain, labels, shared project quotas across two registered
hosts, independent CPU/memory/scratch/slot ceilings, strict/development scratch policy,
backfill, cancellation, session takeover, and injected event failure rollback.
An expiry test observes a replay blocked on a real job lock, expires its lease after
that transaction started, and confirms the replay rejects authority after release.

The Rust fixture also verifies stable empty-queue replay after jobs arrive, three
distinct acquisitions with no duplicate attempts, inventory recovery, and expired
replay rejection through the real Go executable and PostgreSQL. A separate mTLS
fixture delays a grant beyond its usable local lifetime and confirms the client
rejects execution authority despite receiving a successful RPC response.

The store, RPCs, and Rust acquisition/recovery client are implemented.
The production agent loop, integrated Docker execution, Rust renewal loop/reaper, sweep limits, and final
fair scheduling remain required. Two registered database identities are not evidence
for the release gate requiring execution on two independent Linux hosts.
