# Durable cached-image launch

`launch::launch` connects validated assignments, the asynchronous journal, the
STARTING phase RPC, and the runtime's create/start/inspect operations. This component
ends after confirming the container is running or already exited with valid exit
evidence. It does not report RUNNING/FINALIZING, publish results, or release capacity.
`launch::execute` builds on it to supervise execution and enter FINALIZING, as
described below. The agent startup command does not yet call these paths or acquire work.

## Preconditions and sequence

The caller must already have completed session registration/reconciliation, reserved
local capacity, and obtained a live `GrantedAssignment`. Supply that acknowledged
session, the current owner's `AsyncJournal`, and a prepared workspace whose final
directory name is the attempt UUID. The pinned image must already be cached; input
staging, image pulling, strict scratch preparation, and local reservation orchestration
remain separate required work. The authority receiver must come from a channel for
the assignment's exact worker/session/job/attempt/generation tuple.

Launch performs these steps under the live authority receiver:

1. Match the assignment, channel, session, and workspace identity.
2. Under serialized journal access, require the current in-memory incarnation and
   durable registration acknowledgement. Reject any existing attempt record. Save
   the assignment and a stable STARTING report before returning its request.
3. Send STARTING; retry only transient errors with the same event/payload, waiting
   one second between retries. Require an acknowledgement of exactly STARTING.
   Server rejection or already-advanced progress must not initiate a new launch.
4. Read a current authority window and create/inspect the deterministic container.
5. Persist its full container ID before issuing any start request.
6. Recheck authority, call start once, and inspect. Transport/deadline/server errors
   that can hide a committed start are resolved by inspection, never another start.
   Accept only RUNNING or non-running EXITED/DEAD with an exit code in 0–255.

The journal's claim and preparation are serialized, but assignment and phase intent
remain separate durable writes. A failure between them leaves evidence requiring
reconciliation. It does not authorize another launch. Duplicate deliveries are
refused even when an earlier launch appears incomplete; recovery uses fresh session
fencing and cleanup, not replaying the launch function against historical records.

## Authority and cleanup

Run batched lease renewal independently. Launch observes channel changes and local
deadlines while awaiting journal, RPC, and runtime operations. Each create/start uses
a fresh snapshot; these snapshots cannot bypass the outer authority checks. The
caller retains the receiver and must immediately continue runtime supervision and
phase reporting after success. A fast-exit container must proceed to exit evidence
and FINALIZING instead of being restarted.

On failure after a handle is known, launch attempts immediate kill and independent
confirmation through the watchdog's shared bounded termination path. Each operation
has a five-second outer budget. It preserves containers, logs, and journal evidence.

`LaunchError.cleanup` has these meanings:

- `NotCreated`: this invocation never attempted create. Another earlier invocation
  may still own a container; this is not a statement that the worker is empty.
- `Stopped`: the known container was observed exited/dead or explicitly absent.
- `Uncertain`: create may have committed without returning a handle, or termination
  could not be confirmed. Quarantine and reconcile using the journal/ownership labels.

No outcome authorizes server reservation release or result publication. A journal
operation cancelled by local expiry can still finish its blocking write; its permit
and exclusive owner remain held until that write completes. The parent must continue
polling launch through error cleanup. Dropping its future or killing the process
cannot run asynchronous Docker cleanup; restart uses the old-session recovery path.

## Verification scope

Unit fixtures use a real private journal with a deterministic runtime and phase
reporter. They check durable ordering, exact STARTING retry, duplicate refusal,
lost start replies, fast exit, expiry during phase/start, uncertain create, journal
failure before start, unacknowledged/wrong sessions, mismatched generation, rejection,
and already-advanced phase acknowledgement. The shared termination/runtime changes
also retain their real Docker component tests.

`TestRustDurableLaunchAndRenewalStopsRealContainerAfterFencing` runs the Rust
`launch_probe` subprocess against actual PostgreSQL, the authenticated Go service,
and Docker. It provisions a unique development worker, queues a pinned sleeping
workload, and lets the probe durably register, inspect an empty runtime, report
health, acquire its assignment, and launch it through this coordinator. The service
commits STARTING but deliberately loses its reply; the retry must preserve the exact
event and payload. The probe verifies the journal's container binding and actual
running state, then explicitly prepares/reports RUNNING.

The probe requests an explicit authority refresh after RUNNING. The resulting
renewal expires the attempt's database lease and returns FENCED, so the refresh
must fail. The real watchdog kills the container and confirms termination; the Go fixture
independently inspects it as stopped. PostgreSQL retains exactly two phase records,
STARTING and RUNNING, despite the lost response. Cleanup is restricted to that
fixture's unpredictable worker label. This test uses the soft-scratch development
policy and Docker Desktop evidence; it does not prove strict quotas or independent
Linux hosts. The probe's direct post-launch phase sequence is a test fixture, not
the production acquisition loop.

## Execution through finalization (D08h)

`launch::execute` consumes an authority receiver and calls the durable launch path
with the same session, journal, cached image, and prepared-workspace preconditions.
Its runtime handle comes only from that launch. A caller cannot substitute an
unrelated container handle for cleanup or reporting.

After launch, the coordinator inspects runtime state. An already-exited container
skips RUNNING; otherwise, it journals and reports RUNNING while concurrently
observing process exit. Slow journal writes, transient RPC retries, and a missing
acknowledgement do not prevent authority checks or runtime observation. Every retry
uses the exact journaled event and payload. Unexpected advanced acknowledgements
fail closed because this coordinator does not resume an earlier execution.

A conservative local execution deadline starts before the RUNNING report can commit.
It uses the validated execution timeout and the same suspend-aware clock as lease
authority. This bound persists through retries and the post-acknowledgement refresh;
periodic grants cannot overwrite it. Without it, an uncertain RUNNING commit could
start a short server execution timeout while the worker still held a longer startup
window. After a fresh post-phase grant arrives, normal authority observation uses
that grant's current server phase bound.

If runtime exit is observed before RUNNING reporting finishes, the coordinator
cancels that wait and proceeds to finalization. A journal write or server mutation
may already have committed. Serialized journal access preserves its evidence, and
server phase locking permits STARTING-to-FINALIZING or RUNNING-to-FINALIZING without
allowing a late RUNNING mutation to regress progress.

The coordinator records the validated exit code and runtime OOM flag before preparing
FINALIZING. Exit 137 alone is not treated as OOM. A separate conservative finalization
bound covers durable writes, exact FINALIZING retries, and fresh post-phase authority.
Nonzero exits also enter finalization so their logs and failure evidence can be
published later.

Success returns `FinalizingAttempt`: the bound handle, full attempt identity, durable
exit evidence, and the authority consumer retained for artifact work. It is not job
completion. Callers must keep renewing and checking authority during publication;
the returned grant can expire immediately after return. Dropping the result closes
its consumer so the renewal producer can retire the attempt.

Failures before successful launch retain `LaunchError` cleanup evidence. Later
failures use the same bounded kill-and-confirm path, preserve journal/container/log
evidence, and return `Stopped` or `Uncertain`. No path releases server reservations
or publishes a terminal result. The parent must keep polling through cleanup;
aborting the future cannot perform asynchronous termination.

Unit tests exercise normal RUNNING progression, real journaled OOM/exit evidence,
exact FINALIZING replay, fast-exit phase skipping, exit during an uncertain RUNNING
reply, finalization fencing, and expiry when an acknowledgement has no fresh grant.
A one-second execution timeout remains effective despite continuing longer renewal
grants while RUNNING acknowledgement stalls. Clock tests cover elapsed suspend-like
jumps, backwards samples, invalid durations, and overflow.

Two additional real PostgreSQL/mTLS/Docker scenarios run `execute` with a workload
that exits 7. Both lose and replay the committed FINALIZING reply, then require fresh
renewal and inspect matching durable/runtime exit evidence. One also withholds the
committed RUNNING reply until client cancellation, proving observation and finalization
can continue through that uncertainty. Database history retains exactly three phase
records and FINALIZING with exit code 7. These fixtures stop before artifact upload
and terminal publication; production acquisition and full release gates remain open.
