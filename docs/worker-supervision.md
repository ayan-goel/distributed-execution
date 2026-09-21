# Live-container authority watchdog

`rust/worker/src/supervisor.rs` adds the running-container portion of attempt
supervision. It observes an already-started runtime handle and terminates execution
when local authority expires, the server rejects ownership, or the authority
controller disappears. The [periodic renewal task](worker-leases.md#periodic-renewal-task-d09f)
now feeds this channel. Launch journaling, phase reporting, and result finalization
still need integration with these components.

## Authority channel

The caller creates `authority_channel` with the attempt's complete identity and a
fresh `AuthorityWindow` from the validated acquisition response. The initial window
must still be live. The runtime handle and authority tuple must refer to the same
attempt; the future launch coordinator owns that association.

The returned controller accepts the existing client's typed `RenewalOutcome`.
Every update must match worker, session, job, attempt, and generation exactly.
Mismatched identities and invalid decision kinds return errors without extending
the current window. Callers must handle these errors, not silently treat them as a
successful renewal.

Updates are serialized by a Tokio watch channel and coalesce to one bounded state.
A rejection or observed expiry is irreversible for this execution. A renewal
received after the previously held local window expired cannot revive the container,
even if its new window is individually live. Concurrent later renewal also cannot
overwrite a recorded stop. Explicit rejection remains the stop reason when the
renewal task sends it and then exits. Loss of all controller senders independently
triggers `ControllerLost`.

## Observation and termination

`supervise_running` retains each asynchronous runtime inspection while processing
authority changes. It does not restart a slow inspection at every watchdog tick.
Before and after observations, it checks the current authority. During a stalled
operation it checks at the earlier of the local deadline and a 100-ms interval.
Linux checks use the existing suspend-aware `CLOCK_BOOTTIME` window; Tokio timers
only schedule wakeups and do not supply authority.

A confirmed exited/dead, non-running container with a valid exit code returns
`Exited(ContainerStatus)`. This is observed process evidence, not job success or
permission to publish a result. The caller must journal that evidence, obtain valid
finalization authority, and use the fenced completion protocol.

Expiry, server rejection, controller loss, invalid runtime state, or a failed runtime
observation initiates immediate kill. Waiting a configured graceful shutdown period
must not extend execution after authority loss. The kill operation has a five-second
outer budget; a separate five-second inspection budget confirms exit or explicit
404 absence. Sending a signal or receiving a successful kill response alone does
not confirm cleanup. The watchdog never removes the container or collects/deletes
its logs and outputs.

`Stopped { reason, confirmed: false }` requires quarantine and further reconciliation.
Do not release local reservations or claim physical cleanup from that result.
Even confirmed physical exit does not authorize local release of the server's
reservation or publication of a job result. Those are control-plane transactions.

A suspended process, unresponsive daemon, or kernel failure can prevent timely
physical termination. Observation granularity and cleanup budgets are not a proof
that user code stops at the exact deadline. Server fencing remains required to
reject stale updates/results. The parent must keep polling the watchdog until it
finishes; aborting its future or killing the process cannot run asynchronous cleanup.
Process restart uses the separately verified old-session recovery path.

## Evidence and remaining integration

Unit tests prove a stalled inspection cannot suppress expiry, uncertain cleanup
stays uncertain, mismatched generation cannot renew another authority, a late grant
cannot revive expired execution, live renewal extends observation, and rejection
remains irreversible. All three server rejection decisions survive controller exit.
A slow successful inspection completes once rather than being repeatedly cancelled.
The fake runtime rejects unexpected create/start/stop/remove/log operations.

The real Docker lifecycle fixture starts a sleeping workload, supervises it with a
short local grant, observes `AuthorityExpired` and confirmed stop, and independently
inspects the container as no longer running. This uses synthetic grant timing to
isolate the watchdog; it is not a control-channel partition test. Linux tests exercise
the production clock path; the macOS Docker fixture remains development evidence.

The next integration must connect durable assignment/phase ordering, launch checks,
the verified periodic renewal producer, this watchdog, and finalization. The current
worker startup command still does not acquire jobs. Real channel-partition,
cancellation, active-job restart, and complete v0.1 execution gates remain open.
