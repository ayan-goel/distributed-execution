# Worker sessions and recovery

## Registration (D06c)

`RegisterSession` binds a provisioned host credential to a requested session UUID.
The request UUID, protocol version, resource claims, slots, labels, and capability
set are bounded and validated. Resource claims may be lower than the operator's
ceilings; labels must exactly match the provisioned host labels. This prevents a
worker from joining another placement group through registration.

Protocol version 1 requires `docker.v1`, `cpu.hard`, `memory.hard`, `pids.hard`, and
exactly one of `scratch.soft` or `scratch.quota`. These are advertised capabilities,
not evidence that containment works; the Rust runtime must detect/enforce them and
the scheduler must honor the deployment's required scratch profile. Linux amd64
and arm64 are the supported architectures. Unsupported capabilities fail closed.

The canonical registration hash excludes the request ID and sorts capabilities.
The same request returns the existing incarnation. Another request ID can recover
the same incarnation only with identical claims. Reusing a request or session with
changed claims conflicts. Registration never refreshes heartbeat liveness on replay.
An old fenced session can never become current through replay.

New sessions start at generation 1, with monotonically increasing generations for
subsequent incarnations. Session identity, creation time, and registration hash are
immutable; a recorded fencing timestamp cannot be removed or changed. The worker
starts in REGISTERING with reconciliation incomplete. Registration alone does not
make the host eligible to execute work. Concurrent different incarnations currently
fail with `ErrSessionActive`; takeover and expired-session recovery are the next slice.

## Transaction ordering

Reservation-changing paths start with transaction advisory lock `1146310734`,
distinct from migration lock `1146310733`. After that, lock affected jobs by sorted
ID, attempts, then worker/accounting rows. Registration rechecks the credential under
a shared row lock to serialize with revocation. Revocation takes only its credential
lock and never subsequently asks for the cluster lock. Lease renewal must lock its
job/attempt without subsequently taking the cluster lock.

Transactions use a two-second lock timeout and four-second statement timeout;
RPC calls have the transport's five-second deadline. No registry, runtime, object
store, or remote RPC belongs inside a database ownership transaction.

## Verification

Real PostgreSQL tests cover 32 identical concurrent registrations with one session
and one request record, stable replay with a new request ID, changed-claim conflict,
two racing incarnations with one winner, claimed capacity/label/protocol rejection,
revocation rechecked inside the transaction, and rollback after an injected audit
failure. The migration also passed fresh apply, rollback, and reapply. These tests
prove durable registration; no heartbeat or runtime cleanup is implemented yet.
