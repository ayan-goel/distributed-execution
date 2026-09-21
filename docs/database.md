# PostgreSQL ownership foundation

## Schema and migration tests (D03a)

`migrations/0001_core.up.sql` defines projects, workers, historical worker sessions,
jobs, attempts, reservations, events, submission keys, and acquisition request keys.
UUIDs identify entities. UTC instants use `timestamptz`; database `clock_timestamp()`
is the time source. Resource fields are integer quantities with positive checks.

The database enforces:

- One active attempt per job with a partial unique index, and unique attempt numbers.
- Job pointers reference attempts belonging to that same job through composite keys.
- Attempt sessions belong to the correct worker. Session history survives a restart.
- At commit, current ownership agrees with the active attempt, its reservation,
  requested resources, and the attempt counter. Deferred triggers allow atomic
  insertion/update of mutually referencing rows without exposing an incomplete owner.
- A successful job references a successful attempt and a non-null accepted manifest.
- Job execution specifications and terminal outcomes are immutable. Attempt identity
  and terminal outcome cannot be rewritten; cleanup status can still be reconciled.

These constraints are a backstop. They do not yet implement authorization, lease
expiry, placement, sum-of-reservations capacity checks, or allowed transition order.
Those require the transaction operations and race tests in subsequent slices.
Artifact/dataset/sweep tables will be added with their corresponding behavior.

## Verification command

```sh
make integration
```

This starts a uniquely named disposable PostgreSQL 17.11 container using a pinned
image digest, no published ports, and no persistent volume. The script removes only
that test container on exit. Its trust authentication is confined to this isolated
test instance; it is not a deployment authentication configuration.

The suite applies all migrations in order, tests legal assignment and failure
transactions plus forbidden ownership/identity updates, rolls migrations back in
reverse order, asserts that no application tables remain, reapplies them, and reruns
the constraints. This verifies fresh installation and rollback/reapply; upgrades
between released schema versions need additional fixtures when such versions exist.

The image starts a temporary socket-only server during initialization. Readiness
checks use TCP to wait for the final server and avoid a startup race.

## Transaction discipline for subsequent operations

Assignment, cancellation, expiry, completion, and session fencing must acquire the
cluster transaction advisory lock, then jobs in sorted UUID order, then attempts,
then worker/project accounting rows. Lease renewal locks only job/attempt and never
subsequently asks for the cluster lock. Evaluate expiry with fresh DB wall time
after acquiring row locks. No external RPC or object verification occurs while these
locks are held. Configure finite statement/lock timeouts and retry by operation ID.

## References

- [PostgreSQL constraints](https://www.postgresql.org/docs/17/ddl-constraints.html)
- [PostgreSQL explicit locking](https://www.postgresql.org/docs/17/explicit-locking.html)
