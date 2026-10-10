# Worker maintenance

Request drain before planned maintenance:

```sh
bin/dispatch workers drain WORKER_UUID
bin/dispatch workers drain WORKER_UUID --json
```

Use a project token with the `operator` role. The worker must be authorized for
that project. Drain is host-wide: it stops new assignments for every project
sharing the worker. It does not cancel jobs, fence the session, revoke credentials,
stop the agent, or release reservations. Existing attempts may renew authority,
finish, publish outputs, and clean up normally. Wait for those attempts to finish
before shutting down the host.

The CLI prints the worker ID, recorded state, and acknowledged drain intent.
JSON is `{workerId,state,drainRequested}`. Success exits 0; argument, API, transport,
and output errors exit 2. Options follow a canonical worker UUID. Lost responses
are reported as unconfirmed; repeating drain is safe, including after an output
failure. The command sends one request and does not retry automatically.

## HTTP contract

`POST /v1/workers/WORKER_UUID/drain` requires operator permission, an empty body,
and no query string. Success returns 200 with the same JSON as the CLI. Read and
submit tokens receive 403. Invalid, unknown, or unauthorized worker IDs return
404; invalid/revoked credentials receive 401. Body/query options return 400
`INVALID_ARGUMENT`, including a bare trailing `?`.

Drain persists `drainRequested=true` under the same transaction lock used by
assignment. A `READY` worker becomes `DRAINING`; registering, suspect, offline,
and quarantined states retain their health evidence. Heartbeats communicate drain
to the agent, which stops acquisition while continuing existing supervision.
An assignment committed before drain remains authoritative, including replay of
its original acquisition request. Fresh requests after drain cannot assign work.

Token role, revocation, enabled project, and worker membership are checked for
the mutation. Token/project authorization remains locked through commit. The
first transition writes a worker audit and a project/operator audit atomically;
the latter action includes the worker UUID for correlation. Repeated requests
do not add audits. No resume endpoint is implemented in this slice.

## Verification

PostgreSQL tests cover concurrent repeated drains, active reservation and lease
preservation, heartbeat drain without stop, unhealthy states, project scope,
revocation, and rollback when either audit fails. Real CLI/HTTP tests enforce
roles, request shape, and idempotency. Client tests reject malformed acknowledgements;
CLI tests cover invalid arguments, output errors, and lost responses.

A local Rust-agent/Docker gate drains through the operator CLI while an attempt
is RUNNING. A bounded workload barrier waits for the agent's drain observation;
release requires the same job/worker's current RUNNING container. The job then
publishes its verified dataset-backed output and cleans up. A second queued job
stays unassigned while another real heartbeat arrives. The original execution
baseline also passed. Both use one local Docker daemon and explicit soft scratch;
independent Linux-host maintenance remains unverified.
