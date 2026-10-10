# Dispatch

A distributed batch execution platform for running containerized jobs across a
shared pool of Linux machines.

Dispatch is built for researchers and engineers running experiment sweeps,
simulations, data preprocessing, or backtests. Its goal is to replace manual SSH
sessions, machine selection, retry scripts, and scattered output folders with one
place to submit work and collect results.

**Stack:** Go · Rust · gRPC · PostgreSQL · Docker · S3-compatible object storage

## How it works

The planned v0.1 workflow:

1. **Submit a job** with a container image, command, inputs, and resource requirements.
2. **Run it on an available machine.** Dispatch queues the work and assigns it to a
   worker with enough CPU, memory, and scratch space.
3. **Track progress and collect results.** Inspect logs and attempt history, retry
   eligible failures, and download outputs tied to the original job configuration.

For example, a parameter sweep can turn 3 algorithms × 3 learning rates × 3 seeds
into 27 jobs distributed across your machines.

## Project status

**v0.1 is in development and targets CPU workloads.** GPU support is planned for v0.2.
An integration test now runs CLI submission through the real worker daemon and
Docker, then verifies the output with a CLI download. A second integration test
verifies worker-loss retry across two worker identities. Running-job cancellation
is also verified through worker stop, durable acknowledgement, cleanup, and
subsequent admission. Cancellations before launch and during finalization are
also covered. The worker captures bounded logs while jobs run and publishes
sealed segments during execution for `dispatch logs --follow`; missing data is
reported as incomplete. The CLI can register immutable datasets from a local
directory. Dataset-backed jobs now pin project-owned registrations at submission,
refresh signed download grants, and mount verified cache contents read-only.
The real worker and Docker integration test reads an uploaded dataset and
publishes its output. Sweeps now support CLI and HTTP submission, concurrency
limits, cancellation after permanent failure, CLI/HTTP progress inspection, and
CSV/JSON result export. A local 27-job sweep is verified across three worker
processes sharing Docker, including metrics, exports, both fail-fast policies,
and recovery after killing a running worker. Independent Linux-host execution and
the broader failure matrix remain to be verified. A separate local test rejects
a real delayed completion after replacement and downloads the replacement's
distinct output bytes. `dispatch sweep retry` creates fresh linked jobs only for
failed/cancelled children while keeping successful results and the original history.
A local dataset-backed test verifies those retry jobs execute and publish outputs
across the remaining workers after a worker loss.
The scheduler now rotates among eligible projects with a durable database cursor;
jobs accept priorities from 0–3 and gain one level per 10 eligible waiting minutes.
The scheduler records bounded per-job blocker history, visible through job status
and `dispatch jobs get` with worker context and timestamps.
`dispatch jobs list` finds project-owned jobs by state and metadata labels, with
bounded cursor pagination and JSON output.
`dispatch wait` waits for a terminal job outcome with script-friendly exit codes
and an optional timeout; timeout cancellation requires an explicit flag.
`dispatch attempts list` exposes ordered execution history and pending cleanup.
The HTTP API also serves bounded, resumable pages of durable job events.
`dispatch workers list` shows authorized hosts, recorded health, and occupied/free
capacity, including reservations awaiting cleanup.
Workers now pull missing pinned image digests under the assignment's startup
deadline before staging inputs; cached images are reused.
The server fences expired startup, execution, and finalization phases with explicit
timeout reasons and preserves uncertain capacity until cleanup is confirmed.
Rejected Docker starts report runtime failure after verified container removal.
A local control-disconnection test verifies workers stop Docker jobs before lease
expiry and reconcile cleanup before reusing capacity.
`dispatch workers drain` lets authorized operators stop new assignments for
maintenance while existing attempts finish.
Declared metric outputs are collected into source-bound completion reports.
An [ext4 quota primitive](docs/project-quotas.md) passed byte/inode enforcement tests
on a dedicated Linux VM. Durable project-ID allocation also passes restart tests;
worker integration remains pending.
Strict scratch limits, full retry policy coverage, and
other v0.1 gates remain. See the
[implementation ledger](docs/implementation.md) for verified progress and remaining work.

## Development

Requires Go 1.27.1, Rust 1.88.0, and protoc 34.1. Integration tests also require Docker.

```sh
make build
make test lint smoke
make integration
make generate-check
```

## Documentation

- [Project specification](Dispatch_Project_Spec.md) — scope, architecture, and roadmap
- [Operator guide](docs/running.md) — configure and run the current components
- [Object storage](docs/object-storage.md) — versioned storage and local development setup
