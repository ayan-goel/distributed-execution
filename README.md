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
publishes its output. Internal sweeps now enforce concurrency and cancel queued
siblings after permanent failure; public sweep commands are still in development.
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
