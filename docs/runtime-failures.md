# Runtime failure gates

The actual worker runs these jobs through mTLS, PostgreSQL, Docker, and versioned
local object storage. OOM, exit-137, and invalid-output jobs keep infrastructure
retries enabled with two allowed attempts. A permanent workload failure must produce one attempt, one
accepted failure completion, no canonical success, released reservations, and
local container/workspace cleanup.

## Memory exhaustion and exit 137

`TestWorkerDaemonDistinguishesOOMFromExit137` runs two bounded workloads:

| Workload | Docker exit | Docker OOMKilled | Dispatch reason |
| --- | --- | --- | --- |
| AWK array exceeds its 128 MiB memory limit | 137 | true | `OOM` |
| Shell explicitly exits 137 | 137 | false | `APPLICATION_EXIT` |

Before accepting completion, the fixture independently inspects the stopped
container and checks Docker's memory and memory-plus-swap limits both equal
134,217,728 bytes. Exit 137 alone cannot establish OOM. The allocation loop has a
finite upper bound; the job's 30-second execution budget and 60-second fixture
deadline also bound a runtime regression. Failed jobs may retain diagnostic logs,
but neither becomes the job's accepted result.

## Required output validation

`TestWorkerDaemonRejectsMissingAndUnsafeRequiredOutputs` declares one required
regular file at `/outputs/result`, limited to 4096 bytes. Each real workload exits
zero but leaves that path missing, writes 4097 bytes, creates a symlink to
`/etc/passwd`, or creates a directory. Docker independently confirms successful
process exit and no OOM in each case.

All four jobs fail permanently with `OUTPUT_INVALID`. The fixture checks that no
OUTPUT upload declaration exists, so the invalid path or bytes never cross the
output upload boundary. Normal diagnostic log delivery still runs. Container and
workspace cleanup complete without accepting a canonical result. These cases do
not establish every nested-path or concurrent filesystem mutation interleaving.

## Rejected container start

`TestWorkerDaemonReportsRejectedContainerStart` rejects only Docker's start request
with HTTP 503 at a private proxy. Creation and cleanup use the real daemon. The
worker checks ownership, removes the non-running container without force, and
independently observes absence before sealing `RUNTIME_UNAVAILABLE`. A bound
STARTING journal with no exit evidence is required; the completion records no exit
code and incomplete logs. Independent Docker inventory confirms absence before
the server accepts the failure and releases reservations.

CREATED alone cannot prove safe cleanup: a racing start, surviving container, or
unbound ambiguous creation retains uncertainty. Component tests cover lost delete
replies, a start winning the deletion race, journal guards, and exact completion
replay. The agent also handles a server cancellation decision using the existing
cancelled-completion path. This one-attempt integration gate does not establish
retry execution, lost completion replies, or cancellation races on this branch.

`TestWorkerDaemonClassifiesMissingExecutable` provides a counterexample: this
daemon starts the container and records exit 127 for a missing executable, so the
worker preserves `APPLICATION_EXIT` rather than inventing a runtime failure.

Run all eight cases with both disposable data services:

```sh
sh scripts/test-objectstore.sh sh scripts/test-store.sh \
  .tools/go/bin/go test -v -race -tags integration ./cmd/dispatch-server \
  -run '^(TestWorkerDaemonDistinguishesOOMFromExit137|TestWorkerDaemonRejectsMissingAndUnsafeRequiredOutputs|TestWorkerDaemonReportsRejectedContainerStart|TestWorkerDaemonClassifiesMissingExecutable)$' -count=1
```

These gates use one Docker Desktop Linux engine and development scratch. They
verify this daemon's memory enforcement and the worker's classification, not the
dedicated Linux-VM runtime matrix, strict scratch quotas, or host-wide exhaustion.
See [phase-timeouts.md](phase-timeouts.md) and
[worker-leases.md](worker-leases.md#control-channel-partition-gate) for the verified
deadline and control-disconnection cases.
