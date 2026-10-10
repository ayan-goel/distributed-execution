# Runtime failure gates

The actual worker runs these jobs through mTLS, PostgreSQL, Docker, and versioned
local object storage. Each job keeps infrastructure retries enabled with two
allowed attempts. A permanent workload failure must produce one attempt, one
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

Run with both disposable data services:

```sh
sh scripts/test-objectstore.sh sh scripts/test-store.sh \
  .tools/go/bin/go test -v -race -tags integration ./cmd/dispatch-server \
  -run '^TestWorkerDaemonDistinguishesOOMFromExit137$' -count=1
```

These gates use one Docker Desktop Linux engine and development scratch. They
verify this daemon's memory enforcement and the worker's classification, not the
dedicated Linux-VM runtime matrix, strict scratch quotas, or host-wide exhaustion.
See [phase-timeouts.md](phase-timeouts.md) and
[worker-leases.md](worker-leases.md#control-channel-partition-gate) for the verified
deadline and control-disconnection cases.
