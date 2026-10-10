# v0.1 release checklist

The contract is [the project specification](../Dispatch_Project_Spec.md), sections
1–26. This is the remaining work queue, reconciled against the source tree on
2026-10-10. The [implementation ledger](implementation.md) records historical
verification. A test's existence alone does not close its release requirement.

## Implemented core

The ledger records real CLI-to-worker execution and verified downloads, datasets,
bounded live logs, cancellation, failed/cancelled-only sweep retry, placement,
and strict ext4 scratch quotas. The latest two-VM gates establish 27-child sweep
recovery and rejection of a delayed old completion. These establish the core
execution path; they do not establish every fault or release condition below.

## Release deliverables

Close these bounded deliverables in dependency order. Add work only when it maps
to a specification requirement or a reproduced defect. Reuse passing evidence
when its scope matches; do not repeat tests merely for reassurance.

| ID | Specification | Deliverable and acceptance evidence | Current gap |
| --- | --- | --- | --- |
| R01 | §19.1, §20 | `make dev-up` starts isolated, persistent local PostgreSQL and versioned S3 dependencies; documented credentials and stop/restart path; verify both services and preserved data | Complete: [development setup](development.md); actual DB/object-version persistence across restart, credential isolation, and server startup verified |
| R02 | §13.3 | Multipart output transfers above a configurable threshold, scoped parts, exact-version verification, bounded retry, and abandoned-upload abort; real storage and worker tests | [Authenticated multipart API and exact-version publication](multipart-uploads.md) verified within 64 MiB; automatic Rust delivery, larger size policy, and submitted-job fault demonstration pending |
| R03 | §13.5 | Configurable project retention, reference-aware tombstones and exact-version deletion, abandoned multipart cleanup; prove active work and registered datasets survive cleanup | No retention implementation |
| R04 | §14.3, §18 | Bounded platform metric labels, latency histograms, queue/reservation/health/retry/transfer/log-gap/cleanup measurements; verify request-rate limits and audited operator/token actions | Platform metrics absent; HTTP already caps 200 requests/second and 256 in flight; limit and audit coverage need reconciliation |
| R05 | §17, §21.3 | Close the fault matrix below on the supported Linux profile, including live quota-enforcement loss and physical cleanup before restored readiness | Existing VM evidence covers quotas, sweep loss, and fencing; remaining dedicated-VM runtime scenarios unproven |
| R06 | §4–16, §21.1–2 | Contract audit of CLI/HTTP/gRPC, eight invariants, generated transition interleavings, DB races/rollback, schemas, replay and protocol compatibility | Substantial tests exist; consolidated requirement coverage and published schemas remain open |
| R07 | §1, §21.4, §23 | Run one actual CPU evaluation with immutable data, a sweep, exported metrics, logs, and verified downloaded outputs without per-job SSH | Synthetic integration workloads are insufficient; user's workload selection pending |
| R08 | §20, §22 | Reproducible `make bench` and published measurements for every §22 row with hardware, dependency versions, offered load, cache conditions, utilization, transfer/log/retry timing, and limitations | No `benchmarks/` or root task |
| R09 | §19.2–3, §20 | Paired Linux binary packaging, declared architecture builds/CI, TLS deployment, drain/upgrade, combined metadata/object-version backup and isolated restore | CI exists for x86 Linux contracts; release artifacts and verified recovery instructions missing |
| R10 | §20, §23, §26 | Runnable `make demo`, clean tutorial, actual second-user completion, repository/license decision, final sections 1–26 audit | Author-run tests do not prove second-user usability; repository/license decision remains with owner |

R02 precedes multipart retention in R03. R04 supplies benchmark instrumentation.
R07 can proceed independently once a workload is selected. R09/R10 consume the
completed runtime and data contracts. None of these gaps changes v0.1's CPU-only,
trusted-workload, dedicated-Linux deployment boundary.

## Required fault matrix

“Recorded” identifies existing ledger evidence, not a new test run. “Open” means
the exact fault is not yet established by the consolidated release audit.

| §17 fault | Existing evidence or required next proof |
| --- | --- |
| Submit reply lost | Recorded: HTTP replay and concurrent durable submission; reconcile the 100-request count with §21.4 |
| Server crashes after assignment commit | Durable acquisition replay tests exist; open: actual process restart and recovery of the committed assignment |
| Worker crashes before container start | Open: inject process death before start on the dedicated VM and observe natural expiry/retry with no accepted old output |
| Runtime create reply lost | Runtime identity/reconciliation tests exist; open: confirm injected ambiguous Docker response coverage |
| Worker disconnects while running | Recorded: local control-only partition stops before lease expiry; repeat on the supported dedicated-VM profile |
| Old worker returns after reassignment | Recorded: two-VM stale-result rejection; open: restart old worker and observe old container cleanup before readiness/capacity returns |
| PostgreSQL unavailable | Open: interrupt actual DB availability; verify no assignment/renewal and local-deadline shutdown |
| S3 unavailable | Open: interrupt actual storage availability; verify bounded spool/retry and finalization failure/deadline |
| Cancellation races completion | Recorded: both commit orders, real running/pre-launch/finalizing cancellation; consolidate race/replay evidence |
| Completion reply lost | Recorded: identical replay and journal recovery; confirm fault injected after commit, before response |
| Worker disk fills | Open: real dedicated-VM pressure, acquisition stops, bounded logs, explicit transfer failure and status |
| Server restarts | Open: restart actual server with queued and unexpired active work; preserve history and resume reaping |

The §21.3 dedicated-VM runtime gate also needs OOM, noisy logs, startup failure,
execution timeout, cancellation, unsafe/missing outputs, agent restart, and
control-only partition. Existing tests on one Docker Desktop daemon can be reused
as fixtures, but cannot close that deployment-specific gate.

## Final audit

Before declaring v0.1 complete, map every numbered specification requirement to
current source and observed evidence. Check all ten §21.4 gates, every §22
measurement, root tasks, declared architecture artifacts, security boundaries,
deployment/restore, practical evaluation, and second-user tutorial. Report unmet
performance targets as measured limitations; do not call unmeasured targets met.
GPU, dashboard, Python SDK, HA, and cluster-manager adapters remain deferred.
