# Docker runtime adapter

## Implemented library (D08b)

`dispatch_worker::runtime::Runtime` defines create, start, inspect, wait, stop, kill,
logs, and remove operations. Handles are implementation-specific; lifecycle states
and bounded log results are runtime-independent. `DockerRuntime` implements this
contract with pinned Bollard 0.21.1, using only its local socket transport feature.
It negotiates the Docker API version and requires a Linux daemon reporting CPU CFS,
memory, swap, PID-limit, and seccomp support. No environment-based remote daemon
discovery or plaintext TCP connection is permitted.

The adapter is a library, not the production agent loop. A supervisor must persist
the assignment before calling it, account for capacity, renew leases, process stop
intent, reconcile uncertain results, and retain stopped containers until completion
is durable. It must share one runtime instance and hold the future journal's exclusive
agent lock. In-memory serialization alone cannot coordinate separate agent processes.

## Ownership and replay

Container names are `dispatch-<attempt UUID>`. Labels carry worker, session, job,
attempt, generation, immutable spec hash, and workspace policy. A create call first
inspects that name. After any create response, including a lost reply, it inspects the
same name and verifies identity and execution settings before adopting the full
64-character container ID. A conflicting worker/session/spec cannot adopt it.
Docker can reserve the name before inspection sees the container; ambiguous create
responses therefore allow repeated read-only lookups within one five-second budget.

Every later operation rechecks that ID's immutable ownership and image. Starting
also verifies the configured limits, mounts, user, arguments, and security settings.
It starts only CREATED containers; RUNNING is a replay, while an exited attempt
cannot be restarted. A bounded mutex covers inspection through start so two local
callers cannot both observe CREATED and restart a fast-exiting workload. This is a
correctness baseline; it serializes starts across this runtime instance.

Stop, kill, inspect, logs, and remove require ownership but do not require unchanged
resource limits. This allows cleanup if an operator altered a running container's
configuration. Remove refuses a running container and does not force removal;
already-absent IDs are an idempotent success. No operation acts on an arbitrary name
after acquiring a verified full ID.

## Execution restrictions

Only [validated execution settings](execution-spec.md) enter the Docker builder.
Commands and arguments are arrays with no implicit shell. Dispatch overrides image
entrypoint/command and disables inherited image healthchecks. Jobs run as UID/GID
65532 with all capabilities dropped, no-new-privileges, Docker's configured seccomp
profile, a read-only root, disabled workload network, and 256 PIDs. Restart policy is
`no`; automatic container removal is disabled so exited state remains inspectable.

Memory uses the admitted byte limit, with memory-plus-swap set to the same amount
to disable swap. CPU uses CFS quota/period, without physical-core pinning. At least
10 milliCPU uses a 100-ms period; smaller reservations use a one-second period so
the kernel's minimum 1-ms quota does not round usage above the reservation. `/tmp`
and shared memory have separate size settings capped at 64 MiB and one quarter of
the memory limit. CPU accounting can still burst within the kernel scheduler's
documented behavior.

Prepared workspaces bind `/inputs` read-only and `/outputs` and `/scratch` writable.
The caller supplies existing directories under a canonical, agent-owned private
parent; symlink children and group/other-writable parents are rejected. The parent
and its trusted ancestors must remain controlled by the agent/operator. Jobs never
supply host paths. Image-declared volumes are rejected because they could introduce
additional unaccounted writable storage.

The only currently constructible workspace is explicitly `soft_development`.
It does **not** enforce a filesystem quota or advertise `scratch.quota`. Disk-pressure
monitoring, allocation, symlink-safe output collection, and strict quota-backed
workspaces remain required. The control plane's default strict scratch policy is
unchanged. Docker adapter fixtures therefore do not make a production worker READY.

Images must already be present under their admitted pinned references. Pulling,
registry policy/credentials, and image staging deadlines remain the next runtime
integration work; create never silently pulls an image.

## Deadlines and logs

Create/start repeatedly check the conservative local authority window. Their daemon
requests use the smaller of remaining authority and five seconds. Inspection after
an ambiguous create has its own five-second bound and grants no renewed authority.
Connection negotiation, capability discovery, inspection, kill, removal, and each
wait/log observation are bounded to five seconds per request. Stop permits up to the
configured grace (maximum 300 seconds) plus five seconds for the daemon response.
Composite operations may contain multiple bounded requests.

A timeout does not prove Docker cancelled its operation. The supervisor must inspect
before deciding what happened, and must initiate termination early enough for grace
and cleanup. Host/process freezes still require server fencing. These methods do not
implement a watchdog or a renewal loop.

Logs preserve stdout/stderr separately, return at most the requested byte budget
(maximum 1 MiB), and signal truncation. Docker's `json-file` driver rotates at 1 MiB
with two files; this bounds retained file count and rotation targets, not a strict
total-byte quota. Live streaming, durable spool cursors, upload, and retention-gap
reporting remain D15. Bollard decodes trusted daemon metadata; this slice does not
add a separate hard byte limit to its JSON metadata responses or individual frames.

OOM classification reads Docker's `OOMKilled` state. Exit code 137 alone is not
sufficient: an explicit SIGKILL can produce the same exit code without an OOM.

## Startup inventory and fenced-session cleanup

`RecoveryRuntime::inventory(worker_id)` discovers this worker's containers directly
from Docker, including created, paused, and exited containers. It does not require
the journal to have received the container ID before an earlier process died.
Discovery filters by the exact worker label, requests at most 1025 summaries, and
rejects more than 1024 instead of treating truncation as a complete snapshot. One
five-second deadline covers listing and all inspections. Returned snapshots are
sorted by full container ID; duplicate IDs, malformed metadata, and errors fail
the entire call. Only an explicit inspect 404 means a listed container disappeared.

Each container is inspected by full ID. Required labels bind worker, session, job,
attempt, generation, spec hash, and the supported scratch profile; the deterministic
container name and local image ID are checked too. Unexpected same-worker metadata
requires operator investigation rather than guessing ownership. Labels authenticate
ownership only within the trusted local Docker boundary: a user with Docker/root
access can forge them and is already able to control workloads.

The result is a `RecoveredContainer`, distinct from a launchable `ContainerHandle`.
It exposes identity and observed status but cannot be passed to `Runtime::start`.
This preserves the v0.1 policy of stopping old executions rather than adopting them.

After successful registration has fenced the previous incarnation, the caller may
use `remove_previous(container, current_session)`. It refuses the current session,
re-inspects the full ID, and compares the saved identity/image/spec/profile before
mutation. It forcibly removes the old container, including paused workloads, without
unpausing it. Each daemon operation has a five-second bound. Success requires a
404 on the full ID from removal or subsequent inspection. A lost reply is uncertain;
retrying the same handle rechecks reality and treats absence idempotently.

Volume deletion is disabled; bind-mounted outputs, the attempt journal, and separate
volume data remain for their retention workflow. Container-local logs disappear on
removal, so accepted-result/log retention must not use this fenced-session path.
Mutable resource limits are not part of cleanup authorization. This API neither
registers a session nor certifies server fencing: its caller must establish that
precondition and repeat inventory after cleanup before reporting reconciliation.
It must continue heartbeats with reconciliation incomplete during a long cleanup.

## Verification

`sh scripts/test-runtime.sh` creates a unique workspace and job identity, uses the
pinned official Rust image, and removes only containers carrying that run's random
job label. It preserves unrelated containers, images, volumes, and other test runs.
`make integration` includes this check. Ordinary `make test` runs mock-daemon fault
tests; the real-daemon fixture is explicitly ignored until this script supplies its
isolated environment.

Observed on Docker Desktop 28.2.2, Linux arm64, using the macOS Rust client:

- Concurrent create and replay adopted one container; conflicting worker identity/spec
  was rejected.
- The workload checked its effective UID, capabilities, no-new-privileges, seccomp,
  mount flags, network isolation, and actual CPU/memory/swap/PID cgroup settings.
- It wrote an output, exited successfully, and returned separate bounded logs.
- Starting an exited attempt was rejected; running-container removal was refused.
- Stop/kill and repeated removal worked; resource drift blocked start but permitted
  cleanup of the owned container.
- A 128-MiB allocation fixture was OOM-killed; an explicit kill separately produced
  exit 137 without the OOM flag. Expired authority left a new container unstarted.
- A mock Unix-socket daemon committed create, dropped its response, and temporarily
  hid the result from inspection; the adapter
  recovered one ID with one create. Concurrent start replay initially exposed a
  duplicate-start race; the serialization regression test now passes.
- A stalled image-inspection request ended at its short local authority deadline.
- A fresh runtime client, without creation handles or journal container bindings,
  discovered paused, never-started, and exited old-session containers. It removed
  those containers, rejected current-session cleanup, and left both current-session
  and foreign-worker workloads running. Repeated removal was idempotent.
- Recovery fault tests reject overflow, duplicate IDs, foreign/malformed labels,
  changed ownership between discovery and cleanup, and a stalled inventory call.
  A lost delete reply is retried safely; a daemon reporting deletion while inspection
  still finds the container does not produce success. Native and Linux tests cover
  these cases. Parallel fixture creation uses a counter to avoid clock-resolution
  collisions observed during the first fault-test run.

These checks do not establish independent Linux worker hosts, strict scratch quotas,
durable agent recovery, live lease supervision, or the complete v0.1 execution flow.
The new recovery test replaces the runtime client, not the actual worker process.
Durable incarnation state and the agent's registration/cleanup/readiness sequence
remain necessary before claiming the spec's agent-kill/restart acceptance gate.

## References

- [Bollard 0.21.1](https://docs.rs/bollard/0.21.1/bollard/)
- [Docker resource constraints](https://docs.docker.com/engine/containers/resource_constraints/)
- [Docker seccomp profiles](https://docs.docker.com/engine/security/seccomp/)
- [Linux CFS bandwidth control](https://docs.kernel.org/scheduler/sched-bwc.html)
