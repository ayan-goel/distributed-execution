# Worker startup and health loop

The actual `dispatch-worker` executable now opens its journal, registers a fresh
incarnation over mTLS, reconciles previous-session Docker containers, and reports
health/readiness periodically. Startup also delivers journaled completion requests
and persists their authoritative outcomes. Job acquisition and per-attempt execution are not
connected yet. A READY worker currently remains idle; this is not the completed
CLI-to-result execution path.

## Development setup

Build with `make build`, enroll the worker certificate with the operator commands
in [running.md](running.md), and start the server's worker TLS listener. The current
agent supports the explicit soft-scratch development profile:

```sh
dispatch-worker run --config /absolute/path/worker.json --dev-soft-scratch
```

The checkout binary is `.local/cargo-target/debug/dispatch-worker`. The flag is
required: quota-backed scratch and the strict Linux release profile remain pending.
The development client may run on macOS against a local Docker Desktop Linux daemon;
that does not establish an independent Linux worker deployment.

Example configuration (replace IDs, endpoints, labels, and paths with the enrolled
worker's values):

```json
{
  "worker_id": "00000000-0000-0000-0000-000000000001",
  "server_url": "https://dispatch.example:8444",
  "ca_cert": "/var/lib/dispatch/credentials/ca.pem",
  "client_cert": "/var/lib/dispatch/credentials/worker.pem",
  "client_key": "/var/lib/dispatch/credentials/worker-key.pem",
  "journal_dir": "/var/lib/dispatch/state",
  "workspace_root": "/var/lib/dispatch/work",
  "docker_socket": "/var/run/docker.sock",
  "cpu_millis": 2000,
  "memory_mib": 1024,
  "scratch_mib": 4096,
  "execution_slots": 2,
  "labels": { "os": "linux", "architecture": "arm64" }
}
```

Create the journal and workspace parent as the service user with mode 0700 before
starting. They must be separate directories; neither may contain the other. All
configured filesystem paths are absolute. Local ancestor directories and the Docker
socket are trusted operator configuration. The workspace parent is never a job mount.
Keep credentials outside future per-attempt workspace directories.

The JSON config is at most 64 KiB. Unknown/duplicate struct fields, duplicate label
keys, invalid endpoint origins, and invalid capacity bounds fail before network
registration. Credential files are bounded to 1 MiB, regular files, and not symlink
leaves. The private key additionally requires effective-user ownership, no group or
other permission bits, and one hard link. File/parser diagnostics omit contents and
paths. TLS uses only the explicitly configured server CA and client identity.

Labels must exactly match enrollment. Resources and slots cannot exceed enrollment
ceilings. Docker's actual architecture, CPU count, and total memory also bound the
claims; configure allocatable memory below total memory to leave host overhead.
The daemon must support the existing hard CPU/memory/PID/seccomp checks. Scratch is
advertised as `scratch.soft`, never `scratch.quota`. The server's default acquisition
policy continues to reject soft-scratch execution.

## Startup and retry ordering

1. Validate private local state and credentials, acquire the journal lock, and
   durably save a fresh registration request/session.
2. Emit a `session_pending` JSON event with worker/session IDs. Connect to Docker,
   validate host capacity, and establish the mTLS control connection.
3. Retry registration with the same stored IDs and claims on transient connection
   errors or `SESSION_ACTIVE`. The latter waits for the server's existing automatic
   inactivity/lease recovery policy or an explicit operator takeover approval.
4. Persist the matching registration acknowledgement before reporting readiness.
5. Inspect this worker's Docker inventory and send a sequenced health report.
   Remove at most one previous-session container per iteration, with identity
   revalidation. Keep health reports interleaved during cleanup.
6. After fresh inventory proves no containers remain, send a new healthy,
   reconciled report. Announce `ready` only when the server also confirms no
   reconciliation/stop instructions and no drain request.

The preceding saved session ID can be an unaccepted request. For manual takeover,
use the server's actual current session as `--from-session` and this process's
`session_pending` ID as `--to-session`. Approval is still the explicit operator
command documented in [worker-sessions.md](worker-sessions.md); the agent never
approves itself or bypasses the inactivity window.

Each pending heartbeat retains its UUID, sequence, and full payload until a reply
arrives. Cleanup continues after transient heartbeat errors because registration
already fenced the predecessor. A stale pending snapshot cannot announce readiness:
the current local observation must also be healthy, empty, and free of disk pressure.
After accepting an old report, the next report gets a higher sequence and new UUID.
Runtime inventory errors produce an unhealthy report, never successful empty-state
reconciliation. Unknown current-session containers remain unreconciled; this startup
loop does not adopt or delete them through previous-session cleanup.

Available space on the workspace filesystem must cover configured scratch capacity
plus 64 MiB of headroom. A failed space check reports disk pressure. This conservative
check is not a filesystem quota. File sync and space checks run in blocking tasks;
steady-state health reports normally occur every five seconds, while uncertain
requests retry after one second. RPC/runtime deadlines remain in their adapters.
There is no active-job lease maintenance in this loop yet.

JSON state-change events are `session_pending`, `registered`, `reconciling`, `ready`,
`draining`, and `control_unavailable`. `completion_recovered` reports a durably
resolved attempt with its attempt ID and numeric protocol decision/state; it never
includes manifest, spec, metrics, or credentials. Terminal fencing/authentication/conflict
errors exit nonzero. The exclusive journal lock remains held throughout the process,
so a second process using the same state directory exits with `Busy`. Signal-driven
job drain/termination is not implemented; this slice has no acquired jobs, and normal
OS process termination releases the journal lock.

## Completion recovery (D11q)

After recording its new registration, the agent starts a bounded scan of the
journal alongside the health/cleanup loop. Registration first fences the preceding
session. Recovery never resumes its execution authority or starts containers.
Each record without a completion is skipped. A validated accepted/fenced/terminal
reply already on disk needs no network request. Otherwise `deliver_pending` sends
the exact saved request over mTLS and syncs its validated reply before returning.

The scan retains only the journal's bounded attempt-ID inventory (at most 4096)
and loads one record at a time. At most one completion RPC is outstanding. Retryable
transport errors and STOP_REQUESTED are requeued with a one-second delay so other
records can progress. A cloned RPC handle and asynchronous journal access keep
recovery waits separate from heartbeats and physical container cleanup. Terminal
RPC errors, conflicting replies, or corrupt journal evidence end the agent with
the original evidence retained. Cancelling a filesystem await still follows the
journal's existing ambiguous-write/reopen rules.

An accepted historical completion is replayable after session replacement. A
completion that never committed is rejected after the old attempt is fenced; it
must not become a newly accepted result. A recovered rejection is an outcome, not
a reason to re-execute that attempt. The server's retry policy owns replacement work.

This scans startup evidence only. Live acquisition/execution must still call the
delivery component after output verification. Cancellation supersession, local
capacity/workspace cleanup, and retention of resolved records remain required.

## Verification and limits

`TestWorkerProcessReconcilesDockerBeforeAdvertisingReady` launches the actual worker
binary against real PostgreSQL, the authenticated Go gRPC service, and Docker. It
seeds an old registration/container, explicitly approves the named replacement,
checks every complete health report against Docker, and verifies generation 2,
READY state, and one durable registration request. It loses a heartbeat reply after
the database commit and verifies an unchanged replay while cleanup proceeds. A
second worker process cannot share the journal; credential revocation terminates
the running process on its next report.

The completion recovery integration tests seed synthetic local execution evidence
for a real PostgreSQL attempt with a verified artifact. One case commits completion
and withholds its reply while the Rust delivery process is killed. The actual
worker executable then registers a new session, retries a transient server failure,
recovers the original manifest, and persists it. A second restart reads that reply
without resending completion. Another case seeds an unaccepted request: session
takeover marks the old attempt LOST, and recovery persists rejection without any
completion row, accepted result, or completion event. Both use real mTLS and Docker
inventory access; the fixture does not run the workload itself.

`make integration` runs this alongside Linux worker tests, real Docker lifecycle
checks, migrations, and existing Go/Rust mTLS workflows. `make test lint smoke`
covers config rejection, binary argument handling, protocol regression, and lint.

The old container in this test is fixture-created. The stronger release gate still
requires the agent itself to acquire/start a job, be killed while it runs, restart,
and recover through this startup path. Acquisition/supervision, strict scratch,
transfers/live completion, signal shutdown, multi-host verification, and the rest of
v0.1 remain required.
