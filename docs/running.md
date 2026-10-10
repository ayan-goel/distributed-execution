# Running the current control plane

The control plane admits jobs and parameter sweeps, exposes progress and logs,
and serves verified output downloads. The [worker agent](worker-agent.md) executes
jobs, including registered dataset inputs, with cached images under the explicit
soft-scratch development policy. The server command defaults to strict scratch;
opt into the local development profile with `--worker-dev-soft-scratch` on loopback
listeners. Hard scratch quotas and the independent Linux release profile remain
pending.

## Prerequisites

Build with `make build`. Provide a dedicated PostgreSQL database through
`DISPATCH_DATABASE_URL`; do not point development tests at production data.
Database credentials belong in the environment, not command-line arguments.
The test harness provisions disposable databases automatically via `make integration`.

## Operator setup

```sh
bin/dispatch-server migrate
bin/dispatch-server project create --name research --cpu-millis 4000 --memory-mib 8192 --concurrency 4
umask 077
mkdir -p .local
bin/dispatch-server token create --project research --role submit > .local/token.json
```

The token command outputs its ID and raw bearer token once. Keep the file outside
the repository or in ignored `.local/`; remove it after placing the token in your
credential storage. Do not paste it into logs or issues. Read-only and operator
tokens use `--role read` and `--role operator`.

```sh
bin/dispatch-server token revoke --project research --id TOKEN_UUID
```

Revocation is idempotent and takes effect on subsequent authenticated requests.

## Worker operator commands

Enroll the public client-authentication certificate for a dedicated Linux host:

```sh
bin/dispatch-server worker create --name worker-a --certificate /path/worker.crt --architecture arm64 --project research --cpu-millis 4000 --memory-mib 8192 --scratch-mib 16384 --slots 4
```

This outputs `{workerId,credentialId}`. Save both IDs. Repeat `--project` to authorize
additional projects and `--label rack=lab-a` for placement labels. `os=linux` and
the selected architecture cannot be overridden by generic labels. Advertised
resources at registration may be lower than these operator-approved ceilings.

Enrollment reads a bounded public PEM certificate chain, with the client leaf
first. It rejects private keys, CA leaves, expired certificates, and leaves without
client-authentication purpose. It stores only the leaf fingerprint; the worker
keeps its private key. Trust-chain verification happens at the mTLS listener, so
the leaf must be issued by that listener's configured client CA to connect.

```sh
bin/dispatch-server worker revoke --id WORKER_UUID --credential CREDENTIAL_UUID
bin/dispatch-server worker takeover --id WORKER_UUID --from-session OLD_SESSION_UUID --to-session NEW_SESSION_UUID
```

Revocation is idempotent and prevents subsequent authenticated worker operations.
It does not claim to stop containers. Takeover authorizes only the named replacement
session; fencing occurs when that incarnation registers successfully. Without an
approval, automatic recovery requires inactivity and expiry of all active leases.
These commands require direct operator database access, not a project bearer token.

## Listener

Explicit loopback-only development:

```sh
bin/dispatch-server serve --dev-insecure --listen 127.0.0.1:8080 --allow-registry index.docker.io
```

Remote deployment requires a TLS certificate/key:

```sh
bin/dispatch-server serve --listen :8443 --tls-cert /path/server.crt --tls-key /path/server.key --allow-registry index.docker.io
```

Enable worker gRPC in the same process with a server certificate/key and the CA
bundle allowed to issue worker client certificates:

```sh
bin/dispatch-server serve --listen :8443 --tls-cert /path/server.crt --tls-key /path/server.key --allow-registry index.docker.io --worker-listen :8444 --worker-tls-cert /path/server.crt --worker-tls-key /path/server.key --worker-client-ca /path/worker-ca.crt
```

The worker server certificate must be valid for the hostname/IP the agents connect
to; workers verify it against their configured server CA. The worker listener always
requires mTLS, including when HTTP uses `--dev-insecure` on loopback. Partial worker
TLS options are rejected. Omitting all worker options runs HTTP only.

To let the current agent acquire jobs on one development machine, start
both listeners on literal loopback IPs, use `--dev-insecure` for HTTP, and add
`--worker-dev-soft-scratch` to the worker listener options above. The switch is
rejected without this exact local setup; it permits soft scratch without a hard
quota. The agent also needs `--dev-soft-scratch`, a cached pinned Docker image,
and versioned object storage configured for output jobs.

TLS requires version 1.3 or newer. Repeat `--allow-registry` for additional approved
registries; use `--allow-registry-auth-host` for separate authentication hosts.
Private registry credential provisioning remains pending. Plain HTTP registry access
is restricted to explicit loopback development. A blank registry allowlist is rejected.

Startup checks/applies embedded migrations before listening; checksum drift or an
unknown schema version stops startup. JSON startup logs include the listen address
and TLS mode but no credentials. SIGINT/SIGTERM drain HTTP requests for up to 10
seconds before closing the database pool. HTTP and worker RPCs share this shutdown
budget; either listener failing stops the other. Both TLS configurations load and
both ports bind before startup events are logged. Header/body/write/idle timeouts
are bounded. Worker registration, heartbeat, acquisition, and completion RPCs are
available; the remaining job execution features are in progress.

Before opening listeners, the server terminalizes expired attempt leases in
PostgreSQL. It repeats this check every two seconds while running. An expired
attempt is recorded as lost, its physical reservation is quarantined, and its job
retries only when policy allows. Startup failure stops both listeners; periodic
database errors are logged and retried. Worker cleanup must still be proven by a
new incarnation before quarantined capacity is released.

The worker-loss integration gate kills an agent during a real Docker run, lets the
server reap its expired lease, and verifies that a second worker identity completes
the retry with the sole accepted artifact. One test advances expiry to isolate the
transition; another waits for the original 30-second lease. Both workers share one
Docker daemon.

## Configure artifact storage

For worker upload grants, add all three options to `dispatch-server serve`:

```text
--object-endpoint https://storage.example.org
--object-region us-east-1
--object-bucket dispatch-artifacts
```

Supply the backend's credentials through `DISPATCH_S3_ACCESS_KEY` and
`DISPATCH_S3_SECRET_KEY` in the server environment; `DISPATCH_S3_SESSION_TOKEN` is
optional. Ambient AWS variables/profiles are ignored. Workers receive scoped URLs,
never these credentials. Use a bucket with versioning enabled; startup checks it
before opening either listener, and each upload rechecks it.

For an explicitly local development backend, use a loopback HTTP endpoint and
`--object-dev-loopback`. HTTP's `--dev-insecure` does not enable plaintext storage.
Omitting storage settings leaves the other APIs available; `CreateUpload` reports
`OBJECT_STORAGE_NOT_CONFIGURED`. Partial settings fail startup. An AWS account is
not required: the integration fixture uses isolated local SeaweedFS. See
[upload capabilities](artifact-uploads.md) for limits and replay behavior, and
[verified artifacts](verified-artifacts.md) for `FinalizeUpload`. The server can
verify uploaded versions and accept terminal results through `CompleteAttempt`;
see [completion publication](completion.md). The agent recovers journaled
completions and delivers verified outputs for live jobs in the development path.
`GET /v1/jobs/{id}/artifacts` uses the same storage adapter for
project-authorized output download grants; see [artifact downloads](artifact-downloads.md).

## Submit and inspect from the CLI

Validate the CPU example without a server or credentials:

```sh
bin/dispatch validate examples/cpu/job.yaml --json
```

For the loopback server above, load the token from the operator setup file (this
example uses Python 3 to read JSON without printing the token):

```sh
export DISPATCH_URL=http://127.0.0.1:8080
export DISPATCH_DEV_INSECURE=1
export DISPATCH_TOKEN="$(python3 -c 'import json; print(json.load(open(".local/token.json"))["token"])')"
bin/dispatch submit examples/cpu/job.yaml --idempotency-key cpu-demo-1 --json
bin/dispatch jobs get JOB_UUID --json
bin/dispatch cancel JOB_UUID --json
```

A submit-role token can request cancellation through the CLI or
`POST /v1/jobs/JOB_UUID/cancel`. Queued jobs become `CANCELLED` immediately;
active jobs become `CANCELLING` and retain their reservation until stop is
confirmed or authority expires. Repeating the request returns the current job
without adding another event. For a running job, the worker now journals a
confirmed-stop acknowledgement, removes the container, and remains able to
accept another job. Cancellation is verified before launch and both before and
after normal completion is journaled during finalization. The wider fault
matrix still needs verification.

Use an HTTPS origin and omit `DISPATCH_DEV_INSECURE` for remote servers. The client
uses the system certificate trust store and refuses redirects. Environment tokens
are never included in normal output. Do not enable shell tracing while loading them.

Submission resolves the image tag to a verified digest. The example needs no
dataset and computes the deterministic sum 4,999,950,000. Running it currently
requires the explicit soft-scratch development switches and a cached pinned image;
the agent does not pull images yet. The server must permit Docker Hub via
`--allow-registry index.docker.io`.

The CLI prints `Idempotency-Key` to stderr **before** sending a submission. If you
omit `--idempotency-key`, it generates a UUID. Save that key and reuse it with the
same file after a timeout, lost response, or output failure; using a new key can
create another job. Changing the request while reusing the key returns `CONFLICT`.
A successful submission means durable admission, not completed execution.

For parameter grids, `dispatch sweep validate FILE --json` resolves a local
template and validates the expanded sweep offline. `dispatch sweep submit FILE
--idempotency-key KEY --json` submits all children together. Relative
`jobTemplateFile` references resolve beside the sweep file and never reach the
server. The [sweep guide](sweeps.md) includes the 27-child example, recovery rules,
concurrency cap, failure policy, and declared metric outputs. Inspect and export
results with:

```sh
bin/dispatch sweep get SWEEP_UUID --limit 50 --json
bin/dispatch sweep export SWEEP_UUID > results.json
bin/dispatch sweep export SWEEP_UUID --format csv > results.csv
```

Inspection returns one page; pass its `nextCursor` with `--cursor` to continue.
Export follows every page. Export after the sweep is terminal for final comparison;
running sweeps can still contain unfinished children.

Once all children are terminal, rerun only failed/cancelled children with fresh
linked job identities. Successful results and the source history stay intact:

```sh
bin/dispatch sweep retry SWEEP_UUID --idempotency-key retry-grid-1 --json
```

Save the recovery key printed to stderr and reuse the same source/key after an
uncertain response. A new key creates another retry sweep. Inspect/export using
the new ID in the response; its child mapping includes immediate parent job IDs.

For commands with a file or ID, options follow it. Successful commands exit 0; validation, API,
configuration, transport, and output errors exit 2. `dispatch wait` returns 1 for
a failed or cancelled job and 2 for timeout or interruption. For job commands, `--json`
writes one job object to stdout for submit/get/cancel, or `{valid,specHash}` for
validation; diagnostics stay on stderr. Human submit/get/cancel output includes
the job UUID and quoted state. `dispatch wait JOB_UUID --timeout 30m --json`
polls until terminal and emits one final job object. A timeout leaves the job
running unless `--cancel-on-timeout` is supplied. See [waiting for jobs](wait.md)
for exit codes, interruption, and cancellation behavior.
Use `bin/dispatch jobs list --project research --state QUEUED --label cohort=alpha`
to find work. Repeat `--label KEY=VALUE` for AND matching; add `--json` for the full
page envelope. `--limit` accepts 1–100 (default 50). When `hasMore` is true, pass
the returned `nextCursor` with `--cursor` and the same filters. The project defaults
to the token's project. See [job listing](job-listing.md) for bounds and live-page
semantics. Use `bin/dispatch logs JOB_UUID`
to inspect verified segments, or `bin/dispatch logs JOB_UUID --follow` to poll
for new segments while the job runs.

To register a local directory as one immutable dataset:

```sh
bin/dispatch dataset upload ./data --name data-v1 --json
```

The command builds a deterministic archive of up to 1,024 regular files and
64 MiB, then prints a request ID, upload ID, and exact object version to stderr
as each becomes available. If completion returns an uncertain error after the
version was printed, rerun with the same directory and name plus
`--request-id UUID --resume-version VERSION`. This completes the previously
uploaded version without another PUT. Keep those recovery values private.
Reference a registered name in the job's `spec.inputs`:

```yaml
inputs:
  - dataset: data-v1
    mountPath: /inputs/data
```

The name must belong to the submitting project; missing or cross-project names
return 404. Admission pins the immutable dataset version. The worker downloads and
verifies it into its private cache, refreshes signed grants before staging, and
mounts the declared path read-only. Configure versioned object storage for both
registration and execution. See [HTTP dataset admission](http-api.md) and
[worker cache configuration](worker-agent.md).

For a successfully completed job with an accepted output named `result`:

```sh
bin/dispatch artifacts download JOB_UUID result --output result.json --json
```

This checks the accepted object version, size, and SHA-256 before atomically
publishing a new 0600 local file. The destination directory must exist, and existing
files/symlinks are never overwritten. JSON output is a verified download receipt;
no signed URL or storage credential is printed. See
[artifact downloads](artifact-downloads.md) for transfer bounds and failure behavior.

## Current verification

`make integration` verifies command entry points and a real HTTP listener with
PostgreSQL and a local registry: migrate, create project, issue token, submit, stop,
restart, replay while the registry is offline, and revoke the token. This verifies
durable queued-job recovery; the active-worker fault matrix has additional release gates.
The command test checks the stored image digest/spec hash and retries the original
key after restart during the registry outage. CLI unit tests cover offline commands,
invalid usage, terminal-safe errors, and separation of recovery diagnostics from
JSON output. Binary smoke tests validate the checked-in CPU example through the
actual `dispatch` executable.

One active-job recovery gate now kills an agent-owned Docker job, approves a named
replacement session, and verifies fencing and physical cleanup before readiness.
The actual worker daemon also acquires a dataset-backed job through the real gRPC
service, reads its verified read-only input in Docker, publishes an exact-version
output to local SeaweedFS, and cleans up after completion. CLI submission and
verified download cover both ends of the path. The fixture uses explicit
soft-scratch acquisition policy and a fixed
resolver for an already cached digest; external registry access and image pulling
are separate work.

A 27-child sweep also completes across three enrolled native worker processes
sharing Docker Desktop, with two active attempts observed and exact metrics,
pagination, JSON/CSV exports, and artifact downloads verified. Live fail-fast
tests also verify queued-only and running-sibling cancellation after a permanent
failure. Another local 27-child sweep recovers after killing a running worker,
using natural lease expiry and a replacement on another worker. Independent Linux
hosts and the broader failure matrix remain open.

The sweep retry command is also verified with a real uploaded dataset and mixed
source outcomes: one failed after natural worker loss, one successful, and one
cancelled by fail-fast. Only the failed/cancelled jobs are recreated. Both new
jobs execute on remaining workers and publish downloadable results containing
the frozen input and fresh identities. The original history/results remain intact.

A separate three-child test delays a real successful completion before acceptance,
kills its worker, and lets the natural lease expire. After replacement succeeds,
the exact old request is rejected over mTLS as `ALREADY_TERMINAL/LOST`. The accepted
manifest stays unchanged, and CLI download returns the replacement's distinct
attempt-ID bytes. Old verified output remains diagnostic; fixture cleanup does
not demonstrate reconciliation of the killed worker.

Worker command integration verifies enrollment/takeover/revocation, then starts
both actual listeners, registers and reconciles a host over mTLS, restarts the
command, recovers the same session, and observes revocation on an existing channel.
Bad worker TLS fails before either listener is announced. A blocked-request test
verifies that HTTP-only forced shutdown closes requests within its supplied budget.

### Local test host power state

Keep the development host awake during lease-sensitive runtime tests. On macOS,
the command below temporarily prevents idle sleep for the test command's lifetime;
it does not prevent lid-close or manually requested sleep:

```sh
caffeinate -i sh scripts/test-objectstore.sh sh scripts/test-store.sh
```

The disposable object-store wrapper waits for Docker to publish its loopback port
before passing the endpoint to tests. Docker Desktop can report the port late;
after 40 unsuccessful lookups the wrapper fails and cleans up its own container.
Delayed-publication and never-published fixture checks verify both paths. This
startup wait does not change job leases or runtime timeout assertions.

Suspending the host can stop every native worker while database wall time advances,
causing additional legitimate lease losses. Tests that deliberately kill one worker
and assert exactly one loss then fail for a different scenario. Check sleep/wake
history before classifying such a failure; keep lease deadlines and assertions
intact. These local tests still do not prove independent Linux-host behavior.
