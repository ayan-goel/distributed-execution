# Running the current control plane

The control plane can now admit and inspect queued jobs. Worker execution and
artifact retrieval are not implemented yet; queued jobs will remain queued.
The [worker startup command](worker-agent.md) now registers, cleans old Docker
containers, and reports health, but its acquisition/execution loop is still pending.

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
are bounded. Worker registration and heartbeat RPCs are available; the Rust agent
and job execution pipeline remain in progress.

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
verify uploaded versions; the Rust transfer pipeline and terminal job publication
are still in progress.

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
```

Use an HTTPS origin and omit `DISPATCH_DEV_INSECURE` for remote servers. The client
uses the system certificate trust store and refuses redirects. Environment tokens
are never included in normal output. Do not enable shell tracing while loading them.

Submission resolves the image tag to a verified digest. The example needs no
dataset and will eventually produce the deterministic sum 4,999,950,000, but at
this implementation stage it only queues. Execution and artifact download are
still pending. The server must permit Docker Hub via `--allow-registry index.docker.io`.

The CLI prints `Idempotency-Key` to stderr **before** sending a submission. If you
omit `--idempotency-key`, it generates a UUID. Save that key and reuse it with the
same file after a timeout, lost response, or output failure; using a new key can
create another job. Changing the request while reusing the key returns `CONFLICT`.
A successful submission means durable admission, not completed execution.

Options follow the file or job ID. Successful commands exit 0; validation, API,
configuration, transport, and output errors exit 2. `--json` writes one job object
to stdout for submit/get, or `{valid,specHash}` for validation; diagnostics stay on
stderr. Human submit/get output includes the job UUID and quoted state. Wait,
list, cancel, logs, datasets, and artifact commands will be added in their slices.

## Current verification

`make integration` verifies command entry points and a real HTTP listener with
PostgreSQL and a local registry: migrate, create project, issue token, submit, stop,
restart, replay while the registry is offline, and revoke the token. This verifies
durable queued-job recovery; active worker recovery remains a separate release gate.
The same test now exercises CLI submission and inspection, checks the stored image
digest/spec hash, and retries with the original key after restart during the registry
outage. CLI unit tests cover offline commands, invalid usage, terminal-safe errors,
and separation of recovery diagnostics from JSON output. Binary smoke tests validate
the checked-in CPU example through the actual `dispatch` executable.

Worker command integration verifies enrollment/takeover/revocation, then starts
both actual listeners, registers and reconciles a host over mTLS, restarts the
command, recovers the same session, and observes revocation on an existing channel.
Bad worker TLS fails before either listener is announced. A blocked-request test
verifies that HTTP-only forced shutdown closes requests within its supplied budget.
