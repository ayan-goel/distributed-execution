# Running the current control plane

The control plane can now admit and inspect queued jobs. Worker execution and
artifact retrieval are not implemented yet; queued jobs will remain queued.

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

## Listener

Explicit loopback-only development:

```sh
bin/dispatch-server serve --dev-insecure --listen 127.0.0.1:8080 --allow-registry index.docker.io
```

Remote deployment requires a TLS certificate/key:

```sh
bin/dispatch-server serve --listen :8443 --tls-cert /path/server.crt --tls-key /path/server.key --allow-registry index.docker.io
```

TLS requires version 1.3 or newer. Repeat `--allow-registry` for additional approved
registries; use `--allow-registry-auth-host` for separate authentication hosts.
Private registry credential provisioning remains pending. Plain HTTP registry access
is restricted to explicit loopback development. A blank registry allowlist is rejected.

Startup checks/applies embedded migrations before listening; checksum drift or an
unknown schema version stops startup. JSON startup logs include the listen address
and TLS mode but no credentials. SIGINT/SIGTERM drain HTTP requests for up to 10
seconds before closing the database pool. Header/body/write/idle timeouts are bounded.

## Current verification

`make integration` verifies command entry points and a real HTTP listener with
PostgreSQL and a local registry: migrate, create project, issue token, submit, stop,
restart, replay while the registry is offline, and revoke the token. This verifies
durable queued-job recovery; active worker recovery remains a separate release gate.
