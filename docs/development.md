# Local development dependencies

Requires Docker Compose v2, Python 3, and the pinned Go toolchain. From the repo root:

```sh
make dev-up
. .local/dev/env
make build
bin/dispatch-server migrate
```

`dev-up` starts PostgreSQL 17.11 and SeaweedFS 4.47 with pinned image digests,
private Compose volumes, resource limits, and dynamically assigned loopback ports.
It waits for PostgreSQL and initializes/checks versioning on the `dispatch-dev`
bucket before reporting success. It needs no AWS account or AWS CLI.

Generated credentials live in ignored `.local/dev/compose.env`; shell-ready
connection settings live in `.local/dev/env`. Both are created with private file
permissions. Load the latter again after recreating containers because published
ports can change. Keep the credential file: existing volumes still use those
credentials. Do not print either file or commit them. Ambient AWS credentials and
development credential variables do not override the saved stack credentials.

To start the HTTP control plane after creating a project/token as described in
the [operator guide](running.md):

```sh
bin/dispatch-server serve --dev-insecure --listen 127.0.0.1:8080 \
  --allow-registry index.docker.io \
  --object-endpoint "$DISPATCH_OBJECT_ENDPOINT" \
  --object-region "$DISPATCH_OBJECT_REGION" \
  --object-bucket "$DISPATCH_OBJECT_BUCKET" --object-dev-loopback
```

This task starts dependencies only. Real execution also requires an enrolled
Linux worker and mTLS listener; follow the [worker guide](worker-agent.md) and
[strict scratch setup](project-quotas.md). On macOS, use the dedicated Linux VM
profile. Native macOS workers are outside the supported v0.1 target.

## Stop and resume

```sh
make dev-down
make dev-up
. .local/dev/env
```

`dev-down` removes only the `dispatch-dev` Compose services/network. It preserves
the two named volumes and local credentials. Repeated `dev-up` reuses them and
rechecks bucket versioning. There is no automatic data reset or volume deletion.
The small development storage pool is bounded to two 64 MiB SeaweedFS volumes;
use a separately sized backend for larger workloads. This is a local fixture,
not the production storage or backup profile.

## Verification

The initial gate wrote a PostgreSQL marker and two distinct immutable versions
of one object, ran `dev-down`/`dev-up` with deliberately conflicting ambient
credentials, and then read the marker and both exact original versions. It removed
only its own marker/table and object versions afterward. Endpoint rejection tests
prevent the initializer from operating on nonlocal storage. These checks establish
local dependency persistence, not a complete deployment or backup/restore gate.

Compose lifecycle semantics follow [Docker's `compose up` documentation](https://docs.docker.com/reference/cli/docker/compose/up/).
The pinned backend uses [SeaweedFS 4.47 mini](https://github.com/seaweedfs/seaweedfs/blob/4.47/weed/command/mini.go).
