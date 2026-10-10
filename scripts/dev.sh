#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
umask 077
state=.local/dev
compose() (
    # Saved credentials must win over ambient AWS/development shell settings.
    set -a
    . "./$state/compose.env"
    set +a
    docker compose --project-name dispatch-dev --env-file "$state/compose.env" -f deploy/dev/compose.yaml "$@"
)
case "${1:-}" in
    up)
        mkdir -p "$state"
        if [ ! -f "$state/compose.env" ]; then
            # Keep credentials stable across restarts so retained volumes remain usable.
            python3 - <<'PY'
import pathlib, secrets
p = pathlib.Path('.local/dev/compose.env')
with p.open('x') as f:
    for name in ('POSTGRES_PASSWORD', 'DISPATCH_S3_ACCESS_KEY', 'DISPATCH_S3_SECRET_KEY'):
        f.write(f'{name}={secrets.token_hex(24)}\n')
PY
        fi
        compose up --wait --wait-timeout 60
        db_mapping=$(compose port postgres 5432)
        storage_mapping=$(compose port storage 8333)
        export DISPATCH_DEV_DB_PORT=${db_mapping##*:}
        export DISPATCH_DEV_S3_PORT=${storage_mapping##*:}
        python3 - <<'PY'
import os, pathlib, shlex
root = pathlib.Path('.local/dev')
credentials = dict(line.split('=', 1) for line in (root/'compose.env').read_text().splitlines())
ports = [os.environ[k] for k in ('DISPATCH_DEV_DB_PORT', 'DISPATCH_DEV_S3_PORT')]
assert all(p.isdigit() and 1024 <= int(p) <= 65535 for p in ports), 'invalid published port'
values = {
    'DISPATCH_DATABASE_URL': f"postgres://postgres:{credentials['POSTGRES_PASSWORD']}@127.0.0.1:{ports[0]}/dispatch?sslmode=disable",
    'DISPATCH_S3_ACCESS_KEY': credentials['DISPATCH_S3_ACCESS_KEY'],
    'DISPATCH_S3_SECRET_KEY': credentials['DISPATCH_S3_SECRET_KEY'],
    'DISPATCH_S3_SESSION_TOKEN': '',
    'DISPATCH_OBJECT_ENDPOINT': f'http://127.0.0.1:{ports[1]}',
    'DISPATCH_OBJECT_BUCKET': 'dispatch-dev',
    'DISPATCH_OBJECT_REGION': 'us-east-1',
}
temporary = root/'env.tmp'
temporary.write_text(''.join(f'export {k}={shlex.quote(v)}\n' for k,v in values.items()))
temporary.replace(root/'env')
PY
        . "./$state/env"
        "${GO:-go}" run ./deploy/dev
        echo 'Dependencies ready. Load settings: . .local/dev/env'
        ;;
    down)
        # Preserve volumes and credentials; teardown never deletes development data.
        compose down
        ;;
    *) echo 'usage: sh scripts/dev.sh up|down' >&2; exit 2 ;;
esac
