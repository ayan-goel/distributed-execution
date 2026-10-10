#!/bin/sh
set -eu
test "$#" = 1 && test -x "$1" || {
    echo 'usage: test-independent-workers.sh LINUX_ARM64_WORKER_BINARY' >&2
    exit 1
}
repo=$(pwd -P)
mkdir -p "$repo/.local/verification"
hosts="$repo/.local/verification/sweep-vms.json"
python3 - "$repo" "$hosts" <<'PY'
import json, pathlib, sys
repo = pathlib.Path(sys.argv[1])
hosts = [
    {"port": port, "key": str(repo / ".local" / name / "id_ed25519"),
     "knownHosts": str(repo / ".local" / name / "known_hosts")}
    for port, name in [(22231, "quota-vm"), (22232, "sweep-worker-2-vm")]
]
path = pathlib.Path(sys.argv[2])
path.write_text(json.dumps(hosts, indent=2) + "\n")
path.chmod(0o600)
PY
for name in quota sweep-worker-2; do
    port=22231
    if test "$name" = sweep-worker-2; then port=22232; fi
    root="$repo/.local/$name-vm"
    # Host keys must already be verified during VM setup. Never relax SSH
    # identity checks while sending the worker or later fixture credentials.
    scp -q -i "$root/id_ed25519" -o BatchMode=yes -o IdentitiesOnly=yes \
        -o StrictHostKeyChecking=yes -o UserKnownHostsFile="$root/known_hosts" \
        -P "$port" "$1" "dispatch@127.0.0.1:/tmp/dispatch-worker"
    scp -q -i "$root/id_ed25519" -o BatchMode=yes -o IdentitiesOnly=yes \
        -o StrictHostKeyChecking=yes -o UserKnownHostsFile="$root/known_hosts" \
        -P "$port" scripts/worker-vm-fixture.sh \
        "dispatch@127.0.0.1:/tmp/dispatch-worker-vm-fixture.sh"
done
export DISPATCH_TEST_WORKER_VMS="$hosts"
export GOCACHE="$repo/.local/go-build" GOPATH="$repo/.local/go" GOTOOLCHAIN=local
sh scripts/test-objectstore.sh sh scripts/test-store.sh "$repo/.tools/go/bin/go" \
    test -race -tags integration -run '^TestIndependentLinuxWorkers' \
    -count=1 -v -timeout 10m ./cmd/dispatch-server
