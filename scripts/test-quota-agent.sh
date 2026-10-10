#!/bin/sh
set -eu
test "$(uname -s)" = Linux && test "$(id -u)" = 0 || {
    echo 'Run this fixture as root on a dedicated Linux worker VM.' >&2
    exit 1
}
test "$#" = 2 && test -x "$1" && test -x "$2" || {
    echo 'usage: test-quota-agent.sh SERVER_TEST_BINARY WORKER_BINARY' >&2
    exit 1
}
test -n "${DISPATCH_TEST_DATABASE_URL:-}" && test -n "${DISPATCH_TEST_S3_ENDPOINT:-}"
binary=$1
export DISPATCH_TEST_WORKER_BINARY=$2
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
fixture=$(mktemp -d /tmp/dispatch-quota-agent.XXXXXX)
device=
cleanup() {
    # The test writes its provisioned identity outside the job's mount tree.
    # Inventory failure retains storage rather than assuming no container lives.
    if test -f "$fixture/worker-id"; then
        worker=$(cat "$fixture/worker-id")
        ids=$(docker ps -aq --filter "label=dev.dispatch.worker=$worker") || return 1
        for id in $ids; do docker rm -f "$id" >/dev/null || return 1; done
    fi
    if mountpoint -q "$fixture/fs" && ! umount "$fixture/fs"; then
        echo "Unable to unmount agent fixture; retained $fixture and $device" >&2
        return 1
    fi
    if test -n "$device"; then losetup -d "$device"; fi
    rm -rf "$fixture"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP
truncate -s 256M "$fixture/disk"
device=$(losetup --find --show "$fixture/disk")
# Format only this fixture's new file-backed device. Agent admission needs its
# 64 MiB reservation plus headroom, so the primitive's 128 MiB disk is too small.
mkfs.ext4 -q -O project,quota -E quotatype=prjquota "$device"
mkdir "$fixture/fs"
mount -t ext4 -o prjquota,nosuid,nodev "$device" "$fixture/fs"
chmod 700 "$fixture/fs"
export DISPATCH_TEST_QUOTA_MOUNT="$fixture/fs"
cd "$repo/cmd/dispatch-server"
"$binary" -test.run '^TestWorkerDaemonEnforcesStrictScratchQuota$' -test.count=1 -test.v -test.timeout=120s
