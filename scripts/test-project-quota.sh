#!/bin/sh
set -eu
test "$(uname -s)" = Linux && test "$(id -u)" = 0 || {
    echo 'Run this fixture as root on a dedicated Linux VM.' >&2
    exit 1
}
test "$#" -ge 1 && test "$#" -le 2 && test -x "$1" || {
    echo 'usage: test-project-quota.sh PROJECT_QUOTA_BINARY [QUOTA_RUNTIME_BINARY]' >&2
    exit 1
}
binary=$1
runtime_binary=${2:-}
job=
if test -n "$runtime_binary"; then
    test -x "$runtime_binary"
    job=$(cat /proc/sys/kernel/random/uuid)
    export DISPATCH_QUOTA_TEST_JOB="$job"
    export DISPATCH_QUOTA_TEST_IMAGE='debian@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587'
    docker image inspect "$DISPATCH_QUOTA_TEST_IMAGE" >/dev/null
fi
fixture=$(mktemp -d /tmp/dispatch-project-quota.XXXXXX)
chmod 755 "$fixture"
device=
cleanup() {
    # Stop only this fixture's randomly labelled job before unmounting storage.
    # Failed tests must not leave a container holding the owned loop mount open.
    if test -n "$job"; then
        ids=$(docker ps -aq --filter "label=dev.dispatch.job=$job") || return 1
        for id in $ids; do
            docker rm -f "$id" >/dev/null || return 1
        done
    fi
    # Never detach a device whose mount could still contain active test writes.
    # Retain the fixture on cleanup failure so an operator can inspect it safely.
    if mountpoint -q "$fixture/fs" && ! umount "$fixture/fs"; then
        echo "Unable to unmount quota fixture; retained $fixture and $device" >&2
        return 1
    fi
    if test -n "$device"; then losetup -d "$device"; fi
    rm -rf "$fixture"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP
truncate -s 128M "$fixture/disk"
device=$(losetup --find --show "$fixture/disk")
# Format only the newly allocated loop device backed by this fixture's own file.
# Existing host filesystems and quota configuration are never modified.
mkfs.ext4 -q -O project,quota -E quotatype=prjquota "$device"
mkdir "$fixture/fs"
mount -t ext4 -o prjquota,nosuid,nodev "$device" "$fixture/fs"
mkdir "$fixture/fs/work"
chmod 755 "$fixture/fs/work"
uname -r
findmnt -n -o FSTYPE,OPTIONS --target "$fixture/fs/work"
DISPATCH_QUOTA_TEST_ROOT="$fixture/fs/work" "$binary" --ignored \
    --exact ext4_limits_cover_nested_outputs_and_scratch_without_affecting_siblings --nocapture
if test -n "$runtime_binary"; then
    DISPATCH_QUOTA_TEST_ROOT="$fixture/fs/work" "$runtime_binary" --ignored \
        --exact real_container_cannot_escape_its_project_quota --nocapture
fi
# Quota inodes still provide accounting without prjquota. Stored limits alone
# must not authorize strict scratch when the filesystem stops enforcing them.
umount "$fixture/fs"
mount -t ext4 -o nosuid,nodev "$device" "$fixture/fs"
findmnt -n -o FSTYPE,OPTIONS --target "$fixture/fs/work"
DISPATCH_QUOTA_TEST_ROOT="$fixture/fs/work" "$binary" --ignored \
    --exact accounting_without_enforcement_cannot_prepare_strict_scratch --nocapture
