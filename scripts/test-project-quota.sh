#!/bin/sh
set -eu
test "$(uname -s)" = Linux && test "$(id -u)" = 0 || {
    echo 'Run this fixture as root on a dedicated Linux VM.' >&2
    exit 1
}
test "$#" = 1 && test -x "$1" || {
    echo 'usage: test-project-quota.sh /absolute/path/to/project_quota-test-binary' >&2
    exit 1
}
binary=$1
fixture=$(mktemp -d /tmp/dispatch-project-quota.XXXXXX)
chmod 755 "$fixture"
device=
cleanup() {
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
# Quota inodes still provide accounting without prjquota. Stored limits alone
# must not authorize strict scratch when the filesystem stops enforcing them.
umount "$fixture/fs"
mount -t ext4 -o nosuid,nodev "$device" "$fixture/fs"
findmnt -n -o FSTYPE,OPTIONS --target "$fixture/fs/work"
DISPATCH_QUOTA_TEST_ROOT="$fixture/fs/work" "$binary" --ignored \
    --exact accounting_without_enforcement_cannot_prepare_strict_scratch --nocapture
