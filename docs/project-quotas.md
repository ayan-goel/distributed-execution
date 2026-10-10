# Strict scratch: ext4 project-quota foundation

The Linux quota primitive is implemented and verified on a separate Debian VM.
It is not yet connected to worker admission or workspace creation. The agent still
requires the explicit soft-scratch development profile and advertises `scratch.soft`.

The selected release filesystem is ext4 with project quotas enabled. An attempt's
private directory receives a unique project ID and hard byte/inode limits before
any mount children are created. Inheritance charges nested scratch and output
files to the same project; the input cache stays outside that project. Disk limits
count allocated filesystem space, including directory overhead, rather than only
application payload bytes. Limits use exact 1024-byte quota units.

`ProjectQuota::attach_empty` requires a canonical, owned, mode-0700 empty directory,
Linux 5.14 or newer, and permission to administer quotas. It rejects existing
project ownership, non-ext4 filesystems, disabled quotas, and project IDs with
existing limits or usage. Setup checks the kernel's project accounting/enforcement flags, then reads back
the installed limits and inheritance. Verification rechecks enforcement; readable
limits alone do not establish an enforced quota.
Errors retain any installed limits. The caller must serialize allocation and
quarantine IDs after uncertain setup; dropping the handle does not clear quotas.

Before the worker can advertise `scratch.quota`, the next slice must provide:

- Durable project-ID allocation across restart and exclusive filesystem ownership.
- A runtime restriction against project retagging or clearing inheritance through
  `FS_IOC_FSSETXATTR`/`FS_IOC_SETFLAGS`. Unprivileged ownership alone is insufficient.
- Agent configuration, strict workspace preparation, verified cleanup, and runtime
  mount/profile checks, followed by a real quota-exhausting container job.

These are enforcement requirements, not optional hardening. The primitive test
does not establish container isolation or completed strict-scratch integration.

## Reproduce the filesystem gate

On Apple Silicon macOS with Homebrew QEMU installed, start the dedicated VM:

```sh
sh scripts/run-quota-vm.sh
```

The script verifies a pinned Debian 13 ARM64 image checksum, generates a private
fixture SSH key, and runs QEMU in the foreground with 2 vCPUs, 1 GiB RAM, and SSH
bound to `127.0.0.1:22231`. State stays under ignored `.local/quota-vm/`; the base
image is about 322 MiB and the writable disk is sparse with a 4 GiB virtual ceiling.
Keep the foreground process running while testing. The default port can be changed
with `DISPATCH_QUOTA_VM_PORT`.

Build the Linux `project_quota` test binary with the pinned worker toolchain
(`cargo test --locked -p dispatch-worker --test project_quota --no-run`). Copy it
and `scripts/test-project-quota.sh` into the VM, then run:

```sh
sudo sh /tmp/test-project-quota.sh /tmp/project_quota-<build-hash>
```

The fixture creates a new 128 MiB loop-backed ext4 filesystem, formats only its
new device, mounts it with `prjquota,nosuid,nodev`, runs the gate, then unmounts and
detaches that device. Cleanup failure retains evidence rather than detaching a
still-mounted filesystem. Never point formatting commands at an existing disk.

The gate writes as UID/GID 65532. It verifies an 8 MiB project returns `EDQUOT`,
outputs share the exhausted scratch budget, an independent project still writes,
and a 16-inode project returns `EDQUOT` before consuming its byte allowance.
It also rejects symlink paths, repeated attachment, and project-ID collision.
A second mount without `prjquota` retains accounting but must reject quota setup.

Verified locally on Debian kernel `6.12.111+deb13-cloud-arm64`, with ext4 mounted
`rw,nosuid,nodev,relatime,prjquota`. Docker Desktop kernel `6.10.14-linuxkit` rejected
the initial mount because it lacks `CONFIG_QUOTA` and `CONFIG_QFMT_V2`; it cannot
substitute for this gate. AMD64 and the full worker/runtime profile remain unverified.

Shut down the test VM with its disposable key:

```sh
ssh -i .local/quota-vm/id_ed25519 -o IdentitiesOnly=yes \
  -o UserKnownHostsFile=.local/quota-vm/known_hosts \
  -p 22231 dispatch@127.0.0.1 sudo poweroff
```

The implementation follows Linux's [quota syscall contract](https://www.man7.org/linux/man-pages/man2/quotactl_fd.2.html),
[filesystem attribute UAPI](https://github.com/torvalds/linux/blob/v6.12/include/uapi/linux/fs.h),
and [quota UAPI](https://github.com/torvalds/linux/blob/v6.12/include/uapi/linux/quota.h).
VM initialization uses cloud-init's [NoCloud datasource](https://docs.cloud-init.io/en/latest/reference/datasources/nocloud.html).
