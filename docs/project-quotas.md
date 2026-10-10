# Strict scratch: ext4 project quotas and Docker

The Linux quota primitive and Docker enforcement profile are implemented and
verified on a separate Debian VM. They are not yet connected to agent workspace
allocation or worker admission. The agent still
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

`journal::ProjectIds` now persists worker-bound project IDs through the existing
exclusive, checksummed journal writer. Initialize an empty private control
directory once; reopen requires its identity and counter. Each reservation commits
the next counter before returning an ID. IDs are never recycled, including after
failed quota setup. Missing/corrupt state, write uncertainty, and namespace
exhaustion fail closed. The control directory must remain outside workload mounts;
strict integration must still ensure one allocator owns the entire filesystem.

Before the worker can advertise `scratch.quota`, the remaining integration must provide:

- Exclusive filesystem ownership and integration of the durable project-ID allocator.
- Agent configuration, quota-backed workspace preparation, health checks, and
  verified cleanup, followed by a submitted job through the complete agent path.

These are enforcement requirements. The component gates do not establish completed
agent integration or authorize the existing development worker to claim strict scratch.

## Runtime enforcement profile

`PreparedWorkspace::project_quota` retains a verified Linux quota handle. Before
Docker creation, the quota must still belong to the workspace's exact directory
inode and its byte limit must match the job's scratch reservation. Container
labels record `ext4-project-quota-v1`; recovery recognizes this profile for owned
container cleanup. The development profile remains explicit and separately labelled.

The strict runtime requires a rootful Docker daemon with `userns-remap` enabled
and its built-in seccomp profile. Capability checks run before create and start,
so a daemon restart cannot silently remove the remapping requirement. Inspection
rejects `UsernsMode=host`; jobs cannot supply namespace or seccomp overrides.
The worker must also check this capability before advertising strict readiness.

This restriction protects quota attributes, not just process privileges. In the
initial user namespace, an unprivileged inode owner can change project attributes.
Linux rejects project-ID changes and clearing inheritance from a remapped user
namespace through both `FS_IOC_FSSETXATTR` and `FS_IOC_SETFLAGS`. The runtime retains
Docker's default syscall restrictions instead of substituting a permissive custom
seccomp profile. Rootless Docker is outside this selected release profile.

On a new dedicated worker daemon, the operator configuration is:

```json
{"userns-remap":"default"}
```

Use Docker's [user namespace setup instructions](https://docs.docker.com/engine/security/userns-remap/),
including subordinate UID/GID allocation and bind-mount permissions. Enabling this
on an existing daemon changes which stored Docker objects are visible. The gate
uses its own VM daemon; it does not reconfigure Docker Desktop.

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

Build the Linux test binaries with the pinned worker toolchain:

```sh
cargo test --locked -p dispatch-worker --test project_quota --test quota_runtime --no-run
```

Copy the binaries and `scripts/test-project-quota.sh` into the VM. The primitive
gate needs only the first binary. To include the real container gate, configure
the dedicated daemon as above, pull the pinned image, and provide both binaries:

```sh
sudo docker pull debian@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
sudo sh /tmp/test-project-quota.sh /tmp/project_quota-<hash> /tmp/quota_runtime-<hash>
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

The container gate uses the actual Docker adapter with a 64 MiB project. A job-owned
nested directory cannot be retagged or lose inheritance through either ioctl
interface (`EINVAL`); scratch and output writes share the limit and return `EDQUOT`.
The gate also checks root identity, exact reservation matching, and recovery
inventory. Cleanup removes only this run's randomly labelled job before unmounting;
inventory or removal failure retains the fixture. A separate ignored test,
`daemon_without_remapping_rejects_strict_runtime`, passed with remapping disabled
on the idle test VM, then the VM configuration was restored.

Verified locally on Debian kernel `6.12.111+deb13-cloud-arm64`, with ext4 mounted
`rw,nosuid,nodev,relatime,prjquota`. Docker Desktop kernel `6.10.14-linuxkit` rejected
the initial mount because it lacks `CONFIG_QUOTA` and `CONFIG_QFMT_V2`; it cannot
substitute for this gate. The container gate passed with Docker `26.1.5+dfsg1` and
its built-in seccomp profile. AMD64 and the complete agent path remain unverified.

Shut down the test VM with its disposable key:

```sh
ssh -i .local/quota-vm/id_ed25519 -o IdentitiesOnly=yes \
  -o UserKnownHostsFile=.local/quota-vm/known_hosts \
  -p 22231 dispatch@127.0.0.1 sudo poweroff
```

The implementation follows Linux's [quota syscall contract](https://www.man7.org/linux/man-pages/man2/quotactl_fd.2.html),
[filesystem attribute UAPI](https://github.com/torvalds/linux/blob/v6.12/include/uapi/linux/fs.h),
and [quota UAPI](https://github.com/torvalds/linux/blob/v6.12/include/uapi/linux/quota.h).
Attribute protection follows Linux's [user namespace checks](https://github.com/torvalds/linux/blob/v6.12/fs/ioctl.c#L569-L579).
VM initialization uses cloud-init's [NoCloud datasource](https://docs.cloud-init.io/en/latest/reference/datasources/nocloud.html).
