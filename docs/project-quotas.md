# Strict scratch: ext4 project quotas and Docker

The default Linux worker connects ext4 project quotas to workspace allocation,
admission, health checks, and cleanup. A complete submitted-job gate passed on a
dedicated Debian ARM64 VM. The optional `--dev-soft-scratch` profile remains
explicit and advertises only `scratch.soft`.

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
the strict agent holds an exclusive lock on the filesystem root inode throughout
its lifetime. Bind-mount aliases contend on the same lock. A subdirectory cannot
serve as an independent project-ID namespace.

## Agent setup and recovery

Run the strict worker as root on a dedicated Linux host with the Docker profile
below. Set `workspace_root` to the canonical root of a dedicated ext4 filesystem
with project quotas enabled, owned by root with mode 0700. The filesystem must
initially be empty except for an optional empty, root-owned mode-0700 `lost+found`.
Keep the journal and credentials outside this filesystem. Provision the enrolled
worker's configuration before initializing:

```sh
sudo dispatch-worker init-scratch --config /absolute/path/worker.json
sudo dispatch-worker run --config /absolute/path/worker.json
```

Initialization creates `.dispatch-projects/` for the worker-bound counter and
`work/` for attempt directories and the sealed dataset cache. It is an explicit
one-time operation, not a repair command. Restart uses the existing counter;
missing, corrupt, wrong-worker, or partially initialized state stops admission.
Do not delete or reset the counter to recover a filesystem with retained quotas.
Use a new dedicated filesystem or investigate the retained evidence.

Each attempt receives a fresh ID before its scratch/output children exist.
The inode limit is one inode per 4 KiB of requested scratch, bounded to 16–1,000,000.
Reconciliation removes abandoned attempts only under `work/`, preserving the
allocator. Cleanup syncs that parent and never recycles project IDs or removes
their quota records.

Before registration, the agent verifies filesystem enforcement and Docker's quota
profile, then advertises `scratch.quota`. Periodic health checks revalidate storage
identity, counter health, enforcement, and the daemon profile. Observed loss of
enforcement revokes readiness and requests termination of an active job; the live
loss transition is code-reviewed but has not yet been exercised by a fault gate.

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
The worker also checks this capability before registration and during health checks.

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
The gate also checks root identity, exact reservation matching, recovery inventory,
allocator restart, filesystem exclusivity through bind aliases, and refusal to
reset a missing counter. Cleanup removes only this run's randomly labelled job before unmounting;
inventory or removal failure retains the fixture. A separate ignored test,
`daemon_without_remapping_rejects_strict_runtime`, passed with remapping disabled
on the idle test VM, then the VM configuration was restored.

Verified locally on Debian kernel `6.12.111+deb13-cloud-arm64`, with ext4 mounted
`rw,nosuid,nodev,relatime,prjquota`. Docker Desktop kernel `6.10.14-linuxkit` rejected
the initial mount because it lacks `CONFIG_QUOTA` and `CONFIG_QFMT_V2`; it cannot
substitute for this gate. The container gate passed with Docker `26.1.5+dfsg1` and
its built-in seccomp profile. AMD64 remains unverified.

## Reproduce the complete agent gate

Build the Linux worker and an integration-tagged Go server test binary for the
VM's architecture. Place the test binary, worker, this repository's
`schema/examples/job.yaml`, and `scripts/test-quota-agent.sh` on the VM, preserving
the repository layout and `cmd/dispatch-server/` working directory. Supply the
isolated database and versioned S3 fixture environment used by
`scripts/test-store.sh` and `scripts/test-objectstore.sh`, then run:

```sh
sudo -E sh scripts/test-quota-agent.sh /absolute/path/dispatch-server.test /absolute/path/dispatch-worker
```

The script formats only its newly allocated 256 MiB loop-backed fixture and runs
`TestWorkerDaemonEnforcesStrictScratchQuota`. The test enrolls the actual worker,
initializes strict storage, submits a dataset-backed job through the CLI, and
attempts a 128 MiB write against a 64 MiB reservation. It verifies `EDQUOT` and the
bounded file size, frees the test payload, and checks accepted completion, live
logs, exact output download, strict capability claims, and container/workspace
removal. This gate passed in 26.03 seconds using disposable host services through
SSH loopback tunnels. It establishes one Linux worker, not the two-host release gate.

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
