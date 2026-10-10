# Independent Linux worker gates

The release recovery and fencing gates run the actual Rust agent in two Debian
ARM64 VMs. Each VM has its own kernel, writable disk, Docker daemon, enrolled
worker identity, and ext4 quota filesystem. The host test runs the Go API/gRPC
handlers with real PostgreSQL and versioned SeaweedFS. Both VMs share the same
physical macOS host; this is independent VM evidence, not physical host-loss testing.

## Setup

Start the default quota VM, then the named second VM in separate foreground
sessions, as documented in [project-quotas.md](project-quotas.md). The wrapper
expects loopback SSH ports 22231 and 22232 and fixture keys/verified known-hosts
files under `.local/quota-vm/` and `.local/sweep-worker-2-vm/`.
Initialize the default VM first so both reuse the verified read-only base image.

Inside **each disposable VM**, install `docker.io`, `docker-cli`, and `uidmap`.
Configure the dedicated daemon with `{"userns-remap":"default"}`, restart it,
and pull the pinned Debian image used by the gate:

```sh
sudo docker pull debian@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
```

Build `dispatch-worker` for Linux ARM64 using the pinned Rust toolchain and Linux
protoc, as in [project-quotas.md](project-quotas.md). With both VMs live, run from the
checkout root:

```sh
sh scripts/test-independent-workers.sh .local/linux-worker/target/debug/dispatch-worker
```

The wrapper stages the worker and fixture helper into both VMs and starts isolated
host database/object-store services. It does not reconfigure Docker Desktop or
reuse existing databases/buckets. The Go test provisions temporary certificates;
SSH transfers them into root-only fixture directories. Reverse loopback tunnels
carry mTLS control and preserve the signed object URLs' original loopback origin.
No Docker TCP API is exposed. Each test formats only its new 256 MiB file-backed
loop device; cleanup targets its enrolled worker's containers and retains storage
if inventory, removal, or unmount fails.

## What the gates prove

`TestIndependentLinuxWorkersRecover27ChildSweep` submits a 3 × 3 × 3 matrix through
the CLI. Both workers must complete children before the test selects a fresh
RUNNING attempt and kills its agent. Docker inspection proves its container
survived the process kill. The production reaper waits for the issued lease to
expire naturally; no test shortens the database lease. The replacement runs on
the surviving VM with the next generation.

The gate verifies 27 accepted children, 28 attempts, one LOST record, no fail-fast
cancellation, strict capability claims from both successful workers, a concurrency
peak of two, released replacement capacity, and quarantined old capacity. CLI
export preserves all matrix parameters and metrics, and verified artifact download
returns the replacement's source bytes.

`TestIndependentLinuxWorkersRejectDelayedResult` separately holds a real successful
completion before acceptance. Its attempt-owned outputs already exist as verified
immutable versions. After killing that agent and allowing natural expiry and a
replacement, the gate replays the original completion with the original enrolled
certificate/session/generation. It must receive `ALREADY_TERMINAL/LOST`, with no
accepted manifest. The accepted manifest and history stay unchanged, and a verified
download contains the replacement's distinct attempt ID. The rejected old object's
diagnostic version remains intact.

The reviewed local run passed with race detection: recovery took 267.46 seconds
and delayed-result fencing took 33.55 seconds. Recovery recorded loss 1.144223
seconds after lease expiry. Evidence is under ignored
`.local/verification/sweep-vm-final.log`. These durations include deliberate
fixture sleeps and are not startup or throughput benchmarks.

These are execution acceptance fixtures with synthetic metrics, not a completed
research evaluation. Old-worker restart/reconciliation, other fault cases,
benchmarks, AMD64, deployment/restore checks, and the second-user tutorial remain
separate release requirements.
