# Worker authority deadlines

## Local deadline primitive (D09a)

`dispatch_worker::lease::AuthorityWindow` turns a server grant into a local deadline:

```text
lease_deadline = request_send_time + remaining_lease - 5 seconds
phase_deadline = request_send_time + remaining_phase
local_deadline = min(lease_deadline, phase_deadline)
```

Receipt time never starts a fresh lease. The constructor rejects a response already
past that deadline; every later `remaining()` check reads the clock again. Zero or
unsupported durations, arithmetic overflow, and observed backwards time fail closed.
The current wire policy permits leases up to 30 seconds and phases up to seven days,
matching the control-plane lease default and maximum execution timeout. One-second
startup/execution/finalization limits remain usable: the five-second margin applies
to lease authority, not to the configured phase duration.

Linux uses `clock_gettime(CLOCK_BOOTTIME)` so suspend time contributes to elapsed
authority. The syscall uses the already-locked libc 0.2.189 dependency and validates
its return code and timespec range. Clock failures are explicit errors, never a
fallback to wall time. Non-Linux builds use a process-relative `Instant` only for
protocol development; actual worker execution remains Linux-only.

Deadlines are in-memory values for the current boot/process. They must not be
serialized into the worker journal or recovered as authority after restart. Recovery
must obtain current grants and apply the same send-time calculation. A valid grant
also does not prove that runtime/spec validation or reconciliation has completed.

The future supervisor must recheck authority before starting/continuing work and
initiate bounded termination early enough for cleanup. This primitive is not a
running watchdog, renewal loop, reaper, or proof that a frozen host can stop code.
Server-side fencing remains necessary when physical termination cannot be confirmed.

## Verification

Deterministic tests cover delayed receipt, exact expiry, large forward clock advances,
one-second phase deadlines, backwards samples, invalid limits, and overflow. These
tests simulate pauses through clock samples; they do not suspend the user's host.
An OS-clock test exercises the actual clock implementation for the test platform.

`make test lint smoke` verifies the normal workspace. `sh scripts/test-worker-clock.sh`
additionally verifies the production Linux branch. On Linux it uses the pinned native
toolchain; elsewhere it compiles the same module in the official Rust 1.88.0 slim
Bookworm image pinned by manifest digest. The container has no network, a read-only
root filesystem, read-only source/dependency mounts, and an ignored `.local/linux-clock`
build directory. `make integration` includes this check. The observed Linux run was
arm64 on Docker Desktop; independent-host execution and actual pause/termination
fault tests remain release work.

## Source references

- [Linux clock_gettime and CLOCK_BOOTTIME](https://man7.org/linux/man-pages/man2/clock_gettime.2.html)
- [Rust Instant platform and suspend behavior](https://doc.rust-lang.org/std/time/struct.Instant.html)
