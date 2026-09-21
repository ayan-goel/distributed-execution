# Dispatch v0.1 implementation and verification ledger

The contract is `Dispatch_Project_Spec.md`, sections 1–26. This ledger tracks
implementation evidence; unchecked items remain required work. GPU execution is
v0.2. Storage uses configurable S3-compatible endpoints and exact object versions;
development and testing must not require an AWS account.

## Working rule

For each atomic slice: define the acceptance test, observe the failure, implement,
run relevant tests and build/lint checks, inspect the diff, record evidence, commit.
Do not start implementing the next slice until the current gate passes. Split the
tasks below into smaller commits whenever they cross independently testable boundaries.
Never use a fake-runtime test as evidence for a real-runtime or multi-host gate.

## Ordered slices

| Task | Dependencies | Required acceptance evidence | Status |
| --- | --- | --- | --- |
| D01 build foundation | none | Pinned Go/Cargo builds, binary smoke checks, CI configuration | local gate passed; remote CI pending |
| D02 job and sweep contracts | D01 | Strict schema, unsafe input rejection, stable canonical hash, deterministic expansion | parser/expansion gates passed; published schemas pending |
| D03 database invariants | D01 | Real PostgreSQL migrations up/down/upgrade; uniqueness, references, checks | core schema and migration runner gates passed; later feature tables pending |
| D04 worker protocol | D01 | Generated Go/Rust gRPC bindings; cross-language golden round-trip, drift check | wire generation/round-trip passed; service boundary tests pending |
| D05 durable submission | D02,D03 | HTTP/CLI submission; 100 identical requests yield one job; changed payload conflicts | input-free job admission, auth, image resolution, HTTP/CLI gates passed; datasets depend on D16 |
| D06 worker identities | D03,D04 | Authenticated registration, session takeover/recovery; stale-session rejection | control-plane, executable, and Rust client gates passed; runtime/agent loop pending |
| D07 acquisition | D03,D06 | Atomic assignment/reservations; concurrent quota/capacity races; acquisition replay | pending |
| D08 Docker adapter | D04 | Real bounded create/start/inspect/stop; ambiguous create reconciles one identity | pending |
| D09 leases | D06,D07 | Fresh DB-time expiry checks; delayed grants; local monotonic deadline enforcement | pending |
| D10 worker recovery | D08,D09 | Durable journal; agent kill/restart stops old containers before new capacity | pending |
| D11 artifacts | D03,D04 | Scoped grants; verified exact versions; stale publication rejection | pending |
| D12 first real job | D05–D11 | CLI submit → gRPC → Docker → verified output → CLI download | pending |
| D13 loss/retry | D09,D12 | Reaper/reconciliation; recorded retry; backoff/deadlines; nonretryable failure | pending |
| D14 cancellation | D12,D13 | Both completion/cancellation race orders, repeat requests, uncertain cleanup | pending |
| D15 logs | D08,D11 | Bounded queues/spool, noisy-job truncation, stream cursors, reconnect | pending |
| D16 datasets | D11 | Immutable registration/cache; corruption rejection; pin-aware eviction | pending |
| D17 sweeps/metrics | D05,D13,D16 | Atomic 27-child sweep; 1000 cap; concurrency; fail-fast; finite scalar export | pending |
| D18 placement | D07,D17 | Project fairness/aging, blockers, two real independent hosts without oversubscription | pending |
| D19 faults/benchmarks | D14–D18 | Spec §21/22 runtime matrix, 27-job worker kill, stale result, measured benchmarks | pending |
| D20 release | D19 | TLS/auth/permissions, retention, migrations/backups, packaging, tutorial, actual research run | pending |

## Release audit (all required)

- [ ] All CLI and HTTP interfaces in §4, §15 work, with authorization, pagination,
  machine-readable output, stable error codes, and documented wait exit codes.
- [ ] Every gRPC mutation in §16 validates identity and documents replay behavior;
  protocol versions and bounds are enforced.
- [ ] All eight §7.3 invariants hold under generated interleavings and real DB races.
- [ ] All twelve §17 fault scenarios have observed evidence.
- [ ] All ten §21.4 end-to-end gates have observed evidence.
- [ ] Runtime tests cover OOM, disk pressure, startup/execution/finalization timeouts,
  unsafe/missing outputs, control-channel partition, and agent restart.
- [ ] Security boundary in §18 is enforced, including non-root containers, no Docker
  socket exposure, read-only inputs/root, network policy, scoped transfers, and limits.
- [ ] Strict scratch quotas verified on supported dedicated Linux filesystem/profile.
- [ ] All §20 root tasks and generated-binding drift checks work; CI covers declared
  Linux architectures. Benchmarks record conditions and actual results (§22).
- [ ] Deployment/upgrade/backup/restore/retention instructions verified (§19).
- [ ] Real research evaluation and second-user tutorial completed (§23, §26.3).

## Current environment and decisions

- Initial workspace contains only the specification; no prior implementation.
- Development host: macOS arm64. Docker Desktop is available. Its one daemon can
  verify integration but is not two independent workers and cannot prove that gate.
- Installed Rust 1.88.0; Go will be installed locally under ignored `.tools/`.
- Local PostgreSQL and object-store test instances must be isolated from existing
  user services. No production accounts or credentials are needed for development.
- Repository/license choice remains open; do not invent a public repository or license.

## Verification log

### D01: build foundation

- Installed official Go 1.27.1 locally after checking its published SHA-256;
  pinned Rust 1.88.0 with rustfmt/clippy. No system Go install was changed.
- Smoke test first failed because the build target did not exist. After bootstrap,
  all three binaries build, report the expected version, and reject invalid options.
- `make test lint smoke` passed locally using macOS Command Line Tools. There is
  no application behavior yet; the initial test gate is executable smoke coverage.
- CI workflow is checked in but remote CI cannot run until a remote is configured.

### D02a: job contract

- Added bounded JSON/YAML parsing, typed fields, path/resource/deadline validation,
  request normalization and SHA-256 identity, and the specification's job example.
- Tests first failed with missing parser/types. Added negative cases for unsupported
  GPUs/privilege/networking, traversal, overlapping outputs, duplicate keys, malformed
  types, retry bounds, and oversized/multiple documents.
- Two further regression tests reproduced JSON field-case acceptance and omitted
  retry-list round-trip failure; both now pass.
- `make test lint build` passed, including Go's race detector. A 5-second fuzz run
  executed 16,534 inputs with no failures. This establishes parsing, not admission,
  image resolution, authorization, or runtime containment.
- Contract and limits documented in `docs/contracts.md`. Sweep contract remains next.

### D02b: deterministic sweep expansion

- Added the embedded-template server contract; server decoding rejects client paths.
  Expansion sorts parameter keys, preserves value order, deep-copies children, and
  checks the 1000-job and 16-MiB bounds before unbounded work can accumulate.
- Tests first failed because sweep types/expansion did not exist. Tests now prove
  the 27 combinations and order, 1000-job boundary, independent child state, project
  matching, concurrency/failure policy validation, and rejection of invalid matrices.
- `make test lint build` passed with race detection. Runtime sweep accounting and
  transactional persistence remain D17; this gate proves pure expansion only.

### D03a: real PostgreSQL ownership constraints

- Added the core SQL migration and explicit destructive development rollback, with
  foreign keys, active-attempt uniqueness, positive resources, finite states, immutable
  identity/outcomes, and deferred ownership/reservation consistency checks.
- The integration test first failed because the schema was absent. Corrected a test
  startup race (the image's temporary server) and a fixture that unintentionally
  violated generation equality before reaching its intended uniqueness assertion.
- PostgreSQL 17.11 container tests passed: legal assignment/failure transactions;
  invalid values, duplicate owners/numbers, cross-job pointers, missing reservations,
  terminal mutation, and spec mutation all rejected with expected SQLSTATEs.
- Fresh migration, full rollback with no tables left, reapply, and repeated constraint
  tests passed. Go/Rust tests, builds, formatting, and static checks passed as well.
- Added `make integration`, its CI step, and `docs/database.md`. This gate does not
  establish scheduler concurrency correctness; those race tests remain required.

### D03b: transactional migration runner

- Embedded the SQL and added ordered, checksummed migrations under a dedicated
  PostgreSQL advisory lock. Unknown versions, checksum drift, and sequence gaps
  fail rather than silently accepting an incompatible database.
- Tests first failed for missing migration functions. Real PostgreSQL tests now
  pass for eight concurrent callers, repeat application, checksum mismatch, partial
  DDL rollback, incremental upgrade, and an older binary against a newer database.
- `scripts/test-store.sh` passed with Go race detection; `make test lint build`
  passed. The integration command runs schema and store suites in disposable DBs.
- Documented history/checksum behavior and test isolation in `docs/database.md`.

### D04a: generated Go/Rust worker protocol

- Defined all ten worker RPCs and explicit authority, session, phase, decision,
  upload, object-version, log-gap, and completion types. Generated checked-in Go
  bindings; Rust builds from the same proto using pinned Tonic/Prost dependencies.
- Golden test initially failed because bindings were absent. Go → Rust → Go now
  preserves all assignment fields, including Unicode and integers above 2^53/2^32.
- Clippy exposed a large generated acquisition enum. Inspected Prost's oneof path
  matching and configured only the assignment variant to be boxed; lint now passes.
- A concurrent Go module scan raced Cargo's incremental directory. Moved Cargo
  output into ignored `.local/cargo-target`, outside Go's `./...` traversal.
- `make test lint smoke` and `make generate-check` passed. Store integration tests
  passed again after shared Go dependencies changed. Existing pinned generator
  binaries are reused so drift checks do not require a network download each run.
- Documented protocol semantics, limits to enforce, version pins, and evidence in
  `docs/protocol.md`. Authentication and RPC boundary enforcement remain unimplemented.

### D05a: durable idempotent queue insertion

- Added database submission/recovery operations. The project-scoped endpoint/key,
  immutable resolved job, and first event commit atomically. Request hash and final
  execution hash remain distinct so resolving a tag does not change request identity.
- Tests first failed with missing operations. Real PostgreSQL now passes 100
  concurrent identical requests with exactly one job, key, and event; changed payloads
  conflict. Recovery reads the stored job after an uncertain submission response.
- Quota/missing-project/unpinned-image rejection and an injected event-insert failure
  leave zero partial jobs or keys. Race-enabled store tests and `make test lint build`
  passed. The current internal admission function rejects unresolved datasets.
- This is not a public submission endpoint yet. Project authentication, registry
  resolution, dataset manifests, HTTP integration, and CLI remain required.

### D05b: hashed project tokens and permissions

- Added a second migration for project-scoped API token hashes and audit events.
  Tokens use 256-bit random secrets, returned once, with read/submit/operator roles.
- Tests first failed with missing auth operations. Real PostgreSQL tests now pass
  for role separation, hash-only storage, unknown/revoked tokens, project disabling,
  cross-project revocation rejection, repeated revocation, and atomic audit records.
- Full schema migration/rollback/reapply and race-enabled store tests passed;
  `make test lint build` passed. No HTTP authentication endpoint is exposed yet.
- Added `docs/security.md` with implemented boundaries and remaining release work.

### D05c: immutable image admission

- Added approved-registry image resolution with bounded response bodies/deadlines,
  manifest digest verification, and outbound host checks covering auth and redirects.
- Tests first failed with missing resolver types. Unit tests and a real loopback
  registry now pass, including tag movement with old digest recovery, denied hosts,
  wrong digests, malformed responses, cancellation, and explicit development HTTP.
- `make test lint build` and race-enabled store/registry integration tests passed.
- Updated `docs/contracts.md` with resolution policy and documented its limits.

### D05d: authenticated HTTP admission

- Added bounded HTTP handlers for readiness, submission, and project-scoped job
  inspection. Request IDs, structured sanitized errors, request/body/concurrency
  bounds, a global rate cap, and read/submit role checks apply at the boundary.
- Tests first failed with missing handlers. Real PostgreSQL HTTP tests now pass for
  100 concurrent duplicate requests, role and tenant isolation, changed payloads,
  replay without registry access, unavailable resolution, and malformed/large bodies.
- `make test lint build` and race-enabled store/registry/API integration tests passed.
  Executable serving and CLI submission remain next; unresolved datasets still fail
  explicitly until their versioned admission slice is implemented.
- Added `docs/http-api.md` with the implemented contract and remaining endpoints.

### D05e: runnable control plane and operator commands

- Added database migration, project creation, token issuance/revocation, and HTTP
  serving commands. TLS is required outside explicit literal-loopback development;
  listen/registry configuration is checked before exposing a listener.
- Configuration tests first failed with missing parsing. Build, unit tests, static
  checks, and binary smoke tests now pass. Real database/registry integration covers
  command entry points, actual HTTP submission, graceful stop, restart, replay with
  the registry offline, and revocation observed by the running server.
- Added `docs/running.md`; README accurately describes the queue-only current state.
  Active execution and the full v0.1 deployment tutorial remain pending.

### D05f: bounded authenticated HTTP client

- Added a Go client for job submission and inspection with structured API errors,
  20-second request deadlines, bounded responses, stable caller-supplied submission
  keys, and validation of returned job IDs. It does not retry uncertain submissions
  automatically; callers must retain and reuse the original key.
- Client configuration requires HTTPS, with explicit literal-loopback HTTP for
  development. Redirects are rejected before forwarding credentials or job bodies;
  endpoint credentials/query strings and unsafe header values are rejected.
- Tests first failed because the client did not exist. Race-enabled tests now pass
  for redirect refusal, key preservation, API error fields, response size/malformed
  response rejection, and argument validation. The loopback redirect test requires
  network-enabled execution; `make test lint smoke` passed with that permission.
- CLI commands and client-to-server database integration remain the next slice.

### D05g: CLI validation, submission, and inspection

- Added offline validation, authenticated submission, and project-scoped job
  inspection commands with JSON output, bounded client calls, signal cancellation,
  and exit 2 on client errors. Submission prints a generated or explicit recovery
  key to stderr before sending; retries preserve job identity without registry access.
- Tests first failed with the missing CLI entry point. Unit tests now cover offline
  behavior, invalid usage, key preservation, JSON/diagnostic separation, and safe
  terminal errors. `make test lint smoke` passed, including actual-binary validation
  of a new deterministic CPU example.
- Race-enabled PostgreSQL/registry/server integration passed: CLI submission and
  inspection preserve the resolved digest and hash; restart followed by an offline
  registry retry returns the same job. Reviewed request bounds, credential handling,
  argument rejection, output errors, and dependency direction before committing.
- Expanded `docs/running.md` with the current CLI workflow and uncertainty recovery.
  This establishes durable admission only; jobs still cannot execute. D06 worker
  identities and session fencing are next, with remaining CLI commands tracked above.

### D06a: provisioned worker identities and policy storage

- Added a third migration for host resource/label policy, explicit project access,
  public certificate fingerprints, and worker audit events. Provisioning starts in
  REGISTERING and cannot itself make a host eligible for scheduling.
- Tests first failed because provisioning/authentication operations were absent.
  Real PostgreSQL tests now pass for 16 concurrent duplicate certificate enrollments
  with exactly one complete host, bounded project membership, unknown/revoked identity
  rejection, cross-host revocation denial, and idempotent revocation.
- Injected audit failures prove provisioning and revocation roll back completely.
  Platform/resource/label validation tests pass. `make test lint smoke integration`
  passed, including full migration rollback/reapply and all previous HTTP/CLI tests.
- Reviewed transaction scope, uniqueness, audit atomicity, credential separation,
  and the distinction between identity and execution authority. Documented the
  remaining mTLS/session enforcement in `docs/security.md`; this is an internal
  storage slice, not a running worker registration service.

### D06b: verified mTLS worker RPC boundary

- Added a bounded gRPC server constructor with mandatory TLS 1.3/client CA
  verification, provisioned leaf-fingerprint lookup, per-call revocation and
  certificate-validity checks, and host binding for all ten worker request types.
- Tests first failed with missing boundary helpers. Unit tests now cover expiry
  after handshake and nested identity binding. Real TLS/gRPC/PostgreSQL integration
  passes for identity propagation, impersonation rejection, unknown/revoked clients,
  oversized requests, and missing/untrusted/wrong-purpose client certificates.
- Review strengthened the TLS tests by enrolling invalid certificates before
  attempting the handshake, so database authentication cannot mask broken TLS.
  `make test lint smoke` and the expanded race-enabled store integration suite
  passed. No schema or wire-contract changes were needed for this slice.
- Documented security limits and verification scope in `docs/security.md`.
  The test service only echoes authenticated identity. Real registration/session
  transitions and executable listener wiring remain next; no worker execution is
  implied by this transport gate.

### D06c: durable session registration

- Added immutable session registration hashes and replay records, transactionally
  bound to the provisioned worker credential and capacity/label policy. Registration
  starts REGISTERING and does not expose schedulable capacity.
- Tests first failed for missing session operations. Real PostgreSQL now passes
  32 concurrent duplicates with one session, matching-incarnation request aliases,
  changed-claim conflicts, competing incarnation rejection, resource/label/protocol
  validation, transaction-time credential revocation, and injected-audit rollback.
- `make test lint smoke integration` passed, including fourth-migration rollback
  and reapply. Reviewed lock ordering, replay identity, and policy enforcement.
- Added `docs/worker-sessions.md`. Recovery/takeover, heartbeat reconciliation,
  registration RPC handlers, and executable wiring remain required.

### D06d: session recovery, explicit takeover, and fencing

- Added exact-source/exact-target operator approvals and automatic recovery only
  after session inactivity and expiry of all remaining active leases. Recovery
  serializes ownership locks, then reads fresh database wall time.
- Fencing atomically terminalizes old attempts, honors cancellation/retry policy,
  records capped exponential backoff with stable equal jitter, quarantines physical
  reservations, and starts a new unreconciled generation. Old session replay fails.
- Tests first failed for missing takeover operations. Real PostgreSQL now passes
  live-lease refusal, automatic recovery, live takeover, wrong-target rejection,
  retry/cancel/exhaustion outcomes, irreversible fencing, and injected-event rollback.
- The lock-wait regression explicitly observes a blocked recovery transaction,
  ends its lease after transaction start, then proves fresh-time evaluation. Reviewed
  transaction order, failure atomicity, retry bounds, and physical-capacity accounting.
- `make test lint smoke integration` passed; the strengthened deterministic lock
  test also passed in the race-enabled integration suite. Fifth-migration rollback
  and reapply passed. Updated `docs/worker-sessions.md`; heartbeat reconciliation
  and registration RPC/executable integration remain required.

### D04b: ordered heartbeat wire reports

- Added additive field 7, `report_sequence`, to heartbeat requests. Its per-session
  monotonic value will reject delayed observations and permit bounded replay state
  without storing every liveness tick. Exact retries retain sequence/request/payload.
- Regenerated Go and Rust bindings. `make generate generate-check test lint smoke`
  passed, including cross-language round-trip and generated-code drift checks.
- Documented sequence range and replay rules in `docs/protocol.md`. Database and
  handler enforcement are the next slice; the wire field alone grants no behavior.

### D06e: ordered heartbeat readiness and cleanup

- Added bounded, canonical inventory reports with durable monotonic sequence
  enforcement. Exact replay cannot refresh liveness or reapply cleanup; stale and
  changed reports are rejected. Credential/session authorization is rechecked.
- Readiness now depends on health, disk pressure, drain, and reconciliation.
  Unknown/old/expired/cancelled executions receive stop instructions. Only a newer
  session's complete healthy reconciliation can release fenced predecessors'
  quarantined reservations; a session cannot release its own uncertain capacity.
- Tests first failed for missing heartbeat operations. Real PostgreSQL tests pass
  for readiness/health/drain, ordered replay, old-session rejection, cleanup gating,
  unchanged leases/active reservations, same-session quarantine preservation, and
  injected-audit rollback of cleanup and report identity. Unit tests cover inventory
  bounds and order-independent hashes.
- `make test lint smoke integration` passed, including sixth-migration rollback
  and reapply. Reviewed lock order, replay effects, physical-capacity preservation,
  and the separation between heartbeat liveness and execution leases. Documented
  contracts and pending runtime/RPC enforcement in `docs/worker-sessions.md`.

### D06f: registration and heartbeat through mTLS gRPC

- Added service handlers that require transport identity, enforce exact MiB and
  signed database integer bounds, call the verified store transitions, and expose
  stable sanitized error reasons. Other worker RPCs remain explicitly unimplemented.
- Tests first failed for the absent service. Unit tests reject direct calls without
  transport identity. Real mTLS/gRPC/PostgreSQL integration passes for registration,
  readiness, malformed resource/sequence claims, server restart replay, live-session
  conflict, operator-approved takeover, and old-session heartbeat rejection.
- `make test lint smoke` and the expanded race-enabled integration suite passed.
  Reviewed field conversion, context authorization, error mapping, and replay
  semantics. Updated security/session documentation with implemented boundaries.
- Executable listener/provisioning/takeover commands and the Rust session client
  remain next. This is a working control-plane RPC service, not container execution.

### D06g: worker operator commands

- Added certificate-based worker enrollment, idempotent credential revocation,
  and exact-source/exact-target takeover approval to `dispatch-server`. Enrollment
  accepts only bounded public client-certificate PEM input; private keys stay local
  to the agent. Resource/project/label policy uses the verified store operations.
- Tests first failed for the absent certificate reader. Unit tests now reject
  private keys, CA/server-only leaves, missing files, and oversized input. Real
  PostgreSQL command tests enroll a host, register it, approve and complete takeover,
  revoke its credential twice, and observe authentication rejection.
- `make test lint smoke` and race-enabled integration passed. Reviewed certificate
  handling, authority scope, duplicate labels, and command output. Added the operator
  workflow and its current limits to `docs/running.md`.

### D06h: combined HTTP and worker gRPC executable

- Added optional worker listener flags with mandatory certificate/key/client-CA
  configuration. HTTP development mode does not weaken worker mTLS. Both TLS setups
  load and both ports bind before announcing readiness; either listener failing
  shuts down the other. Both share a bounded graceful shutdown budget.
- Config tests first failed on the missing flags. Unit/build checks and real
  PostgreSQL command integration now pass for HTTP readiness, mTLS registration
  and heartbeat, restart/replay, live operator revocation, and bad-TLS startup.
- Review found a nil gRPC pointer on forced HTTP-only shutdown. A blocked HTTP
  request reproduced the panic; the fix and regression test pass. `make test lint
  smoke` and the race-enabled command/database integration suite passed afterward.
- Reviewed startup failure cleanup, mTLS isolation, listener lifetime, and pool
  shutdown ordering. Updated the operator/session docs with runnable commands.

### D06i: Rust mTLS registration and heartbeat client

- Added an HTTPS-only Tonic client with explicit CA/client identity, five-second
  connection/RPC deadlines, bounded messages, stable caller-owned retry identities,
  registration authority checks, and host-scoped cleanup-response validation.
- Tests first failed for missing client operations. Unit tests cover endpoint
  policy, response identity/protocol, cleanup scope/bounds, and retry categories.
  The real Rust fixture registers and replays heartbeats through the Go executable
  against PostgreSQL, including process restart replay, unrelated CA rejection,
  credential revocation, and stalled TLS/RPC deadlines. It stays quarantined and
  unreconciled; no runtime execution/cleanup claim is made by this fixture.
- Reviewed explicit trust roots, credential-safe diagnostics, unchanged dependency
  versions outside the added TLS graph, replay identity, and cleanup authority.
  `make test lint smoke` and the expanded race-enabled database/integration suite
  passed, including both five-second stalled-transport cases.
  Documented the library contract and remaining runtime/agent-loop work in
  `docs/worker-sessions.md`. Full v0.1 scheduling/execution/release gates remain open.
