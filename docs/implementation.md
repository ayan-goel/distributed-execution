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
| D04 worker protocol | D01 | Generated Go/Rust gRPC bindings; cross-language golden round-trip, drift check | wire and implemented mTLS service boundary gates passed; remaining handlers/client integration pending |
| D05 durable submission | D02,D03 | HTTP/CLI submission; 100 identical requests yield one job; changed payload conflicts | input-free job admission, auth, image resolution, HTTP/CLI gates passed; datasets depend on D16 |
| D06 worker identities | D03,D04 | Authenticated registration, session takeover/recovery; stale-session rejection | control-plane, Rust client, and worker startup/health loop gates passed; active-job supervision pending |
| D07 acquisition | D03,D06 | Atomic assignment/reservations; concurrent quota/capacity races; acquisition replay | store, RPC, and Rust client gates passed; production agent loop pending |
| D08 Docker adapter | D04 | Real bounded create/start/inspect/stop; ambiguous create reconciles one identity | cached launch, RUNNING/FINALIZING coordination, and phase store/RPC/client gates passed; agent integration, staging, and strict workspaces pending |
| D09 leases | D06,D07 | Fresh DB-time expiry checks; delayed grants; local monotonic deadline enforcement | deadline, store/RPC/client, watchdog, periodic renewal, and execution refresh gates passed; reaper and production agent integration pending |
| D10 worker recovery | D08,D09 | Durable journal; agent kill/restart stops old containers before new capacity | journal, discovery/cleanup, and actual startup process gates passed; agent-owned job kill/restart gate pending |
| D11 artifacts | D03,D04 | Scoped grants; verified exact versions; stale publication rejection | grants, verification, completion, public metadata, and verified CLI download gates passed; Rust transfers and multipart support pending |
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

### D07a: transactional assignment and acquisition replay

- Added credential/session-checked acquisition with resource/placement/quota filters,
  physical quarantine accounting, strict scratch policy by default, and atomic
  attempts/reservations/job state/events/request identity. Queue backfill selects a
  fitting job before a blocked candidate; final fairness and sweep limits remain
  explicit D18/D17 dependencies rather than claims of this store slice.
- Tests first failed for missing acquisition types/operations. Real PostgreSQL
  tests exercise 16 concurrent exact replays, concurrent fresh requests, separate
  capacity dimensions, shared project quotas on two worker identities, stable
  no-work replay, expired/cancelled/fenced/revoked authority, and audit rollback.
  A lock-observed expiry race checks fresh database time after a blocked replay.
- Documented the transaction, replay decisions, scratch policy, verification scope,
  and remaining runtime/fairness integration in `docs/acquisition.md`.
- Reviewed credential/session scope, replay authorization, lock order, fresh time,
  capacity arithmetic, project accounting, and rollback boundaries. `make test lint
  smoke` and the expanded race-enabled PostgreSQL integration suite passed.

### D07b: authenticated acquisition RPC

- Wired AcquireWork into the mTLS service and executable with an explicit strict
  scratch policy. Responses preserve authoritative identity, canonical spec/hash,
  pinned image, argv, byte-based resources, and remaining lease/phase durations.
  Expired/submillisecond deadlines reject execution; unknown or ambiguous outcomes
  cannot silently become a zero-valued wire grant.
- Tests first failed for the missing response adapter. Unit tests cover transport
  identity, field conversion, deadline underflow, and invalid outcomes. Real mTLS
  integration covers empty-queue replay after admission, acquisition, server restart
  replay without lease renewal, cross-worker rejection, expired leases, and takeover.
- Assignment inventory recovery and the Rust acquisition loop remain next. Reviewed
  identity binding, integer conversion, deadline semantics, and policy defaults.
  `make test lint smoke` and the real PostgreSQL/mTLS integration suite passed.

### D07c: bounded assignment recovery pages

- Added current-session assignment inventory with stable UUID pagination, default
  32/maximum 64 candidates per page, conservative spec-byte budgeting, and exact
  protobuf message limits. Pages recheck authority without renewing leases or
  releasing reservations. Empty pages advance past non-actionable candidates.
- Added backward-compatible request page-size/cursor and response continuation
  fields, regenerated Go/Rust bindings, and extended the cross-language round-trip
  fixture to preserve a recovery page's assignment and cursor.
- Tests first failed for missing inventory operations/response conversion. Real
  PostgreSQL tests cover ordered continuation, repeat reads without renewal, large
  page splitting, expired/phase-expired/cancelled omission, retained reservations,
  invalid cursors/counts, stale sessions, and revocation. mTLS tests recover after
  server restart and reject cross-worker, oversized-count, expired, and old-session
  requests. Unit tests enforce exact encoded message bounds.
- Reviewed lock ordering, cursor progress, per-page authorization, bounded payloads,
  and the distinction between omitted authority and physical cleanup. Documented
  that the future agent must pause acquisition during recovery and process all pages.
- `make generate generate-check test lint smoke` and the expanded race-enabled
  PostgreSQL/mTLS suite passed, including full traversal of byte-limited pages with
  no skipped or repeated assignments.

### D09a: conservative local authority windows

- Added a send-time-based lease/phase deadline primitive ahead of Rust acquisition
  so incoming grants cannot start a fresh lease on receipt. It applies the specified
  five-second lease margin, preserves short phase timeouts, and rejects late grants,
  invalid ranges, backwards time, and arithmetic overflow.
- Linux uses suspend-aware CLOCK_BOOTTIME through the existing pinned libc version.
  Non-Linux builds remain protocol-development fixtures. Tests first failed for
  missing clock/deadline types, then passed for deterministic delay/expiry/clock
  cases and the actual OS clock path.
- `make test lint smoke` passed. The exact module also compiled and passed its five
  tests inside the pinned official Rust 1.88.0 Linux arm64 container with no network.
  Added that platform check to `make integration` and documented sources, semantics,
  platform limitations, and remaining supervision/renewal work in `docs/worker-leases.md`.

### D07d: Rust acquisition and paginated recovery client

- Added bounded, deadline-aware acquisition and assignment-page operations over the
  existing mTLS channel. Validates session/attempt identity, resources, argv, pinned
  image, known outcomes, page ordering, and cursor progress. Expired grants retain
  identity for reconciliation without returning execution authority.
- The Go executable/PostgreSQL integration fixture verifies stable empty-queue
  replay, three acquisitions, exact request replay without duplicate attempts,
  single-item recovery traversal, and expired replay fencing. A separate mTLS
  service deliberately delays a grant past its usable lease; the Rust client rejects
  it. These fixtures do not claim physical execution or runtime health discovery.
- Reviewed identity binding, bounded decoding, monotonic send-time authority,
  independent page-count/order checks, and incremental recovery. A regression test
  exposed acceptance of NUL in image names; image control characters now fail closed.
- `make test lint smoke` and the real PostgreSQL/mTLS integration suite passed;
  workspace checks passed again after the image-validation fix. Documented contracts
  and remaining full spec validation, journal, runtime, and supervisor requirements
  in `docs/acquisition.md`, `docs/worker-leases.md`, and `docs/worker-sessions.md`.

### D08a: validated worker execution settings

- Added strict, bounded execution JSON decoding with exact-byte SHA-256 verification,
  typed runtime settings, and agreement checks against the image/argv/resource wire
  fields. Both Rust acquisition paths require validation and recheck authority after
  parsing. Nonempty inputs remain explicitly unsupported until dataset staging.
- Tests first failed for the missing execution module, then passed for hash and wire
  mismatch, literal arguments/Unicode environment values, unsafe paths, overlapping
  outputs, forbidden networking, timeout/retry bounds, reserved environment names,
  ambiguous JSON, and payload bounds. Real Go-canonicalized specs (including escaped
  HTML and Unicode characters) passed through the executable/PostgreSQL/mTLS fixture.
- Reviewed resource conversion bounds, duplicate-key handling, immutable access to
  validated settings, environment-safe errors, input rejection, and lease expiry
  during parsing. Pinned Serde/JSON dependencies and reused the existing ring version
  for hashing; the lockfile does not upgrade existing dependency versions.
- `make test lint smoke` and the expanded race-enabled PostgreSQL/mTLS suite passed.
  Recorded contracts, sources, and remaining runtime enforcement requirements in
  `docs/execution-spec.md`. Docker operations, the journal, and supervision are next;
  no container-execution or full v0.1 release gate is claimed by this slice.

### D08b: Docker container lifecycle and restrictions

- Added a runtime abstraction and Bollard-backed local-socket adapter with negotiated
  API version, Linux capability checks, deterministic attempt names, immutable
  ownership/spec labels, and bounded create/start/inspect/wait/stop/kill/log/remove
  operations. The builder applies non-root execution, CPU/memory/PID limits, disabled
  network, dropped capabilities, no-new-privileges, read-only root/inputs, bounded
  temporary storage, and daemon-side log rotation.
- Tests first failed for the missing runtime module. Real Docker fixtures now cover
  concurrent create/replay, foreign ownership/spec rejection, successful execution and output,
  kernel security/resource settings, bounded separate logs, terminal-start rejection,
  stop/kill/removal, expired authority, and OOM classification distinct from SIGKILL.
- A mock daemon commits create then drops its reply; the adapter recovers by name
  without allocating a second container. Real concurrent creation exposed Docker's
  name-reserved-but-not-inspectable interval; bounded lookup retries handle it, with
  a deterministic delayed-visibility mock regression. The concurrent-start test exposed
  duplicate starts; a bounded inspect/start gate fixes that race. A real resource-
  drift test exposed cleanup rejection; immutable ownership now permits cleanup
  while changed launch settings remain rejected. A stalled request respects the
  shorter local authority deadline.
- Reviewed mutation ownership, deadlines and ambiguity, input-free workspace mounts,
  resource conversions, non-root/security settings, cleanup after expiry/drift,
  dependency changes, and fixture isolation. New dependencies compile on pinned
  Rust 1.88.0; existing locked versions remain unchanged.
- `make test lint smoke` and `make integration` passed, including the Linux clock,
  real Docker lifecycle, fresh/rollback/reapply migrations, and PostgreSQL/mTLS
  regressions. Added the runtime fixture to integration and documented observed
  evidence and limits in `docs/docker-runtime.md`.
- This is cached-image execution using explicitly soft development workspaces.
  Image staging, strict filesystem quotas, production journaling/recovery, live
  supervision, artifact transfer, and independent-host release gates remain open.

### D09b: transactional lease renewal and bounded replay grants

- Added migration 0007 and `store.RenewLeases` for 1–64 authorities per current
  worker session. Only current active, unexpired, uncancelled attempts renew; all
  other members receive explicit decisions with no execution authority.
- Persisted original batch grants and a canonical authority-set hash. Concurrent
  retries return the original remaining authority, preserve caller order, and
  cannot borrow a newer renewal's expiry. Changed payloads conflict; revoked or
  replaced sessions cannot replay authority. New periodic renewals use fresh UUIDs.
- Renewal avoids the scheduler advisory lock and worker row lock. Sorted job and
  attempt locks coordinate recovery; fresh database wall time follows lock waits.
  Phase deadlines, physical reservations, and worker readiness remain unchanged.
- First tests failed for the absent API; after implementation, race-enabled real
  PostgreSQL tests passed. Additional tests verified observed-lock expiry and
  takeover races, overlapping opposite-order batches, rollback of both leases on
  injected history failure, unknown attempts, corrupt history, and independence
  from held scheduler/worker locks. Validation also covers batch/identity bounds.
- `sh scripts/test-store.sh`, `sh scripts/test-schema.sh`, and `make test lint smoke`
  passed. The initial sandboxed workspace check could not bind an existing local
  HTTP test listener; rerunning with local socket access passed. No production
  services were modified; the scripts removed their disposable test containers.
- Documented behavior, locking, replay identity, verification, and pending retention
  in `docs/worker-leases.md`. Renewal RPC/client/loop, reaper, supervisor, and all
  remaining full v0.1 gates stay open.

### D09c: authenticated lease renewal RPC

- Wired the existing `RenewLeases` protocol method into the verified store path.
  mTLS identity binding, live credential checks, nested session ownership, batch
  limits, generation overflow checks, and stable RPC errors protect the boundary.
- Converted remaining authority to signed, floored milliseconds before unsigned
  wire encoding. Rejected and submillisecond grants expose no execution duration;
  unknown decisions and out-of-policy grants fail closed.
- Added failing wire tests before implementation. Unit checks now cover identity,
  duration underflow/flooring, explicit rejection decisions, and malformed results.
- Extended the real mTLS/PostgreSQL service test to renew a job, restart the service,
  replay the same request, and confirm durable expiry is unchanged. It rejects
  invalid batch sizes/duplicates, generation overflow, cross-worker/session claims,
  changed replay payloads, expired leases, old sessions, and revoked credentials.
- `sh scripts/test-store.sh` and `make test lint smoke` passed. Reviewed the adapter
  and documented response/error semantics in `docs/worker-leases.md`. No protocol
  regeneration was needed because this method already existed in the contract.
- Next: Rust renewal client with per-attempt conservative authority windows, then
  its production maintenance/supervision integration. Full v0.1 remains open.

### D09d: Rust renewal client and delayed-grant rejection

- Added `ControlClient::renew` with bounded validated batches and caller-owned
  operation identities. It checks complete result tuples/count/order and rejects
  malformed responses before returning any grants. Renewal outcomes distinguish
  live immutable identity/window pairs, explicit server rejections, and locally
  expired grants that carry only cleanup identity.
- Reused the existing request-send monotonic authority calculation and five-second
  margin, then rechecked windows after full response validation. Kept periodic
  scheduling and runtime supervision outside the transport method.
- Tests first failed for the missing API. Rust unit tests now cover identity/batch
  validation, substituted/reordered/missing results, invalid decision/duration
  bounds, and mixed live/rejected/expired outcomes. A direct local compile initially
  selected the unconfigured Xcode toolchain; using the already-established
  DEVELOPER_DIR=/Library/Developer/CommandLineTools resolved the linker setup.
- Extended the real Rust work probe to renew and retry each acquired job through
  mTLS. PostgreSQL verifies one original grant record per UUID and unchanged exact
  expiries after replay. Added a separate delayed-renewal response case: 200 ms
  delivery exceeds the 100 ms usable window after the local margin, and the real
  client rejects execution authority despite a successful RPC.
- `sh scripts/test-store.sh` and `make test lint smoke` passed; clippy reported no
  warnings. Reviewed full-tuple binding, unsigned bounds, batch publication, and
  elapsed-time handling. Updated `docs/worker-leases.md` and `docs/acquisition.md`.
- The production renewal loop, per-attempt supervision, durable journal, reaper,
  and remaining full v0.1 gates are still required. No container execution or
  termination-at-expiry gate is claimed from a protocol fixture.

### D08c: fenced attempt phase transitions

- Added migration 0008 for immutable container bindings and durable phase-report
  identities, plus `store.ReportPhase` for startup, running, and finalization.
  Startup acknowledgement preserves the assignment timeout; forward transitions
  use configured deadlines without renewing leases or releasing reservations.
- Reports validate full ownership, current session, fresh post-lock database time,
  cancellation, container identity, and exit status. Replays return current progress
  without regression or deadline resets. Fast-exit processes may move directly from
  STARTING to FINALIZING with a container ID and exit status.
- The store gate first failed for missing types/API. Real PostgreSQL tests now pass
  for concurrent replay, cross-attempt event-ID conflicts, deadline bounds, terminal
  outcomes, cancellation/expiry, stale authority, immutable container identity,
  changed exit evidence, and transaction rollback on injected event-write failure.
  An observed lock-wait expiry test proves transaction-start time is not used.
- Confirmed phase reporting does not wait on held scheduler or worker row locks.
  Reviewed event-claim/FK ordering and fresh-time evaluation after replay waits.
  Shared only the existing current-session check and ownership-lock helpers with
  renewal; the phase path does not touch capacity or publication.
- `sh scripts/test-store.sh`, `sh scripts/test-schema.sh`, and `make test lint smoke`
  passed. Documented semantics, replay, transitions, verification, and worker
  integration requirements in `docs/worker-phases.md`.
- Phase RPC/client, journaling, production renewal/supervision, and all remaining
  v0.1 release gates stay open. A database phase report is not runtime evidence.

### D08d: authenticated phase-reporting RPC

- Connected the existing `ReportPhase` RPC to the verified store. The adapter
  binds mTLS worker identity, rejects unreportable/unknown phases and unsigned
  generation overflow, and preserves explicit zero versus absent exit status.
- Added strict decision/state mapping so malformed internal results cannot become
  accepted progress or invented terminal outcomes. Unknown tuples may omit state
  only with FENCED; terminal decisions must name an actual terminal phase.
- Added failing adapter tests before implementation. The real mTLS/PostgreSQL
  workflow now reports startup/running, replays old progress, restarts the service,
  replays again, and finalizes with zero exit status verified in the database.
  Invalid evidence, changed payloads, cross-host claims, expiry, replaced sessions,
  and live credential revocation are rejected through the transport.
- `sh scripts/test-store.sh` and `make test lint smoke` passed. Reviewed nil request
  handling, enum conversion, optional-field preservation, and error mapping.
  Documented RPC semantics and evidence in `docs/worker-phases.md`.
- Rust phase reporting, journal/supervisor integration, and full v0.1 remain open.

### D08e: Rust phase client and cross-language progress replay

- Added `ControlClient::report_phase` using the existing bounded mTLS transport.
  It validates event/authority identifiers, signed generation bounds, phase-specific
  container and exit evidence, and consistent decision/state responses. Accepted
  replies can acknowledge later progress but cannot regress or invent completion.
- Returned a typed progress status with no execution window. Stable event IDs and
  exact payload retention remain the caller/journal's responsibility; the transport
  method does not retry, renew leases, or update a supervisor implicitly.
- Added failing Rust tests before implementation. Unit tests now check invalid
  authority/evidence, explicit zero versus absent exit status, malformed enums,
  regressing acknowledgements, and inconsistent terminal/active decisions.
- Extended the real Rust/Go/PostgreSQL protocol fixture with three phases and retries
  per attempt. Database assertions verify three finalizing attempts with zero exit
  status and exactly nine report records/events despite duplicate delivery.
- The delayed-renewal fault fixture initially returned an old requested phase after
  later progress, preventing the test from reaching its intended delay. Corrected
  the fixture to retain monotonic phase state; delayed renewal rejection now passes
  alongside the real database workflow. Synthetic fixture evidence does not count
  as actual container observation or the production supervisor gate.
- `sh scripts/test-store.sh` and `make test lint smoke` passed. Reviewed transport
  deadlines, optional evidence, state ordering, and separation from lease grants.
  Updated `docs/worker-phases.md` and `docs/acquisition.md`.
- Next required integration includes the durable journal and per-attempt supervisor;
  periodic renewal, recovery/reaping, storage/publication, and full v0.1 stay open.

### D10a: durable attempt evidence and phase retry identities

- Added a bounded private attempt journal with exact assignment/spec bytes,
  immutable container and exit/OOM evidence, and durable phase-report UUIDs and
  payloads. Assignment replay preserves evidence; conflicting data is rejected.
  Stored transport timing is zeroed so reopening cannot restore lease authority.
- Updates sync a complete framed/checksummed replacement, rename it on the same
  filesystem, and sync the directory before returning. An exclusive permanent
  flock inode protects ownership; ambiguous write errors poison the current handle.
  Private modes, ownership, link count, canonical paths, frame bounds, and semantic
  validation protect recovery. Trusted ancestors remain an installation requirement.
- Native tests cover replay, ordering, fast exit, budgets, unsafe files, corruption,
  and concurrent owners. Injected failures at five commit steps recover a whole old
  or new record. Killing a separate owner process releases the lock and preserves
  the exact saved retry identity. Review fixed a readiness-marker race in that test.
- Added a complete Linux worker gate to `make integration`, using an anonymous
  Linux volume on Docker Desktop. Initial runs exposed rustup component setup on
  the read-only root and a Linux-only crate missing from the native cache. Selecting
  the image's installed toolchain and fetching locked target dependencies before
  offline compilation resolved both. Protoc archives use official pinned checksums.
- `make test lint smoke` and `make integration` passed. Integration includes Linux
  journal/clock tests, real Docker execution/resource enforcement, fresh/down/up
  migrations, and real PostgreSQL/mTLS Go/Rust workflows. The final test-marker fix
  also passed the focused native journal and clippy gates.
- Documented format, write ordering, caller responsibilities, resource bounds,
  verification, and limitations in `docs/worker-journal.md`. These tests establish
  process/error recovery, not power-loss durability. Session persistence, production
  supervision, old-container cleanup, completion retention, and full v0.1 remain open.

### D10b: runtime discovery and old-session cleanup

- Added bounded worker-label inventory with fresh full-ID inspection, stable ordering,
  exact ownership/image/spec/profile validation, and a distinct cleanup-only handle.
  More than 1024 entries, duplicates, malformed metadata, and daemon failures cannot
  become a complete snapshot. Listing plus inspection has one five-second deadline.
- Added idempotent previous-session forced removal after ownership revalidation;
  current-session containers are refused. Forced removal handles paused executions
  without resuming them, disables volume deletion, and verifies actual absence.
  Callers must establish server fencing before using this path and repeat inventory
  before reporting reconciliation complete.
- The acceptance test first failed because the recovery API did not exist. Real
  Docker tests now discover and remove paused, created, and exited old containers
  through a fresh client while preserving current-session and foreign-worker work.
  Fixtures use isolated job/worker identities and retain the existing narrow cleanup.
- Unix-socket fault tests cover truncated/duplicate/foreign inventory, ten malformed
  identity cases, changed ownership, missing containers, uncertain delete replies,
  false delete success, and stalled discovery. An initial parallel test exposed
  timestamp collisions in fixture names; an atomic counter removes that ambiguity.
- `make test lint smoke`, `sh scripts/test-runtime.sh`, the final focused native
  recovery/clippy gate, and `sh scripts/test-worker-linux.sh` passed. Checked the
  pinned Bollard query source for list limits/filtering and forced-delete semantics.
  Documented caller preconditions, bounds, retention, and evidence in
  `docs/docker-runtime.md`.
- This verifies the runtime recovery component, not the full agent restart protocol.
  Session persistence, registration/heartbeat orchestration, supervisor integration,
  storage/publication, and the remaining v0.1 acceptance gates stay open.

### D10c: durable registration and fresh incarnation identity

- Added private bounded `.session` metadata using the verified journal commit path.
  Beginning an incarnation persists generated request/session UUIDs and complete
  registration claims before exposing the request. A handle cannot begin twice.
  Registration acknowledgement records only matching identity and generation;
  replay is idempotent and changed generation conflicts.
- Reopening exposes the preceding request as history, including an uncertain
  registration, but cannot acknowledge that old incarnation. A replacement writes
  fresh IDs and retains the predecessor ID without modifying attempt evidence.
  Server recovery remains responsible for fencing; saved readiness/lease authority
  is never restored. Missing worker identity with session metadata is corruption.
- Tests first failed for the missing API. Native and Linux tests now cover exact
  request fields across reopen, retry/acknowledgement conflicts, invalid claims,
  corruption without overwrite, and prior attempt preservation. The killed-owner
  subprocess test additionally proves an unacknowledged session survives process
  death and the replacement chooses a different incarnation.
- Cross-checked bounds against the Go registration contract, including 128-byte
  ASCII label keys with uppercase support, whole-MiB resources, and scratch
  capability exclusivity. Session protobuf map ordering is intentionally not treated
  as canonical bytes; replay preserves values and IDs and the server hashes
  normalized claims. Session metadata adds at most 64 KiB plus framing outside the
  attempt byte budget.
- `make test lint smoke` and `sh scripts/test-worker-linux.sh` passed. Updated
  `docs/worker-journal.md` and `docs/worker-sessions.md`. No worker RPC or database
  semantics changed in this slice. Production startup orchestration, heartbeat and
  supervisor loops, storage/publication, and all remaining v0.1 gates stay open.

### D10d: actual worker startup, cleanup, and health loop

- Added `dispatch-worker run --config FILE --dev-soft-scratch` with bounded strict
  configuration, explicit TLS credentials, private nonoverlapping state/workspace
  directories, and durable fresh-incarnation registration. The development flag
  prevents implying that quota-backed scratch or the release profile is complete.
- Connected the verified journal, mTLS client, and Docker recovery adapter. The
  process persists its acknowledgement before readiness, interleaves heartbeats
  with one-container cleanup steps, and requires a fresh empty healthy inventory
  plus server agreement before announcing READY. CPU/memory/architecture claims
  are checked against Docker; workspace space checks report pressure conservatively.
- Added stable heartbeat sequences/request IDs across uncertain replies. Cleanup
  continues while retrying an unchanged pending report. Registration retries use
  the same durable identity, including while SESSION_ACTIVE awaits automatic
  recovery or explicit operator approval. Revocation/fencing errors remain terminal.
- Configuration tests first failed because the agent module did not exist. They
  now cover bounds, unknown/duplicate fields and labels, HTTPS/local paths, and the
  required development profile. Local file errors omit paths/content; key files
  reject public permissions, foreign ownership, symlink leaves, and hard links.
- The real process integration test uses PostgreSQL, mTLS, and an old Docker
  container. It approves exactly the emitted replacement session, verifies cleanup
  before any complete health report, observes generation 2 and one registration,
  loses a committed heartbeat reply and checks replay, rejects a second process
  sharing state, and verifies process exit after credential revocation. `store-test`
  now builds worker binaries in addition to its protocol fixtures.
- `make test lint smoke` and `make integration` passed, including Linux worker
  tests, real Docker checks, fresh/down/up migrations, and all Go/Rust PostgreSQL
  workflows. Documented setup, retries, state events, and limitations in
  `docs/worker-agent.md`, with links from the running/session/journal documentation.
- The old container in this test is fixture-created. The agent-owned job kill/restart
  gate still depends on acquisition and supervision. This worker reports readiness
  but does not yet acquire work. Strict scratch, signal shutdown, leases during
  active work, storage/publication, and the remaining complete v0.1 gates stay open.

### D09e: live-container authority watchdog

- Added a bounded watch channel for exact attempt identity and live authority.
  Typed renewal updates cannot revive an expired window or overwrite a stop.
  Server rejection, controller loss, expiry, and runtime uncertainty cause immediate
  kill followed by an independent bounded inspection. Unconfirmed cleanup remains
  explicit; the watchdog does not publish results, release reservations, or delete
  containers and evidence.
- Tests cover stalled and slow inspections, identity mismatch, late renewal,
  continuing valid renewal, all three rejection decisions, and uncertain cleanup.
  The real Docker fixture starts a sleeper, expires its synthetic local grant,
  observes confirmed termination, and independently checks that it is stopped.
  This is component evidence, not a real control-channel partition gate.
- Reviewed the pinned Tokio watch implementation: `send_if_modified` serializes
  state changes under its write lock. Runtime futures remain pinned across deadline
  checks so slow inspections are not continually restarted. Checks use the earlier
  of remaining authority and 100 ms; cleanup has separate five-second budgets.
- Verification handles from the preceding turn were no longer available, so fresh
  `make test lint smoke` and `make integration` runs supplied current evidence.
  Native testing exposed same-tick temporary-directory collisions in the existing
  Docker fault fixture. Added a process ID and atomic counter to its timestamp name;
  focused fault tests, final native checks, and final Linux worker tests passed.
  The full integration run also passed Docker, migrations, and PostgreSQL workflows.
- Documented the component and its limits in `docs/worker-supervision.md` and linked
  it from lease documentation. Periodic batched renewal, durable launch coordination,
  agent acquisition, reaping, finalization, and the remaining v0.1 gates stay open.

### D09f: periodic bounded renewal and watchdog authority updates

- Added `ControlClient::maintain_leases` for fixed same-session batches of 1–64
  attempt controllers. Renewal starts immediately, then runs every five seconds
  with a five-second RPC bound. Retryable failures wait one second and replay the
  same UUID, order, and payload, including members that finish during uncertainty.
  Resolved batches use a new UUID and omit inactive consumers on the next period.
- Connected validated grants/rejections to the watchdog channel. Transport failure
  cannot extend local deadlines. Fatal protocol/authentication/clock errors stop
  authority; exhausted or closed consumers retire. Added safe client cloning for
  independent RPC tasks sharing the existing authenticated Tonic channel.
- Added a drop guard that stops retained authority even when another owner holds
  cloned senders. Review identified cancellation before the first future poll as
  a gap: its regression test failed, then passed after constructing the guard before
  returning the future. The future remains `Send` and is spawned by the real probe.
- Tests first failed for the missing renewal loop. Eight unit tests now cover exact
  replay and changed membership, 64/65-member boundaries, empty/duplicate/cross-session
  input, outage expiry, stalled RPCs, fatal/malformed responses, and both cancellation
  timings. Existing supervisor and validated-client tests cover sticky rejection,
  late grants, exact identity, and runtime termination.
- Added a real Rust subprocess/mTLS/PostgreSQL test. It reads two live assignments,
  loses a committed renewal reply, verifies exact retry, observes the next period's
  new identity, expires database authority, and checks fencing ends the loop. Three
  RPC calls create exactly two durable renewal records. Readiness is fixture-seeded;
  this test does not launch containers or prove active-job recovery.
- `make test lint smoke` and `make integration` passed. After the cancellation fix,
  final native test/lint/smoke, Linux worker tests, and the race-enabled real
  PostgreSQL/mTLS suite passed again. Documented lifecycle, timing, batching, and
  cancellation in `docs/worker-leases.md` and updated supervisor links.
- Durable launch/phase coordination, acquisition orchestration, cross-batch scheduling,
  finalization, reaping, and all remaining v0.1 release gates stay open. The startup
  command still reports readiness without acquiring jobs.

### D10e: serialized asynchronous journal access

- Added cloneable `AsyncJournal` ownership for attempt operations and registration
  acknowledgement. One shared semaphore is acquired asynchronously before spawning
  blocking work, so only one journal operation per worker occupies the blocking
  pool. The journal mutex and existing file lock preserve exclusive ownership and
  poison semantics across all handles.
- The blocking closure retains its permit and journal reference even if its async
  caller is cancelled. A late filesystem commit remains possible and must be
  reconciled; cancellation is not evidence of rollback. A blocking panic poisons
  later access. No filesystem format, commit boundary, or lease semantics changed.
- Routed actual worker registration persistence through this handle and retained
  it for the health loop lifetime. This is the bounded filesystem prerequisite for
  concurrent launch/renewal/watchdog work; the startup command still acquires no jobs.
- Tests first failed for the missing API. A single-thread Tokio test now stalls one
  blocking operation, cancels its waiter, verifies timers still progress and the
  journal remains exclusively locked, then checks a queued operation starts only
  after release. Concurrent real writes produce one STARTING event; reopen preserves
  the exact FINALIZING request, OOM evidence, and zeroed persisted authority timing.
- Checked the pinned Tokio `spawn_blocking` documentation for cancellation and
  shutdown behavior. Documented kernel-stall/shutdown limits, caller queue bounds,
  and uncertain commit handling in `docs/worker-journal.md`.
- Focused journal tests, `make test lint smoke`, Linux worker tests, and the real
  PostgreSQL/mTLS/Docker startup suite passed. The actual startup test still rejects
  a second process sharing the journal and exits after credential revocation.
- Durable launch/phase sequencing and the remaining full v0.1 gates stay open.

### D08f: durable cached-image launch coordinator

- Connected exact assignment/session/channel/workspace identity, acknowledged current
  journal incarnation, a serialized unique attempt claim, durable STARTING intent,
  phase acknowledgement, Docker creation, durable container binding, one start call,
  and post-start inspection. Repeated deliveries and already-advanced phase replies
  cannot initiate another launch. A fast exit is accepted as process evidence.
- STARTING transport retries preserve the journaled request. Runtime start uncertainty
  is resolved through inspection without repeating start. Live authority checks bound
  journal/RPC/runtime waits; failure with a known handle uses shared bounded kill and
  confirmation. Unknown create outcomes remain explicitly uncertain. No cleanup path
  deletes logs/containers or releases server capacity.
- Added receiver identity/current-window access within the worker and a runtime
  container-ID accessor for journal binding. Factored the existing watchdog termination
  sequence into a shared helper without changing its stop-confirmation requirements.
- Tests first failed for the missing launch API. Seven real-journal/fake-runtime tests
  cover ordering and exact phase replay, duplicate delivery, ambiguous start, fast
  exit, expiry during RPC/start, unknown create, failed durable binding, unacknowledged
  or wrong session, generation mismatch, and server rejection/advanced progress.
- `make test lint smoke`, Linux worker tests, and real Docker lifecycle/recovery
  checks passed. Documented the component, caller preconditions, cancellation limits,
  cleanup evidence, and verification scope in `docs/worker-launch.md`.
- Combined real service/Docker launch verification, post-launch phase orchestration,
  acquisition, staging/strict workspaces, and the remaining full v0.1 gates stay open.

### D08g: real authenticated launch, renewal, and fencing fixture

- Added `launch_probe` and a real PostgreSQL/mTLS/Docker integration test. The probe
  creates a durable incarnation, registers, checks daemon capacity and empty inventory,
  reports health, acquires a queued pinned workload, and launches through the verified
  coordinator while a separate task maintains its lease.
- The service commits STARTING then loses its reply. Exact event/payload replay is
  asserted before the probe confirms a running container and matching durable binding.
  The fixture explicitly journals/reports RUNNING; this direct sequence is test setup,
  not an implementation of general post-launch phase orchestration.
- A later periodic renewal expires real database authority and returns FENCED. The
  watchdog kills and confirms the actual container, and independent Docker inspection
  checks it is stopped. Database phase history contains only STARTING and RUNNING.
  Test cleanup targets only the unpredictable provisioned worker label.
- The real race-enabled PostgreSQL/mTLS suite, all-target Clippy, and final Linux
  worker tests (including fixture compilation) passed. This
  fixture uses the development soft-scratch policy and does not replace the release
  gates for strict Linux storage, production acquisition/finalization, complete result
  publication, independent hosts, or active-job worker restart.

### D09g: post-phase authority refresh through the renewal producer

- Added an explicit authority refresh barrier that wakes the existing periodic
  producer. Each new renewal operation captures refresh revisions; only an applied
  live grant from that operation can acknowledge them. An uncertain older request
  retains its exact retry before a new UUID is constructed. Stale wake permits do
  not cause surplus RPCs, and no second producer competes for the attempt.
- Existing authority and sticky rejection checks remain active while refresh waits.
  Neither a phase acknowledgement, an old replay, nor a missing producer creates
  additional execution time. Documented cancellation behavior and the one-producer
  requirement in `docs/worker-leases.md`.
- Four new tests cover prompt refresh, uncertain replay followed by a distinct
  operation, expiry without a producer, and fencing during refresh. The real launch
  probe now requires its post-RUNNING refresh to observe server fencing before the
  watchdog kills and independently confirms the Docker container stopped.
- `make test lint smoke` and `make integration` passed with confirmed zero exits.
  Integration includes Linux worker tests, actual Docker lifecycle/recovery,
  PostgreSQL migration rollback/reapply, and the race-enabled store/mTLS/executable
  suites. Initial sandbox attempts could not open local sockets; the same checks
  passed with authorized local socket access.
- Production RUNNING/FINALIZING coordination still needs conservative local bounds
  while phase commits and replies are uncertain. Acquisition, finalization output
  publication, reaping, and the remaining full v0.1 release gates remain open.

### D08h: supervised execution through fresh FINALIZING authority

- Added `launch::execute`, which owns an authority receiver, uses the verified durable
  launch, observes its bound runtime handle, journals/reports RUNNING, and proceeds
  through observed exit to durable FINALIZING. Runtime inspection remains active
  while journal writes, phase retries, and refresh barriers wait. Fast exits skip
  RUNNING; exit during an uncertain RUNNING reply cancels that wait and advances
  safely through the server's existing phase rules.
- Added conservative local phase bounds using the existing suspend-aware clock.
  They start before possible phase commits and persist across retries and refresh.
  An old startup window or continuing renewal cannot bypass a shorter, uncertain
  execution phase. After fresh authority arrives, its server phase bound applies.
- Exit code and the separately observed OOM flag are durable before FINALIZING.
  Exact phase payloads survive transient retries. Success retains the handle,
  identity, exit evidence, and authority consumer for future artifact work. Errors
  retain evidence and use bounded kill/confirmation without releasing reservations
  or claiming a terminal result. Aborting the future still cannot run async cleanup.
- Tests first failed for the missing execution API. Six execution tests and a clock
  test now cover normal and fast exit, OOM evidence, finalization replay, uncertain
  RUNNING replies, finalization rejection, missing fresh grants, and one-second
  execution bounds despite continuing long renewal grants. The missing-grant test
  uses a one-second initial window so its real journal writes reach FINALIZING
  before testing expiry; a 100-ms window initially expired during startup work.
- Extended the existing real PostgreSQL/mTLS/Docker fixture with exit-code-7
  workloads. Both new cases lose and replay a committed FINALIZING reply, preserve
  exactly three phase records, and obtain fresh authority. One withholds RUNNING's
  committed reply; FINALIZING must arrive before that RPC's deadline, proving runtime
  observation does not serialize behind the stalled call.
- `make test lint smoke` and `make integration` passed with zero exits. Native and
  Linux worker libraries each passed 52 tests; real Docker lifecycle/recovery,
  migrations, and race-enabled database/service/executable checks passed. After
  strengthening the before-deadline assertion, `scripts/test-store.sh` passed again.
- Updated the component docs, overview, and ledger statuses. Production acquisition,
  staging/image pulling, strict workspaces, artifacts/completion, reaping, and all
  remaining v0.1 acceptance gates stay open.

### D11a: bounded versioned S3-compatible storage adapter

- Added `internal/objectstore` with explicit endpoint/region/bucket/static credentials,
  versioning checks, signed single-part upload/download capabilities, and exact-version
  streaming SHA-256 verification. It does not use ambient AWS profiles, instance
  metadata, or implicit cloud endpoints. Account-free development uses SeaweedFS 4.47
  pinned by manifest digest; no existing AWS account or user object store is accessed.
- Upload capabilities bind one key, declared size, and checksum and expire in one
  second to five minutes. Downloads pin an opaque non-null version. Verification
  independently checks returned version, size, and bytes rather than trusting ETag.
  Versioning is rechecked before upload signing; a suspended bucket cannot mint a
  new grant. Reusable upload URLs may create later versions without changing an
  already verified exact-version reference.
- Server requests stay on the configured origin, refuse redirects, use HTTPS except
  explicit loopback development, and bound response headers/XML bodies. Operations
  and queued work honor caller deadlines with a 30-second maximum. Concurrency
  defaults to four; single-part bytes default to 64 MiB and may be lowered. SDK
  diagnostics are reduced to stable categories so signed URLs and credential data
  are not copied into errors.
- Tests first failed for the missing adapter API. Unit tests cover invalid settings,
  signed fields, exact versions, misleading ETags, integrity mismatches, cancellation,
  redirect refusal, and the concurrency bound. The real SeaweedFS test rejects
  unversioned/suspended buckets, tampered checksums/size/key, expired URLs, wrong
  contents, and deleted versions. It proves overwrites and replayed uploads leave
  the original version downloadable, and verifies empty and configured-limit outputs.
- Added `scripts/test-objectstore.sh` and `make objectstore-test`; `make integration`
  now includes this isolated backend. The fixture uses fresh credentials, a loopback
  port, bounded container resources, and ephemeral tmpfs. Cleanup targets only its
  own container. Official SDK modules and transitive versions are pinned in Go's
  lockfiles; docs record the API references and backend compatibility evidence.
- `make test lint smoke` and full `make integration` passed with zero exits. After
  adding real empty/size-boundary coverage, the isolated object-store suite passed
  again. The full gate retained Linux/Docker, migrations, and race-enabled PostgreSQL,
  mTLS, and execution-to-finalization checks.
- This slice is a storage primitive, not artifact authorization or job completion.
  Pending upload metadata, lease/session fencing during verification/publication,
  worker transfers, multipart support, retention, and the remaining v0.1 gates stay open.

### D11b: durable, fenced upload declarations

- Added immutable upload declarations bound to the full project/job/attempt/worker/
  session/generation identity. PostgreSQL generates attempt-specific object keys;
  workers cannot select writable paths. Request UUIDs deduplicate identical retries
  and reject changed payloads, including reuse across attempts.
- Creation rechecks credentials, session ownership, cancellation, and fresh database
  time after ownership/replay lock waits. It changes no lease, phase, or reservation
  and performs no network I/O. Only declared outputs in FINALIZING, reserved log
  streams in active execution phases, and bounded result manifests are accepted.
- Single-part declarations are bounded to 64 MiB each, 1024 per attempt, and 8 GiB
  total. Competing requests serialize on the attempt; replay consumes no additional
  budget. The declaration and its event/sequence update commit or roll back together.
- Unit and real PostgreSQL tests cover concurrent replay, request conflicts, scope
  foreign keys, cap races, event failure rollback, revoked credentials, takeover,
  cancellation, and expiry during a confirmed lock wait. Scheduler and worker locks
  do not block creation. A populated migration-eight-to-nine upgrade preserves an
  active attempt's identity and deadlines.
- Review reproduced an unchanged-row update failing because a BEFORE trigger read
  the generated key before computation. Moved the immutable-row check to AFTER;
  the regression now accepts a no-op while still rejecting content/key mutation.
- `make test lint smoke` and full `make integration` passed before the trigger
  refinement. The final `scripts/test-schema.sh` and `scripts/test-store.sh` passed
  again, covering fresh/down/reapply migrations and race-enabled database, mTLS,
  and executable suites. No dependencies or worker behavior changed in this slice.
- Upload signing through the service, exact-version verification/publication,
  worker transfers, multipart support, and the remaining v0.1 release gates remain.

### D11c: authenticated upload grants and server storage configuration

- Implemented `CreateUpload` over the existing mTLS worker service. It commits the
  durable declaration, signs a 30-second PUT after checking bucket versioning outside
  database locks, then replays the declaration to recheck current authority before
  returning the URL. Retries preserve upload ID/key and may refresh the capability;
  storage failures retain one declaration/event and do not renew compute authority.
- The wire response preserves required signed headers and expiry. Invalid kinds,
  multipart requests, oversized unsigned values, cross-worker claims, stale tuples,
  and undeclared outputs are rejected. Stable errors distinguish storage failure,
  missing versioning/configuration, fencing, cancellation, and exhausted budgets
  without exposing URLs or backend diagnostics.
- Added explicit endpoint/region/bucket server options and separate loopback-HTTP
  opt-in. Credentials come only from `DISPATCH_S3_*` environment variables. Startup
  verifies a configured bucket before opening listeners; ambient AWS credentials
  cannot enable storage. Without storage configuration, other APIs remain usable.
- The initial RPC test failed with Unimplemented; configuration tests failed on the
  missing configuration boundary. Unit/configuration tests now pass. PostgreSQL/mTLS
  tests verify signed fields, unchanged deadlines, durable retry behavior, and fault
  recovery. During blocked versioning I/O, independent expiry/cancellation/revocation
  transactions commit and the RPC subsequently refuses to return a capability.
- Extended the isolated storage fixture to run a supplied command. Full integration
  now runs the database suites with a fresh SeaweedFS endpoint. A real mTLS grant
  successfully transfers bytes verified by exact version; tampered checksum/key fail.
  Cancellation rejects new grants while a previously issued capability can create
  another version without changing the original. This is scoped transfer evidence,
  not yet verified-artifact registration or result acceptance.
- `make test lint smoke` and full `make integration` passed with zero exits, including
  native/Linux worker tests, real Docker lifecycle/recovery, schema rollback/reapply,
  object-store checks, and Go race-enabled database/mTLS/executable suites. Reviewed
  the configuration, auth ordering, lock boundary, retry state, and secret handling.
- Documented configuration in `docs/running.md`, RPC semantics/errors in
  `docs/artifact-uploads.md`, and the combined fixture in `docs/object-storage.md`.
  `FinalizeUpload`, immutable verified-artifact records, terminal publication,
  worker transfer integration, multipart support, and remaining v0.1 gates stay open.

### D11d: fenced exact-version artifact registration

- Added immutable finalization intents and verified artifacts in migration 0010.
  Replay hashes bind the complete authority, upload, key, version, size, and checksum.
  Foreign keys tie each artifact to its exact finalization/upload/version, and an
  upload can select only one artifact. Attempts retain at most 1024 finalization
  request identities; exact retries do not consume more history.
- `store.FinalizeUpload` commits a preflight intent, invokes a trusted exact-object
  verifier without database locks, then rechecks current authority in a new
  transaction before committing the artifact and event together. No public store
  method bypasses the verifier callback. Storage failures preserve retry identity;
  expired/cancelled/revoked/taken-over attempts cannot register verified data.
- Concurrent equal verifications return one artifact/event. Different candidates
  can be read concurrently, but the first committed version wins and cannot be
  rebound. Replays of the selected object use the durable verified record without
  repeating storage I/O. Job success, reservations, phases, and leases are unchanged.
- Initial unit tests failed for missing request/object types. Passing unit and real
  PostgreSQL tests cover malformed objects, 16 concurrent verifications, competing
  versions, immutable records, exact-version foreign keys, request-budget races,
  storage retry, event rollback, and authorization changes during verification.
  Populated upgrades preserve active schema-eight ownership and schema-nine uploads.
- `make test lint smoke` and full `make integration` passed with zero exits. The
  gates include native/Linux workers, Docker, schema rollback/reapply, SeaweedFS,
  and race-enabled database/mTLS/executable checks. Store verifier fixtures isolate
  concurrency; they do not replace the real object-storage compatibility evidence.
- Documented the transaction/replay/retention contract in `docs/verified-artifacts.md`.
  Real `FinalizeUpload` RPC verification, terminal manifest publication, worker
  transfers, multipart support, and the rest of v0.1 remain required.

### D11e: authenticated exact-version finalization

- Implemented `WorkerService.FinalizeUpload` using the verified store boundary and
  real storage adapter. The mTLS identity, unsigned wire bounds, object presence,
  and single-part profile are checked before dispatch. The verifier receives only
  metadata already matched to the durable upload; its streamed version/length/hash
  checks precede the transaction that revalidates authority and records the artifact.
- Replies contain the stable artifact UUID and exact object reference. Integrity
  mismatch, unavailable storage, invalid declarations, conflicts, and fencing use
  stable reasons without copying SDK/backend diagnostics. A shared upload-decision
  mapper keeps creation/finalization rejection semantics consistent.
- The first boundary test failed with Unimplemented. Passing tests now cover wire
  validation, wrong version/size/body, cancelled verification followed by the same
  request retry, and replay of a durable success while storage is unavailable.
  A partially streamed body stays blocked while separate database transactions
  expire leases/phases, cancel work, or revoke credentials; each subsequently
  prevents artifact registration without blocking the database mutation.
- Extended the real PostgreSQL/mTLS/SeaweedFS test: upload via RPC grant, overwrite
  the current key with different bytes, reject finalization of that wrong version,
  then register the original exact version and replay its stable result. Later URL
  reuse cannot rebind the artifact. Cancellation rejects verified retries as well
  as new grants; verified data alone never authorizes publication.
- `make test lint smoke` and the combined
  `scripts/test-objectstore.sh scripts/test-store.sh` fixture passed with zero exits.
  This reruns the changed Go race-enabled database/mTLS/executable boundary against
  real storage. The preceding D11d full Linux/Docker/migration gate remains applicable;
  no worker runtime or schema changed in D11e.
- Updated the protocol, operator, storage, and verification docs. Terminal manifest
  validation/publication, Rust transfers, multipart support, retention, and the
  remaining v0.1 acceptance requirements stay open.

### D11f: bounded completion payload and digest contract

- Added completion request types and a deterministic payload digest that binds
  authority, exit/failure/stop evidence, verified output references, log completeness
  and gaps, and exact metrics source bytes. Output/gap ordering is normalized without
  mutating caller buffers; request UUID and the claimed digest remain outside payload.
- Enforced output identity/count, nonoverlapping positive log ranges, valid failure
  evidence, and bounded finite numeric metrics with duplicate-key rejection. Kept
  numeric source text to avoid rounding large integers, rejected nonzero underflow,
  and normalized true zero to bound extreme exponent representations.
- Initial tests failed for the missing contract. `make test lint smoke` passed;
  after adding the fixed digest vector and count/zero boundaries, the targeted
  race-enabled completion tests passed again. No database or runtime behavior changed.
- Documented the byte encoding and future Rust compatibility requirement in
  `docs/completion.md`. Atomic publication and its RPC remain the next required slice.

### D11g: atomic completion publication and capacity transition

- Added migration 0011 and the authenticated completion transaction. Required
  outputs must reference verified versions owned by the attempt. Successful
  completion publishes one immutable manifest with the job spec, attempt history,
  exact output/log versions, log gaps, and bounded metrics bound to their source
  artifact bytes. Database constraints bind the canonical job result to this
  successful completion and prevent result/reference updates.
- Terminal state, completion identity, artifact references, reservation release
  or quarantine, retry eligibility, and the event commit together. Authority is
  checked with fresh database time after locks and again after reference writes.
  Cancellation intent wins if committed first; uncertain physical stop retains
  quarantined capacity. No storage I/O occurs inside the transaction.
- Exact authenticated replay returns the original accepted bytes after lease
  expiry or session replacement. Changed evidence or a reused completion ID
  conflicts; historical failed replay cannot mutate a replacement attempt.
- Verification: native `make test lint smoke` and full `make integration` passed,
  including Linux Rust, Docker runtime, schema rollback/reapply, versioned storage,
  and combined PostgreSQL/storage suites. A subsequent `scripts/test-store.sh`
  run passed after adding the historical replacement/reused-ID regression test.
  Store tests cover 16 concurrent completions, missing/foreign outputs, cancellation
  ordering, retry and quarantine, metrics precision/source identity, revoked
  credentials, immutable replay, and populated schema upgrade. Injected event or
  manifest errors and expiry while a reference insert waits leave no partial
  completion, capacity transition, or canonical result.
- Completion RPC, public result retrieval, and the Rust transfer/completion loop
  remain subsequent slices; this gate establishes the store transaction only.

### D11h: authenticated completion RPC

- Connected `CompleteAttempt` to the verified completion transaction with mTLS
  worker binding, bounded wire conversion, and presence-preserving exit evidence.
  Replies validate decision/state combinations and expose the original canonical
  manifest bytes only for accepted success. Database errors retain stable, redacted
  reasons. Completion needs no object-store calls or configured storage adapter.
- Initial boundary tests failed for missing conversion/response implementation.
  Unit checks now pass for malformed/nil entries, unknown reasons, unsigned integer
  overflow, field bounds, omitted exit versus zero, and invalid internal results.
- Native `make test lint smoke` and `scripts/test-store.sh` passed with zero exits.
  The real mTLS/PostgreSQL tests exercise 16 equal concurrent calls yielding one
  completion/event and released reservation, byte-identical retries, exact verified
  versions, changed evidence, missing outputs, foreign authority, lease/phase expiry,
  cancellation acknowledgement, revoked credentials, and rollback after event failure.
  A discarded acknowledgement is recovered over mTLS. Storage is unavailable after
  verification; completion never reads it. The preceding full Linux/Docker/schema/
  versioned-storage gate remains applicable; no schema or Rust runtime changed here.
- Updated completion, protocol, operator, and README status. Public canonical-result
  retrieval and the Rust transfer/completion loop remain subsequent slices.

### D11i: project-scoped accepted result inspection

- Added `acceptedAttemptId` and `acceptedManifest` to job inspection and submission
  replay. Queries join only the job's accepted successful completion, preserving
  its frozen JSON bytes rather than reconstructing a result from mutable upload
  state. Jobs without accepted success expose null fields.
- The Go client and `dispatch jobs get --json` retain raw manifest JSON and exact
  numeric text. No registry/storage lookup or download capability is needed for
  result metadata. Authorized artifact downloads remain a separate slice.
- Initial client coverage failed for the absent result fields. Native
  `make test lint smoke` and real PostgreSQL `scripts/test-store.sh` passed with
  zero exits. Integration coverage publishes a verified output over mTLS and then
  exercises the HTTP server/client: active jobs have no result, successful jobs
  expose the exact accepted identity/manifest, failed diagnostics stay noncanonical,
  submission replay returns the same current result, and another project's token
  receives 404. Client and CLI tests preserve integers above 2^53.
- Updated HTTP/completion contracts and implementation status. Rust transfers,
  completion delivery, and the complete workload-to-download gate remain open.

### D11j: authorized exact-version artifact download grants

- Added `GET /v1/jobs/{id}/artifacts`, projecting only accepted outputs from the
  immutable completion manifest. Responses include exact object metadata and
  60-second GET capabilities. Pending/failed jobs expose empty arrays; missing and
  foreign jobs return 404. The existing explicit storage configuration now supplies
  both worker transfer APIs and HTTP download signing.
- Signing happens without a state transaction; token/project authorization is
  rechecked before returning any URL. A signing failure cannot emit a partial page,
  and error responses omit backend diagnostics and capability URLs. Already issued
  URLs remain bearer capabilities until expiry; revocation cannot recall them.
- Initial endpoint tests failed for the missing API/configuration. The first native
  command hit the sandbox's local-listener restriction; the socket-enabled native
  `make test lint smoke` gate subsequently passed. Both `scripts/test-store.sh` and
  the expanded `scripts/test-objectstore.sh scripts/test-store.sh` combined gate
  passed with zero exits. No schema or Rust runtime changed.
- Tests cover signed version/key/expiry, read-role authorization, foreign/missing
  jobs, pending/failed output exclusion, missing storage, revocation, and redacted
  signing failures. A controlled signer revokes a token while signing and confirms
  revocation can commit without a retained database authorization lock; no grant is
  returned. The real SeaweedFS flow uploads, overwrites the key, verifies the original
  version, completes over mTLS, and downloads original accepted bytes through the
  HTTP-issued grant. Removing the signed version parameter is rejected.
- Added `docs/artifact-downloads.md` and refreshed HTTP/storage/operator contracts.
  CLI download verification, Rust transfers/completion delivery, multipart objects,
  retention, and the full multi-host release gate remain required work.

### D11k: verified local artifact download client

- Added a bounded client path from accepted artifact metadata to a private local
  file. Metadata validates scope, identities, counts, hashes, URL key/version,
  expiry, method, and storage headers before transfer. The shared API request
  reader keeps existing submission/inspection behavior and error handling intact.
- Transfers use a separate credential-free HTTP client with no proxies, redirects,
  or decompression. Version, byte count, and SHA-256 must match before the temporary
  file is synced and published without overwriting existing paths. Directory-relative
  operations preserve the opened directory across parent renames; the final link
  rejects competing destinations. Receipts omit capabilities and credentials.
- Tests initially failed for the missing download method. Client race tests and
  native `make test lint smoke` passed. The combined PostgreSQL/SeaweedFS gate also
  passed after adding the real client to the overwritten-key/original-version flow.
  Fixtures cover invalid metadata, empty/chunked bodies, corruption, partial/oversized
  responses, redirects, cancellation, private permissions, existing files/symlinks,
  competing destination creation, and parent-directory replacement.
- Documented the 64-MiB single-part profile, transfer bounds, atomic publication,
  filesystem requirements, and explicit errors if sync/cleanup fails after verified
  publication. CLI command integration and Rust transfers remain subsequent slices.

### D11l: verified artifact download CLI

- Added `dispatch artifacts download JOB_ID NAME --output FILE [--json]`, using
  the verified download client. Output paths are explicit user inputs, never chosen
  from server metadata. Success prints a terminal-safe text receipt or one JSON
  receipt without transfer capabilities. Errors produce no success receipt and
  retain the existing exit-2/error-stream behavior.
- Initial CLI tests failed because the command was absent. CLI race tests, native
  `make test lint smoke`, and the combined PostgreSQL/SeaweedFS integration gate
  passed. Tests cover usage errors, text/JSON receipts, quoted filenames, absent
  project credentials at storage, and corrupt bytes leaving no destination.
- The real-storage test now builds a fresh CLI binary after verified completion,
  downloads the accepted original version through the HTTP artifact endpoint,
  compares local bytes and receipt identity/hash, and invokes the binary again to
  prove an existing destination is preserved. This extends the real API/version
  overwrite test to executable local publication; no schema or Rust runtime changed.
- Updated README, operator examples, and artifact download documentation. The full
  worker acquisition/staging/transfer/completion loop, multipart objects, and the
  remaining multi-host v0.1 gates remain open.

### D11m: Rust completion payload and Go/Rust parity

- Added `control::completion_digest` with the Go completion contract's authority,
  failure/exit/stop, output, gap, and metric validation. Normalized views preserve
  caller-owned evidence and deterministic field/set ordering. Original metrics
  source bytes are separately hashed; completion UUID and claimed digest stay out
  of the payload while the UUID is still validated.
- Enabled `raw_value` on the existing pinned serde_json version so exact numeric
  text survives serialization. A map visitor rejects duplicate decoded keys before
  insertion. Float parsing enforces finite export bounds without rounding stored
  integers/exponents; nonzero underflow is rejected and true zero is normalized.
  No dependency versions or lockfile changed.
- Initial Rust tests failed for the missing helper. Golden, ordering/immutability,
  evidence, numeric, count, and byte-bound tests pass. `make protocol-test` now sends
  Go protobuf fixtures through Rust hashing/rejection and checks each outcome against
  the Go store contract, including numeric edge cases and int64 boundaries.
- Native tests and the full `make integration` gate passed, including Linux Rust,
  real Docker runtime, migration rollback/reapply, PostgreSQL, and versioned storage
  with CLI downloads. Clippy caught an equivalent boolean simplification and an
  unnecessary Copy-value clone in a test; both were fixed, and final
  `make lint smoke protocol-test` passed with zero exit.
- Updated completion/protocol documentation. This verifies worker-side payload
  compatibility; completion RPC delivery, durable retry/recovery, Rust transfers,
  and the production worker job loop remain separate required work.

### D11n: Rust completion RPC and lost acknowledgement recovery

- Added `ControlClient::complete_attempt` using the existing mTLS transport and
  five-second wire/local bounds. It validates the claimed digest before sending
  caller-owned evidence and never rewrites the identity or payload on retry.
- Validate decision/state combinations and bind accepted successful manifests to
  the request authority, outcome, and exact output references. Retain original
  manifest bytes with a 2 MiB bound; no metric float conversion or artifact I/O.
- Added Rust unit tests and a Rust executable probe driven by the real Go server
  and PostgreSQL. The fixture commits and discards the first acknowledgement for
  success, failure, and cancellation; exact retries recover one completion/event
  and one released reservation. Changed evidence conflicts, spoofed worker claims
  fail authentication binding, and unknown attempts are fenced.
- Initial tests failed for the missing client validation. Review then reproduced
  Serde accepting a positional array as a struct; added a manifest object check
  and a regression test. Four completion-client unit tests pass.
- `scripts/test-store.sh`, native `make test lint smoke`, and Linux worker tests
  passed. The first sandboxed native attempt could not bind fake-daemon Unix
  sockets; rerunning with socket access passed. After the object check, targeted
  PostgreSQL/mTLS tests, Linux worker tests, and `make lint smoke` passed again.
- Updated completion documentation. Durable completion journaling/restart recovery,
  Rust output transfers, and the production acquisition/execution/delivery loop
  remain required; this fixture verifies transport replay, not process recovery.

### D11o: Durable pending completion requests

- Added synchronous/asynchronous `persist_completion` and recovered request access.
  Complete caller-owned protobuf evidence is synced before returning, with full
  authority and payload/digest validation. Exact retries and assignment replay
  preserve the entry; changed UUIDs, evidence, or raw metrics conflict.
- Require matching observed exit presence/value on first persistence. Successful
  completion requires durable FINALIZING, zero exit, and no observed OOM. New phase
  progress/container binding stops once completion is pending; later cleanup may
  still persist exit observations without altering the original request.
- Added an optional attempt-record field using existing checksum, canonical decode,
  atomic replacement, poison, and storage-budget rules. New binaries read previous
  records; older binaries reject completion-bearing records instead of discarding
  unknown evidence. No dependency, wire protocol, or database schema changed.
- Initial tests failed for missing journal methods. Tests now cover concurrent
  persistence, reopen/replay, digest/authority/evidence conflicts, OOM, late cleanup,
  and tampered metrics with a recomputed frame checksum. The existing killed-owner
  subprocess test also recovers its exact pending completion and retains zero lease
  timing while choosing a new incarnation.
- Native `make test lint smoke` passed. `scripts/test-worker-linux.sh` passed on the
  isolated Linux volume, including 56 worker library tests and 15 journal tests;
  the ignored child fixture is exercised by its parent process-death test.
- Updated completion/journal documentation and corrected outdated coordinator
  descriptions. Persisting authoritative replies, safe cancellation handling after
  a rejected success request, cleanup/retention, and connecting recovered requests
  to the actual agent's delivery loop remain required. This slice stores pending
  evidence; it does not claim completion acceptance or restore execution authority.

### D11p: Durable completion acknowledgements

- Persist validated completion replies atomically beside their exact pending
  request. Recovered acknowledgements preserve original manifest bytes and are
  revalidated before use. Replies for changed/missing requests cannot resolve them.
- Repeated replies are idempotent; accepted/fenced/already-terminal outcomes cannot
  change. STOP_REQUESTED may advance only to a fenced/already-terminal rejection,
  never acceptance of the same payload after irreversible stop intent.
- Tests first failed for missing journal APIs. Native `make test lint smoke` and
  Linux worker tests passed after implementation. Journal tests cover reply binding,
  conflicting decisions, stop resolution, malformed successful replies, and original
  manifest formatting/large integers. The process-death fixture runs both pending
  and acknowledged cases and recovers the matching evidence after killing its owner.
- Updated completion/journal documentation. No network or runtime operation occurs
  under the journal lock. A persisted publication decision does not prove physical
  cleanup or release local capacity; delivery integration and retention remain open.

### D11q: Actual agent completion recovery after process death

- Added one-attempt `completion::deliver_pending`: read durable evidence, reuse an
  immutable saved decision when present, otherwise send the exact request and sync
  its validated acknowledgement before returning. Network I/O holds no journal lock.
- Connected recovery to the actual startup command after durable registration and
  predecessor fencing. A bounded attempt-ID queue and one completion RPC at a time
  run concurrently with health reporting/Docker cleanup through a cloned client.
  Retryable errors and stop responses requeue after one second; permanent errors
  retain evidence and stop the agent. Resolved events omit manifest/spec contents.
- The initial fixture build failed because delivery did not exist. The real
  PostgreSQL/mTLS test now withholds an already-committed reply and kills the Rust
  delivery process. The actual worker registers a new incarnation, retries an
  injected transient outage, recovers and persists the original accepted result.
  A second worker restart uses the persisted reply without another completion RPC.
- Added the complementary unaccepted case: startup fences the old attempt as LOST,
  persists ALREADY_TERMINAL, and produces no accepted manifest, completion row, or
  completion event. Both cases verify unchanged request evidence and no new storage
  reads. Runtime observations are fixture-seeded; these are completion recovery
  tests, not an acquired-workload execution/restart release gate.
- Native `make test lint smoke`, Linux worker tests, and the complete
  `scripts/test-store.sh` suite passed, including existing real Docker startup and
  Go/Rust RPC workflows. After adding the unaccepted fixture case, both recovery
  scenarios passed together and final `make lint smoke` passed.
- Updated README, operator guidance, completion, journal, and worker-agent docs.
  Live acquisition/execution still needs output verification and delivery wiring;
  safe cancellation supersession, local cleanup/retention, and remaining v0.1
  features/release gates are still required.

### D11r: Safe declared output collection after real container exit

- Added a bounded Rust collector that walks only validated declarations through
  directory descriptors. It rejects symlinks at every level, hard links, and
  non-regular files, and enforces required outputs and file/aggregate byte limits.
- Hashes fixed-size chunks, checks for changes during reads, and returns rewound
  open handles with names, sizes, and SHA-256. Path replacement cannot redirect a
  later read; open handles do not freeze contents, so upload checksum and immutable
  version verification remain necessary. No new dependencies were introduced.
- The integration assertion first failed with missing output evidence. Both real
  Docker execution/finalization scenarios now collect a workload-written `abc`
  file after confirmed exit and check its name, length, and known hash. The existing
  lost FINALIZING and uncertain RUNNING acknowledgement checks remain intact.
- Native `make test lint smoke`, Linux worker tests, and targeted PostgreSQL/mTLS/
  Docker `TestRustExecution` integration tests passed. Four filesystem tests cover
  empty files, missing outputs, exact limits, aggregate bytes, retained inode
  identity after path replacement, and unsafe path/file rejection.
- Added `docs/output-collection.md` and updated execution/README status. Collection
  currently uses the initial 64 MiB single-part limit; multipart, Rust transfer
  and durable upload evidence, live execution wiring, and remaining v0.1 release
  gates remain required. Collection alone neither accepts results nor frees capacity.

### D11s: Observe the intended job lock in the takeover race test

- The full upload verification run exposed an existing lease-test synchronization
  defect: observing any PostgreSQL lock wait could mistake contention on the
  database-wide scheduler advisory lock for a wait on the fixture's job row.
  Renewal could then legitimately reach the job before takeover, contradicting
  the test's assumed order. The isolated takeover test passed.
- Restricted the observation to the actual `SELECT ... FROM jobs ... FOR UPDATE`
  statement, matching the transaction ordering the test intends to prove. No
  production session, lease, or locking behavior changed.
- The complete store integration package passed after the correction, alongside
  admission, API, and worker RPC tests in the combined versioned-storage run.

### D11t: Rust single-part upload and exact-version registration

- Added validated Rust CreateUpload/FinalizeUpload methods with five-second local
  and wire deadlines, caller-owned stable request IDs, scoped object keys, bounded
  declarations/grants, and exact finalization reply binding.
- Added streaming HTTP PUT with independent TLS, explicit loopback-only development
  HTTP, no redirects/proxies/automatic retries, restricted signed headers, four
  shared permits, and an expiry-bounded 30-second queue/transfer budget. Fixed-size
  chunks recompute SHA-256 and verify length/EOF before releasing the final bytes.
  Empty files take an explicit verification path. Responses require exactly one
  non-null immutable version and expose no raw backend diagnostics/capabilities.
- Tests first failed because the component was absent, then caught missing explicit
  TLS crypto-provider initialization. Pinned reqwest 0.13.5 with default features
  disabled and selected the existing ring-backed rustls 0.23.40 dependency. Native
  and pinned Linux Rust 1.88 builds/tests pass with the updated lockfile.
- Six transfer tests cover bytes across multiple chunks, empty files, invalid
  grants, modified/truncated/grown files, redirects, response deadlines, and
  missing/null/duplicate version headers. The real PostgreSQL/mTLS/SeaweedFS suite
  now calls Rust grant/PUT/finalization, drops first committed replies, and verifies
  identical retries leave one upload and one exact-version artifact.
- `make test lint smoke`, Linux worker tests, and the complete combined object-store
  and PostgreSQL integration suite passed. The first full run exposed the unrelated
  lease-test observation defect corrected separately in D11s; the final run passed
  all store, admission, API, worker RPC, and server integration packages.
- Added `docs/worker-transfers.md`, refreshed artifact API docs and README. This
  component path is not yet connected to live acquisition/execution: durable upload
  intent/version recovery, authority-aware orchestration, multipart, and remaining
  v0.1 features and release gates are still required.

### D11u: Persist output upload declarations and exact-version evidence

- Added journal records for declared output uploads, stable grant scope, exact
  finalization requests, and matching artifact acknowledgements. Reopening retains
  request IDs and evidence; conflicting retries are rejected. Signed capabilities
  are omitted from disk, and journal/RPC paths share object validation.
- Added reopen and mutation-rejection tests, including asynchronous access and
  capability omission. `make test lint smoke` passed before committing this saved
  slice at the user's request. Updated `docs/worker-journal.md` with its contract.
- The broader build remains paused. Upload-specific process-death testing and
  live transfer/execution coordination remain unfinished.
