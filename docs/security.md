# Security boundaries and implemented evidence

This document distinguishes implemented protections from remaining release work.
Dispatch v0.1 runs trusted workloads on dedicated hosts. Container controls do not
establish a hostile multi-tenant sandbox.

## Project tokens (D05b)

Operator provisioning generates 32 cryptographically random bytes and returns a
`dsp_`-prefixed URL-safe bearer token once. PostgreSQL stores only its SHA-256 digest.
This hashing choice relies on 256-bit random input; it is not a password-storage
scheme. Neither raw tokens nor hashes belong in logs, errors, events, or manifests.

Every token belongs to exactly one project. Roles are cumulative:

| Role | Permissions |
| --- | --- |
| read | Inspect that project's jobs, logs, artifacts, events |
| submit | Read plus submit/cancel in that project |
| operator | Submit plus operator actions within the authorized project |

Unknown roles fail closed. Authentication queries PostgreSQL on every request and
rejects revoked tokens and disabled projects without revealing which condition
failed. There is no revocation cache. Tokens do not confer access to another project.

Issuance and revocation write audit events in the same transaction as the token
change. Revocation is idempotent and project-scoped. Database failures return no new
raw token. The provisioning CLI is an operator tool with database access;
there is no unauthenticated token-creation HTTP endpoint.

## Verified so far

Real PostgreSQL tests cover all three role levels, malformed/unknown tokens, hash
storage, cross-project revocation denial, repeated revocation, immediate rejection
after revocation, project disabling, unknown-role denial, and one audit event per
issuance/revocation. Full migration rollback/reapply includes the token tables.

## Worker authorization foundation (D06a)

Worker credentials are separate from project bearer tokens. The store now supports
atomic provisioning of a host, its approved resource ceilings, Linux architecture
and placement labels, explicit project memberships, and a SHA-256 fingerprint of
its public leaf certificate. No private key enters PostgreSQL. The fingerprint is
globally unique even after revocation, so concurrent enrollment cannot attach the
same credential to two hosts. Multiple credentials can belong to one host for
future rotation; adding/rotating them is not exposed yet.

Provisioning requires 1–64 distinct enabled projects and bounded capacities/labels.
It creates a `REGISTERING` worker without a session. Neither a certificate lookup
nor provisioning grants execution authority. The registration handshake must check
claimed resources against these stored ceilings, establish a session, and reconcile
old executions before making capacity eligible. Scheduling must filter through
`worker_projects`; workers cannot grant themselves project membership.

`AuthenticateWorker` is an internal lookup for an **already verified mTLS leaf
certificate**, not a public fingerprint-based login. A fingerprint supplied in an
RPC body is not authentication. The mTLS transport boundary below now verifies
this distinction. Session registration/recovery and heartbeat state transitions
are implemented as described in [worker sessions](worker-sessions.md). Provisioning
commands and executable listener wiring remain required.

Credential revocation and its audit event commit together. Repeated revocation adds
no extra event; authentication checks the database without caching. Revocation
does not claim to stop a container or release its reservation. Lease expiry/fencing
and physical cleanup must handle that independently.

Real PostgreSQL tests passed for 16 concurrent duplicate enrollments with one host,
identity lookup, unknown/revoked credential rejection, cross-host revocation denial,
project scoping, missing-project rollback, and injected audit failures that roll
back both provisioning and revocation. Unit tests cover platform/label/resource
bounds. All migrations pass fresh apply, rollback, and reapply.

## Worker mTLS transport boundary (D06b)

`internal/workerapi.NewServer` constructs a gRPC server requiring TLS 1.3 and a
client certificate verified against explicit operator-provided client CA roots.
After TLS verifies the chain and client-authentication purpose, the interceptor
looks up the public leaf's SHA-256 fingerprint in PostgreSQL. It does not trust
certificate subjects, request fields, or metadata to identify the installed host.

Every unary worker method binds its worker ID to that identity. Heartbeat inventory
may report old sessions only on the same host; renewal batches must also match the
enclosing session. Registration and heartbeat now enforce durable session state in
their transactions. Attempt mutation methods remain unimplemented; a valid host
credential does not itself authorize an attempt mutation.

The interceptor rechecks certificate-chain validity dates and database revocation
on every RPC, including an existing connection. Calls have a five-second deadline,
256 global in-flight slots, 4-MiB request/response limits, and 16-KiB header limits.
Connections allow at most 64 simultaneous streams and have a five-second handshake
timeout. This constructor has no plaintext mode.

Tests use a real gRPC/TLS listener and PostgreSQL with a test-only identity-echo
service. They prove valid identity propagation, host impersonation rejection,
unknown certificate rejection, live revocation on the same connection, oversized
message rejection, and TLS refusal of missing, untrusted, and server-only client
certificates. Invalid-certificate fingerprints are deliberately enrolled in the
database so lookup rejection cannot hide a missing TLS check. Unit tests cover
certificate expiry after handshake and nested inventory/renewal identity binding.
This test verifies transport authentication. A separate service integration test
now verifies actual registration, heartbeat readiness, server restart replay, and
approved takeover through the same mTLS boundary. Neither test proves execution.

References: [Go TLS configuration](https://pkg.go.dev/crypto/tls#Config) and
[gRPC TLS peer information](https://pkg.go.dev/google.golang.org/grpc/credentials#TLSInfo).

## Still required before release

- Enforce these permissions at every HTTP endpoint, including artifact and event reads.
- TLS except explicitly configured loopback development; worker mTLS identities.
- API limits, sanitized errors/logs, scoped object grants, approved registry access.
- Container privileges/network/filesystem restrictions and runtime verification.
- Retention/audit coverage for submission, cancellation, provisioning, and operator actions.
- Dependency vulnerability audit and tested deployment instructions.
