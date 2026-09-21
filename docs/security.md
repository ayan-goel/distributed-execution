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
RPC body is not authentication. The mTLS listener, certificate verification,
provisioning commands, session registration, and enforcement at worker RPCs remain
the next required slices.

Credential revocation and its audit event commit together. Repeated revocation adds
no extra event; authentication checks the database without caching. Revocation
does not claim to stop a container or release its reservation. Lease expiry/fencing
and physical cleanup must handle that independently.

Real PostgreSQL tests passed for 16 concurrent duplicate enrollments with one host,
identity lookup, unknown/revoked credential rejection, cross-host revocation denial,
project scoping, missing-project rollback, and injected audit failures that roll
back both provisioning and revocation. Unit tests cover platform/label/resource
bounds. All migrations pass fresh apply, rollback, and reapply.

## Still required before release

- Enforce these permissions at every HTTP endpoint, including artifact and event reads.
- TLS except explicitly configured loopback development; worker mTLS identities.
- API limits, sanitized errors/logs, scoped object grants, approved registry access.
- Container privileges/network/filesystem restrictions and runtime verification.
- Retention/audit coverage for submission, cancellation, provisioning, and operator actions.
- Dependency vulnerability audit and tested deployment instructions.
