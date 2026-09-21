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
raw token. The eventual provisioning CLI is an operator tool with database access;
there is no unauthenticated token-creation HTTP endpoint.

## Verified so far

Real PostgreSQL tests cover all three role levels, malformed/unknown tokens, hash
storage, cross-project revocation denial, repeated revocation, immediate rejection
after revocation, project disabling, unknown-role denial, and one audit event per
issuance/revocation. Full migration rollback/reapply includes the token tables.

## Still required before release

- Enforce these permissions at every HTTP endpoint, including artifact and event reads.
- TLS except explicitly configured loopback development; worker mTLS identities.
- API limits, sanitized errors/logs, scoped object grants, approved registry access.
- Container privileges/network/filesystem restrictions and runtime verification.
- Retention/audit coverage for submission, cancellation, provisioning, and operator actions.
- Dependency vulnerability audit and tested deployment instructions.
