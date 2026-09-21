# Worker execution-spec validation

## Contract (D08a)

`ExecutionSpec::from_assignment` validates the control plane's execution document
before it becomes available to runtime code. Both acquisition and paginated recovery
use it; `GrantedAssignment::execution()` exposes validated settings through an
immutable reference. This type does not authorize execution by itself. Session and
lease checks, durable journaling, host capacity, and runtime reconciliation remain
separate requirements.

The worker hashes the exact received JSON bytes using SHA-256 and compares the
lowercase digest with `spec_sha256`. It does not reserialize in Rust: Go's field order
and HTML/Unicode escaping are part of those original bytes. The document is bounded
to 2 MiB before hashing or parsing, matching the assignment payload policy.

Typed decoding requires the supported API version and Job kind, exact field names,
named objects, and explicit required fields. It rejects unknown fields, nulls,
duplicate fields/map keys, non-string environment values, extra documents, and
nesting beyond 32 levels. Only fields omitted by Go's canonical representation
(optional arrays/maps) receive empty defaults. A shape preflight rejects positional
arrays accepted by Serde's struct decoder; typed decoding uses the original bytes
so duplicate keys cannot be hidden by that preflight.

Validation checks the [submission contract](contracts.md)'s names, map/argument
bounds, reserved `DISPATCH_` environment prefix, CPU/memory/scratch ceilings, finite
timeouts, termination grace, retry bounds/reasons, and disabled networking. Output
names must be unique and their paths must be clean, non-overlapping descendants of
`/outputs`, with bounded positive sizes. This is lexical validation; symlink-safe
collection and actual output-size enforcement still belong to the runtime/artifact
implementation. Nonempty input specifications or wire manifests fail closed until
dataset staging is implemented.

The pinned image, combined command/args, and all three resource quantities must
agree exactly with the duplicated protobuf fields. MiB-to-byte conversion happens
only after validating bounds. Workload strings remain argument/environment values;
the validator does not add a shell, expand variables, or interpret host paths.

After parsing, the acquisition client rechecks the local authority window so time
spent validating cannot turn an expired grant into a live one. Runtime operations
must still recheck authority when they act. Parser errors expose fixed categories
without copying document/environment values into diagnostics.

## Dependencies and verification

Serde 1.0.229 and serde_json 1.0.151 are pinned. The SHA-256 implementation reuses
ring 0.17.14, already present in the TLS dependency graph. Cargo.lock adds serde,
serde_json, and its zmij dependency without upgrading existing versions. Their
declared minimum Rust versions fit the pinned Rust 1.88.0 toolchain.

Tests cover exact-byte checksum mismatch, wire/spec disagreement, literal argument
and Unicode environment preservation, unsafe paths, output overlap in both orders,
unsupported networking/input mounts, invalid deadlines/retry policy, reserved
environment names, duplicate keys, wrong field case/types, nulls, extra documents,
and oversized payloads. Existing acquisition/recovery tests now carry valid hashed
documents. The real Go/PostgreSQL/mTLS fixture sends Go-canonicalized documents,
including HTML characters and a Unicode line separator; the delayed-grant fixture
uses a valid spec so expiry remains the reason execution authority is refused.

This slice validates execution settings. It does not implement Docker operations,
input/output transfer, the durable journal, or the production supervision loop.

## References

- [Serde container attributes](https://serde.rs/container-attrs.html)
- [Serde custom map deserialization](https://serde.rs/deserialize-map.html)
- [serde_json 1.0.151](https://docs.rs/serde_json/1.0.151/serde_json/)
- [ring SHA-256 digest API](https://docs.rs/ring/0.17.14/ring/digest/index.html)
