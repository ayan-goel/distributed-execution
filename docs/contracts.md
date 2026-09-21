# Submission contracts

## Job parsing (D02a)

`internal/spec` accepts one JSON or YAML document of at most 1 MiB. The example
in `schema/examples/job.yaml` matches spec §4; its registry and dataset are placeholders.
The same typed representation is used by future HTTP admission and the CLI.

Unknown fields (including wrong capitalization), duplicate keys, nulls, YAML aliases,
non-string environment values, multiple documents, and nesting over 32 levels are
rejected. No implicit shell is added. The parser reserves `DISPATCH_` environment
names for execution identity. Project authorization and quota checks are admission
responsibilities; structural validation is not authorization.

Names/project/dataset identifiers have 1–128 ASCII letters, digits, dots, underscores,
or hyphens, beginning with a letter or digit. Environment names use shell identifier
syntax. Maps have at most 128 entries; values and individual arguments are at most
8192 bytes. Commands contain a nonempty executable and at most 256 combined arguments.

### Resource and deadline ceilings

These are defensive platform ceilings, not promised machine capacity. Actual
project quotas and eligible worker capacity can be much smaller.

| Field | Accepted range |
| --- | --- |
| CPU millis | 1–1,024,000 |
| Memory MiB | 1–16,777,216 |
| Scratch MiB | 1–1,073,741,824 |
| Startup/finalization | 1–3600 seconds each |
| Execution | 1–604800 seconds |
| Termination grace | 0–300 seconds |
| Attempts including initial | 1–10 |
| Initial/maximum backoff | 1–3600 seconds; maximum ≥ initial |
| Inputs/outputs | at most 64 each |
| Per-output maximum size | 1 byte–1 TiB |

Input paths must be clean descendants of `/inputs`; outputs must be clean descendants
of `/outputs`. Overlapping paths and duplicate output names fail validation. These
lexical checks complement, but do not replace, runtime symlink-safe file collection.
Only `disabled` workload networking is currently admitted. Omission defaults to
disabled. Other resource/time/retry fields are explicit rather than guessed.

### Canonical identity

Validated jobs serialize to compact JSON with sorted map keys. Retry reasons are
treated as a set; their canonical order is sorted. Argument and input/output order
remain unchanged. SHA-256 of these bytes is the normalized request identity.
Admission must separately resolve image tags and datasets to immutable versions
before storing the execution specification and its final hash. A request hash alone
does not prove that an image or dataset was pinned.

The initial supported retry reasons are `WORKER_LOST`, `RUNTIME_UNAVAILABLE`, and
`TRANSFER_FAILED`. Application exit-code retry extensions are not yet admitted.

### Implementation references

- [Official YAML parser](https://pkg.go.dev/go.yaml.in/yaml/v3): parse into a bounded
  node tree, reject ambiguous syntax, then convert to strict typed JSON. This avoids
  YAML's automatic scalar-to-string coercion for environment parameters.
- [Go JSON encoding](https://pkg.go.dev/encoding/json#Marshal): map key ordering is
  deterministic. Exact field-name checks supplement the decoder's case folding.

## Sweep expansion (D02b)

The server contract embeds `spec.jobTemplate` as a complete Job, never a local path.
The CLI will resolve the spec's `jobTemplateFile` form on the client before sending
this request. Sweep/template projects must agree. Matrix keys override environment
values; sorted parameter names and original value order determine child indices.
Each child owns its maps and slices, so modifying one cannot change another.

Expansion accepts 1–32 dimensions, at most 1000 children, at most 16 MiB of expanded
canonical specifications, and a concurrency cap of 1–1000. Count multiplication is
checked before allocation. Empty dimensions and reserved environment names fail.
Repeated values remain separate children. `cancelRunningOnFailure` requires
`failFast`. This slice validates policy; execution-time enforcement belongs to D17.
