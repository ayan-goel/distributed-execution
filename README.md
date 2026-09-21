# Dispatch

A distributed batch execution platform with a Go control plane and Rust workers.
The implementation is in progress. The binaries currently expose version commands;
they do not yet submit or execute jobs.

See [the specification](Dispatch_Project_Spec.md) for the product contract and
[the verification ledger](docs/implementation.md) for completed slices and release gates.

## Build

Install Go 1.27.1, Rust 1.88.0, and protoc 34.1, then run:

```sh
make build
make test lint smoke
make integration
make generate-check
```

On macOS with separately installed Command Line Tools, use
`DEVELOPER_DIR=/Library/Developer/CommandLineTools make test lint smoke` if the
selected Xcode installation is not configured. Runtime integration requires Linux;
Docker Desktop tests do not substitute for the independent-host release test.

Go/Cargo dependency versions are locked. The Go module path is local until a public
repository is chosen. No cloud account is required for development.

Implementation details and verification records live in `docs/`:
[contracts](docs/contracts.md), [database](docs/database.md),
[worker protocol](docs/protocol.md), and [slice ledger](docs/implementation.md).

## Build references

- [Go toolchain selection](https://go.dev/doc/toolchain)
- [Cargo workspaces](https://doc.rust-lang.org/cargo/reference/workspaces.html)
