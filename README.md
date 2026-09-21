# Dispatch

A distributed batch execution platform with a Go control plane and Rust workers.
The implementation is in progress. The server supports project/token provisioning,
authenticated HTTP/CLI submission, and job inspection. The CLI can also validate
job files offline. The worker command registers, reconciles old containers, and
reports health; its acquisition loop is still pending. The execution component is
integration-tested through Docker exit and FINALIZING, with artifact publication
and terminal completion still to implement.

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
See [versioned object storage](docs/object-storage.md) for the isolated local backend
and transfer compatibility gate, which require no AWS account.
See [running the control plane](docs/running.md) for the current operator commands.

## Build references

- [Go toolchain selection](https://go.dev/doc/toolchain)
- [Cargo workspaces](https://doc.rust-lang.org/cargo/reference/workspaces.html)
