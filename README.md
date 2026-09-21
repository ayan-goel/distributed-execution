# Dispatch

A distributed batch execution platform with a Go control plane and Rust workers.
The implementation is in progress. The binaries currently expose version commands;
they do not yet submit or execute jobs.

See [the specification](Dispatch_Project_Spec.md) for the product contract and
[the verification ledger](docs/implementation.md) for completed slices and release gates.

## Build

Install Go 1.27.1 and Rust 1.88.0, then run:

```sh
make build
make test lint smoke
```

On macOS with separately installed Command Line Tools, use
`DEVELOPER_DIR=/Library/Developer/CommandLineTools make test lint smoke` if the
selected Xcode installation is not configured. Runtime integration requires Linux;
Docker Desktop tests do not substitute for the independent-host release test.

Go/Cargo dependency versions are locked. The Go module path is local until a public
repository is chosen. No cloud account is required for development.

## Build references

- [Go toolchain selection](https://go.dev/doc/toolchain)
- [Cargo workspaces](https://doc.rust-lang.org/cargo/reference/workspaces.html)
