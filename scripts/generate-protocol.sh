#!/bin/sh
set -eu
test "$(protoc --version)" = 'libprotoc 34.1' || { echo 'Install protoc 34.1 for reproducible bindings' >&2; exit 1; }
export PATH="$PWD/.tools/bin:$PATH"
test "$(protoc-gen-go --version)" = 'protoc-gen-go v1.36.11'
test "$(protoc-gen-go-grpc --version)" = 'protoc-gen-go-grpc 1.6.2'
mkdir -p gen
protoc -I proto --go_out=gen --go_opt=paths=source_relative --go-grpc_out=gen --go-grpc_opt=paths=source_relative proto/dispatch/worker/v1/worker.proto
cargo build --locked -p dispatch-protocol
