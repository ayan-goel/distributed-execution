#!/bin/sh
set -eu
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
${GO:-go} run ./internal/protocol/cmd/fixture create > "$tmp/go.bin"
cargo run --quiet --locked -p dispatch-protocol --example roundtrip < "$tmp/go.bin" > "$tmp/rust.bin"
${GO:-go} run ./internal/protocol/cmd/fixture check < "$tmp/rust.bin"
echo 'Go -> Rust -> Go assignment round-trip passed'
