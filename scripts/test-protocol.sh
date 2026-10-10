#!/bin/sh
set -eu
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
${GO:-go} run ./internal/protocol/cmd/fixture create > "$tmp/go.bin"
cargo run --quiet --locked -p dispatch-protocol --example roundtrip < "$tmp/go.bin" > "$tmp/rust.bin"
${GO:-go} run ./internal/protocol/cmd/fixture check < "$tmp/rust.bin"
echo 'Go -> Rust -> Go assignment round-trip passed'
${GO:-go} run ./internal/protocol/cmd/fixture create-page > "$tmp/go-page.bin"
cargo run --quiet --locked -p dispatch-protocol --example roundtrip -- page < "$tmp/go-page.bin" > "$tmp/rust-page.bin"
${GO:-go} run ./internal/protocol/cmd/fixture check-page < "$tmp/rust-page.bin"
echo 'Go -> Rust -> Go assignment page round-trip passed'
for kind in upload-request upload-response part-request part-response finalize; do
  ${GO:-go} run ./internal/protocol/cmd/fixture "create-$kind" > "$tmp/go-multipart.bin"
  cargo run --quiet --locked -p dispatch-protocol --example roundtrip -- "$kind" < "$tmp/go-multipart.bin" > "$tmp/rust-multipart.bin"
  ${GO:-go} run ./internal/protocol/cmd/fixture "check-$kind" < "$tmp/rust-multipart.bin"
done
echo 'Go -> Rust -> Go multipart message round-trips passed'
${GO:-go} run ./internal/protocol/cmd/completion-fixture create > "$tmp/completions.bin"
cargo run --quiet --locked -p dispatch-worker --example completion_digest < "$tmp/completions.bin" > "$tmp/digests.txt"
${GO:-go} run ./internal/protocol/cmd/completion-fixture check < "$tmp/digests.txt"
echo 'Go/Rust completion digest and rejection parity passed'
