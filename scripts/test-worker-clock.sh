#!/bin/sh
set -eu
if [ "$(uname -s)" = Linux ]; then
    cargo test -p dispatch-worker --lib --locked lease::
    exit
fi

# macOS protocol tests use a development clock. Compile the exact same module
# inside Linux as well so its production syscall path is actually exercised.
libc_source=
for manifest in "${CARGO_HOME:-$HOME/.cargo}"/registry/src/*/libc-0.2.189/Cargo.toml; do
    if [ -f "$manifest" ]; then libc_source=${manifest%/Cargo.toml}; break; fi
done
test -n "$libc_source" || { echo 'Build the worker first to cache pinned libc'; exit 1; }
clock_dir="$PWD/.local/linux-clock"
mkdir -p "$clock_dir"
cat > "$clock_dir/Cargo.toml" <<'EOF'
[package]
name = "dispatch-clock-check"
version = "0.0.0"
edition = "2021"
[workspace]
[lib]
path = "/src/lease.rs"
[dependencies]
libc = { path = "/libc" }
EOF
docker run --rm --network none --read-only --tmpfs /tmp \
    -v "$PWD/rust/worker/src/lease.rs:/src/lease.rs:ro" \
    -v "$libc_source:/libc:ro" -v "$clock_dir:/check" -w /check \
    -e CARGO_HOME=/check/cargo -e CARGO_TARGET_DIR=/check/target \
    rust:1.88.0-slim-bookworm@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 \
    cargo test --offline
