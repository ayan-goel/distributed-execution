#!/bin/sh
set -eu
if [ "$(uname -s)" = Linux ]; then
    cargo test -p dispatch-worker --locked
    exit
fi

# Match protoc to the Docker host architecture. Checksums come from the official
# protobuf v34.1 release metadata; native binaries cannot run in the Linux fixture.
case $(docker version --format '{{.Server.Arch}}') in
    arm64) platform=aarch_64; target=aarch64-unknown-linux-gnu; checksum=31c5e9e3c7bf013cf41fb97765ee255c140024a6b175b6cc9b64beddd7c23ba7 ;;
    amd64) platform=x86_64; target=x86_64-unknown-linux-gnu; checksum=af27ea66cd26938fe48587804ca7d4817457a08350021a1c6e23a27ccc8c6904 ;;
    *) echo 'Unsupported Docker architecture for Linux worker verification'; exit 1 ;;
esac
tool_dir="$PWD/.tools/protoc-linux-34.1-$platform"
archive="$tool_dir/protoc.zip"
mkdir -p "$tool_dir"
if ! printf '%s  %s\n' "$checksum" "$archive" | shasum -a 256 -c --status 2>/dev/null; then
    curl --fail --location --retry 3 --silent --show-error \
        "https://github.com/protocolbuffers/protobuf/releases/download/v34.1/protoc-34.1-linux-$platform.zip" -o "$archive.pending"
    printf '%s  %s\n' "$checksum" "$archive.pending" | shasum -a 256 -c
    mv "$archive.pending" "$archive"
fi
unzip -q -o "$archive" -d "$tool_dir"
cache="$PWD/.local/linux-worker"
registry="${CARGO_HOME:-$HOME/.cargo}/registry"
mkdir -p "$cache/cargo" "$cache/target"
test -d "$registry" || { echo 'Build the worker natively first to cache pinned crates'; exit 1; }
# Native builds omit target-specific crates; fetch their locked versions before
# entering the offline container so macOS and Linux exercise the same lockfile.
cargo fetch --locked --target "$target"

# Source and cached crates are read-only and the compiler has no network. Tests
# use an anonymous Linux volume (removed by --rm), not a macOS bind mount or tmpfs.
docker run --rm --network none --read-only --tmpfs /tmp:exec \
    --mount type=volume,destination=/journal-test \
    -v "$PWD:/src:ro" -v "$registry:/cargo/registry:ro" \
    -v "$cache/cargo:/cargo" -v "$cache/target:/target" \
    -v "$tool_dir:/protoc:ro" -w /src \
    -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR=/target -e TMPDIR=/journal-test \
    -e RUSTUP_TOOLCHAIN=1.88.0 \
    -e PROTOC=/protoc/bin/protoc -e PROTOC_INCLUDE=/protoc/include \
    rust:1.88.0-slim-bookworm@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89 \
    cargo test --offline --locked -p dispatch-worker
