#!/bin/sh
set -eu
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
cd "$repo"
mkdir -p .local
workspace=$(mktemp -d "$repo/.local/runtime.XXXXXX")
export DISPATCH_TEST_WORKSPACE="$workspace"
export DISPATCH_TEST_JOB_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
export DISPATCH_TEST_ATTEMPT_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
cleanup() {
    # Only this run's unpredictable job label is eligible for cleanup. Preserve
    # unrelated user containers, images, volumes, and all other test runs.
    for id in $(docker ps -aq --filter "label=dev.dispatch.job=$DISPATCH_TEST_JOB_ID"); do
        docker rm -f "$id" >/dev/null
    done
    rm -rf "$workspace"
}
trap cleanup EXIT INT TERM
mkdir "$workspace/inputs" "$workspace/outputs" "$workspace/scratch"
chmod 700 "$workspace"
chmod 755 "$workspace/inputs"
chmod 777 "$workspace/outputs" "$workspace/scratch"
socket=$(docker context inspect --format '{{.Endpoints.docker.Host}}')
case "$socket" in
    unix:///*) export DISPATCH_TEST_DOCKER_SOCKET="${socket#unix://}" ;;
    *) echo 'Runtime tests require an explicit local Unix Docker socket' >&2; exit 1 ;;
esac
export DISPATCH_TEST_IMAGE='rust@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89'
docker image inspect "$DISPATCH_TEST_IMAGE" >/dev/null 2>&1 || docker pull "$DISPATCH_TEST_IMAGE"
cargo test -p dispatch-worker --test docker_runtime --locked -- --ignored --nocapture
