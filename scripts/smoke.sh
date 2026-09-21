#!/bin/sh
set -eu
make build
test "$(bin/dispatch --version)" = 'dispatch 0.1.0-dev'
test "$(bin/dispatch-server --version)" = 'dispatch-server 0.1.0-dev'
test "$(target/debug/dispatch-worker --version)" = 'dispatch-worker 0.1.0-dev'
for executable in bin/dispatch bin/dispatch-server target/debug/dispatch-worker; do
    if "$executable" --invalid-option > /dev/null 2>&1; then
        echo "$executable accepted an invalid option" >&2
        exit 1
    fi
done
echo 'All three binary smoke checks passed'
