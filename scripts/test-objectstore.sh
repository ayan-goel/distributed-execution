#!/bin/sh
set -eu
container="dispatch-objectstore-test-$$"
image='chrislusf/seaweedfs:4.47@sha256:ce9e796f1fe6f06968f4c04bdaf8f678dad9c8acdfef3d244133d71bfa6bf882'
export DISPATCH_TEST_S3_ACCESS_KEY="dispatch-$(python3 -c 'import secrets; print(secrets.token_hex(12))')"
export DISPATCH_TEST_S3_SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
docker run --rm -d --name "$container" --memory 1g --cpus 1 --pids-limit 256 \
    -p 127.0.0.1::8333 --tmpfs /data:rw,size=512m \
    -e AWS_ACCESS_KEY_ID="$DISPATCH_TEST_S3_ACCESS_KEY" \
    -e AWS_SECRET_ACCESS_KEY="$DISPATCH_TEST_S3_SECRET_KEY" \
    -e S3_BUCKET=dispatch-test "$image" mini -dir=/data -admin.ui=false \
    -webdav=false -master.volumeSizeLimitMB=64 -volume.max=2 > /dev/null
# This disposable name is the only cleanup target; no shared bucket or user
# container is inspected or deleted. All object versions live in its tmpfs.
trap 'docker rm -f "$container" > /dev/null' EXIT INT TERM
port=$(docker port "$container" 8333/tcp | sed 's/.*://')
export DISPATCH_TEST_S3_ENDPOINT="http://127.0.0.1:$port"
make objectstore-test
