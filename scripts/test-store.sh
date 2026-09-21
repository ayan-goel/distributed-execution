#!/bin/sh
set -eu
container="dispatch-store-test-$$"
image='postgres:17.11@sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232'
docker run --rm -d --name "$container" -p 127.0.0.1::5432 -e POSTGRES_PASSWORD=dispatch_test -e POSTGRES_DB=dispatch_test "$image" > /dev/null
trap 'docker rm -f "$container" > /dev/null' EXIT INT TERM
tries=0
until docker exec "$container" pg_isready -h 127.0.0.1 -U postgres > /dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 40 ]; then docker logs "$container"; exit 1; fi
    sleep 0.25
done
port=$(docker port "$container" 5432/tcp | sed 's/.*://')
export DISPATCH_TEST_DATABASE_URL="postgres://postgres:dispatch_test@127.0.0.1:$port/dispatch_test?sslmode=disable"
make store-test
