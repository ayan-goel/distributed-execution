#!/bin/sh
set -eu
container="dispatch-schema-test-$$"
image='postgres:17.11@sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232'
# This disposable container has no published port or persistent volume. Cleanup
# targets only the unique test container, never an existing development database.
docker run --rm -d --name "$container" -e POSTGRES_HOST_AUTH_METHOD=trust "$image" > /dev/null
trap 'docker rm -f "$container" > /dev/null' EXIT INT TERM
tries=0
# The image uses a temporary socket-only server during initialization. TCP
# readiness avoids racing that server's shutdown before the final server starts.
until docker exec "$container" pg_isready -h 127.0.0.1 -U postgres > /dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 40 ]; then docker logs "$container"; exit 1; fi
    sleep 0.25
done
for migration in migrations/*.up.sql; do
    docker exec -i "$container" psql -X -v ON_ERROR_STOP=1 -U postgres --single-transaction < "$migration"
done
docker exec -i "$container" psql -X -v ON_ERROR_STOP=1 -U postgres < tests/integration/schema.sql
for migration in $(find migrations -name '*.down.sql' | sort -r); do
    docker exec -i "$container" psql -X -v ON_ERROR_STOP=1 -U postgres --single-transaction < "$migration"
done
test "$(docker exec "$container" psql -X -At -U postgres -c "SELECT count(*) FROM pg_tables WHERE schemaname='public'")" = 0
for migration in migrations/*.up.sql; do
    docker exec -i "$container" psql -X -v ON_ERROR_STOP=1 -U postgres --single-transaction < "$migration"
done
docker exec -i "$container" psql -X -v ON_ERROR_STOP=1 -U postgres < tests/integration/schema.sql
echo 'Fresh migration, constraints, rollback, and reapply passed on real PostgreSQL'
