#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
unset DOCKER_HOST DOCKER_CONTEXT DOCKER_TLS DOCKER_TLS_VERIFY DOCKER_CERT_PATH
# Fixed names are also used by ElectricIntegrationTest. Refuse to disturb existing containers.
for name in electric4k-test-pg electric4k-test-sync; do
  if docker --host=unix:///var/run/docker.sock container inspect "$name" >/dev/null 2>&1; then
    echo "Container $name already exists; stop your previous fixture first." >&2
    exit 1
  fi
done
docker --host=unix:///var/run/docker.sock network create electric4k-test-net >/dev/null
pg_started=false
sync_started=false
cleanup() {
  if "$sync_started"; then docker --host=unix:///var/run/docker.sock rm -f electric4k-test-sync >/dev/null; fi
  if "$pg_started"; then docker --host=unix:///var/run/docker.sock rm -f electric4k-test-pg >/dev/null; fi
  docker --host=unix:///var/run/docker.sock network rm electric4k-test-net >/dev/null
}
trap cleanup EXIT
# This password is only for the disposable local fixture and is never published to a host port.
docker --host=unix:///var/run/docker.sock run -d --name electric4k-test-pg --network electric4k-test-net \
  -e POSTGRES_PASSWORD=electric4k-test-only -e POSTGRES_DB=electric4k \
  postgres:17 -c wal_level=logical >/dev/null
pg_started=true
ready=false
for attempt in {1..30}; do
  if docker --host=unix:///var/run/docker.sock exec electric4k-test-pg pg_isready -U postgres -d electric4k >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
"$ready" || { echo 'PostgreSQL did not become ready' >&2; exit 1; }
docker --host=unix:///var/run/docker.sock run -d --name electric4k-test-sync --network electric4k-test-net \
  -p 127.0.0.1:33001:3000 \
  -e DATABASE_URL=postgresql://postgres:electric4k-test-only@electric4k-test-pg:5432/electric4k \
  -e ELECTRIC_INSECURE=true "${ELECTRIC_IMAGE:-electricsql/electric@sha256:efb6fa43859d67cb8c73439e0c8bc0f7a3daa467500fb06f2a924bcb2070c139}" >/dev/null
sync_started=true
ready=false
for attempt in {1..45}; do
  if curl --noproxy 127.0.0.1 -fsS http://127.0.0.1:33001/v1/health >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
"$ready" || { echo 'Electric did not become ready' >&2; exit 1; }
./gradlew --no-daemon --max-workers=2 integrationTest "$@"
