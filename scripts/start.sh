#!/usr/bin/env bash
# start.sh — build JARs and bring up the full Docker Compose stack
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Building JARs..."
./gradlew :flink-job:shadowJar :generators:shadowJar --no-daemon -q
echo "    flink-job/build/libs/rt-index-pipeline.jar"
echo "    generators/build/libs/generators.jar"

echo "==> Starting Docker Compose services..."
docker-compose up -d kafka postgres elasticsearch prometheus grafana

wait_healthy() {
  local service=$1
  local max_wait=${2:-120}
  local elapsed=0
  echo -n "    Waiting for $service..."
  while [[ "$(docker-compose ps --format json "$service" 2>/dev/null \
              | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Health',''))" 2>/dev/null)" != "healthy" ]]; do
    sleep 3
    elapsed=$((elapsed + 3))
    echo -n "."
    if [[ $elapsed -ge $max_wait ]]; then
      echo " TIMEOUT"
      docker-compose logs --tail=20 "$service"
      exit 1
    fi
  done
  echo " OK"
}

wait_healthy kafka    120
wait_healthy postgres  60
wait_healthy elasticsearch 180

echo "==> Creating Elasticsearch index..."
docker-compose up --no-deps es-setup || true

echo "==> Starting Flink JobManager / TaskManager + Generator..."
docker-compose up -d flink-jobmanager flink-taskmanager generator

wait_healthy flink-jobmanager 120

echo ""
echo "==> Stack is up. Access:"
echo "    Flink UI    →  http://localhost:8081"
echo "    Grafana     →  http://localhost:3000"
echo "    Prometheus  →  http://localhost:9090"
echo "    ES          →  http://localhost:9200"
echo ""
echo "    Check status:  ./scripts/check-health.sh"
echo "    Stop all:      ./scripts/stop.sh"
