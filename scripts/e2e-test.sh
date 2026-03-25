#!/usr/bin/env bash
# e2e-test.sh — full Docker Compose stack validation
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PASS=0
FAIL=0

pass() { echo "  [PASS] $1"; PASS=$((PASS + 1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL + 1)); }

echo "==> Building JARs..."
./gradlew :flink-job:shadowJar :generators:shadowJar --no-daemon -q
echo "    Built flink-job and generators JARs"

echo "==> Starting Docker Compose stack..."
docker-compose up -d

wait_healthy() {
  local service=$1
  local max_wait=${2:-180}
  local elapsed=0
  echo -n "    Waiting for $service to be healthy..."
  while [[ "$(docker-compose ps --format json "$service" 2>/dev/null \
              | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Health',''))" 2>/dev/null)" != "healthy" ]]; do
    sleep 5
    elapsed=$((elapsed + 5))
    echo -n "."
    if [[ $elapsed -ge $max_wait ]]; then
      echo " TIMEOUT"
      docker-compose logs --tail=30 "$service"
      docker-compose down
      exit 1
    fi
  done
  echo " OK"
}

wait_healthy kafka           120
wait_healthy postgres         60
wait_healthy elasticsearch   180
wait_healthy flink-jobmanager 180

echo "==> Waiting 60s for generators to populate data..."
sleep 60

echo "==> Checking Elasticsearch..."
ES_COUNT=$(curl -sf "http://localhost:9200/products/_count" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count', 0))" 2>/dev/null || echo "0")
echo "    Document count: $ES_COUNT"
if [[ "$ES_COUNT" -gt 0 ]]; then
  pass "ES has $ES_COUNT documents indexed"
else
  fail "ES has no documents (expected > 0)"
fi

echo "==> Sampling pipeline latency..."
LATENCY_OK=true
for PRODUCT_ID in $(curl -sf "http://localhost:9200/products/_search?size=5" \
    | python3 -c "
import sys,json
data = json.load(sys.stdin)
for hit in data['hits']['hits']:
    print(hit['_id'])
" 2>/dev/null || true); do
  INDEXED_AT=$(curl -sf "http://localhost:9200/products/_doc/$PRODUCT_ID" \
    | python3 -c "
import sys,json
from datetime import datetime, timezone
src = json.load(sys.stdin).get('_source', {})
indexed_at = src.get('indexedAt')
last_product = src.get('lastProductUpdate')
last_price = src.get('lastPriceUpdate')
last_stock = src.get('lastStockUpdate')
timestamps = [t for t in [last_product, last_price, last_stock] if t]
if not timestamps or not indexed_at:
    print('N/A')
else:
    event_time = min(timestamps)
    try:
        t_indexed = datetime.fromisoformat(indexed_at.replace('Z','+00:00'))
        t_event   = datetime.fromisoformat(event_time.replace('Z','+00:00'))
        latency_ms = int((t_indexed - t_event).total_seconds() * 1000)
        print(latency_ms)
    except Exception:
        print('N/A')
" 2>/dev/null || echo "N/A")
  if [[ "$INDEXED_AT" == "N/A" ]]; then
    echo "    $PRODUCT_ID: no timestamp data"
  else
    echo "    $PRODUCT_ID: latency=${INDEXED_AT}ms"
    if [[ "$INDEXED_AT" -gt 30000 ]]; then
      LATENCY_OK=false
    fi
  fi
done
if [[ "$LATENCY_OK" == "true" ]]; then
  pass "Pipeline latency within acceptable range"
else
  fail "Some documents exceeded 30s latency"
fi

echo "==> Checking Prometheus metrics..."
METRIC_QUERY="flink_taskmanager_job_task_operator_events_processed_products_total"
PROM_RESULT=$(curl -sf "http://localhost:9090/api/v1/query?query=${METRIC_QUERY}" \
  | python3 -c "
import sys,json
data = json.load(sys.stdin)
results = data.get('data', {}).get('result', [])
print(len(results))
" 2>/dev/null || echo "0")

if [[ "$PROM_RESULT" -gt 0 ]]; then
  pass "Prometheus has Flink metrics ($PROM_RESULT series)"
else
  fail "Prometheus has no Flink custom metrics (is the Prometheus reporter configured?)"
fi

DEDUP_QUERY="flink_taskmanager_job_task_operator_dedup_filtered_total"
DEDUP_RESULT=$(curl -sf "http://localhost:9090/api/v1/query?query=${DEDUP_QUERY}" \
  | python3 -c "
import sys,json
data = json.load(sys.stdin)
results = data.get('data', {}).get('result', [])
print(len(results))
" 2>/dev/null || echo "0")

if [[ "$DEDUP_RESULT" -gt 0 ]]; then
  pass "Prometheus has dedup_filtered metric"
else
  fail "Prometheus missing dedup_filtered metric"
fi

echo "==> Checking Grafana..."
GRAFANA_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" \
  "http://localhost:3000/api/dashboards/uid/rt-index-pipeline" 2>/dev/null || echo "0")
if [[ "$GRAFANA_STATUS" == "200" ]]; then
  pass "Grafana dashboard rt-index-pipeline loads (HTTP 200)"
else
  fail "Grafana dashboard returned HTTP $GRAFANA_STATUS"
fi

echo ""
echo "═══════════════════════════════════════"
echo "  E2E Test Summary"
echo "  PASSED: $PASS  FAILED: $FAIL"
echo "═══════════════════════════════════════"
echo "  Flink UI    →  http://localhost:8081"
echo "  Grafana     →  http://localhost:3000"
echo "  Prometheus  →  http://localhost:9090"
echo "  ES count    →  http://localhost:9200/products/_count"
echo "═══════════════════════════════════════"

echo ""
echo "==> Tearing down stack..."
docker-compose down -v

if [[ "$FAIL" -gt 0 ]]; then
  exit 1
fi
