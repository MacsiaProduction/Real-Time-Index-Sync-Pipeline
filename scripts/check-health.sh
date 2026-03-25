#!/usr/bin/env bash
# check-health.sh — verify all services are up and report pipeline stats
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}[OK]${NC}   $1"; }
warn() { echo -e "  ${YELLOW}[WARN]${NC} $1"; }
fail() { echo -e "  ${RED}[FAIL]${NC} $1"; }

check_http() {
  local name=$1
  local url=$2
  local expected=${3:-200}
  local code
  code=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 3 "$url" 2>/dev/null || echo "000")
  if [[ "$code" == "$expected" ]]; then
    ok "$name ($url)"
  else
    fail "$name ($url) — HTTP $code"
  fi
}

echo ""
echo "==> Infrastructure health"
check_http "Kafka broker"          "http://localhost:9092"           000   # TCP only; 000 = port reachable
# Use direct TCP check for Kafka
nc -z localhost 9092 2>/dev/null && ok "Kafka broker (localhost:9092)" || fail "Kafka broker not reachable"
check_http "Elasticsearch"         "http://localhost:9200/_cluster/health"
check_http "Flink JobManager UI"   "http://localhost:8081/overview"
check_http "Prometheus"            "http://localhost:9090/-/ready"
check_http "Grafana"               "http://localhost:3000/api/health"

echo ""
echo "==> Flink jobs"
FLINK_JOBS=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null)
if echo "$FLINK_JOBS" | python3 -c "
import sys, json
data = json.load(sys.stdin)
jobs = data.get('jobs', [])
for j in jobs:
    status = j.get('state', 'UNKNOWN')
    name   = j.get('name', 'unknown')
    tasks  = j.get('tasks', {})
    print(f'  {status}: {name}  (running={tasks.get(\"RUNNING\",0)})')
" 2>/dev/null; then
  :
else
  fail "Could not reach Flink REST API"
fi

echo ""
echo "==> Elasticsearch index stats"
ES_STATS=$(curl -s "http://localhost:9200/products/_stats" 2>/dev/null)
if [ $? -eq 0 ]; then
  echo "$ES_STATS" | python3 -c "
import sys, json
data = json.load(sys.stdin)
idx = data.get('_all', {}).get('total', {})
docs  = idx.get('docs', {}).get('count', 0)
store = idx.get('store', {}).get('size_in_bytes', 0)
print(f'  documents : {docs:,}')
print(f'  store size: {store / 1024:.1f} KB')
" 2>/dev/null || warn "Could not parse ES stats"
else
  warn "products index not found (pipeline may not have started yet)"
fi

echo ""
echo "==> Kafka consumer lag (products group)"
docker-compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group rt-index-pipeline \
  --describe 2>/dev/null \
  | grep -v "^$" \
  | grep -v "^WARN" \
  | head -20 \
  || warn "Could not query Kafka consumer lag"

echo ""
