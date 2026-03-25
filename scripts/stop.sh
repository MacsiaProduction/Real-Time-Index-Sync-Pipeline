#!/usr/bin/env bash
# stop.sh — stop all Docker Compose services
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Stopping all services..."
docker-compose down

echo "==> Done. To also remove volumes (delete all data):"
echo "    docker-compose down -v"
