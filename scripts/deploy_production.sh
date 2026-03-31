#!/usr/bin/env bash
set -euo pipefail

echo "[1/2] Build and start nienna-backend container..."
docker compose up -d --build

echo "[2/2] Health check..."
for i in {1..20}; do
  if curl -fsS http://127.0.0.1:8015/healthz >/dev/null 2>&1; then
    echo "nienna-backend is healthy."
    exit 0
  fi
  sleep 1
done

echo "nienna-backend health check failed." >&2
docker compose ps >&2 || true
exit 1
