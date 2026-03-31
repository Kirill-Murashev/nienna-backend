#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-https://app.digitalpm.info/api/nienna}"

python3 - "$BASE_URL" <<'PY'
from __future__ import annotations

import json
import sys
import urllib.request


base_url = sys.argv[1].rstrip("/")


def get_json(path: str) -> dict:
    with urllib.request.urlopen(f"{base_url}{path}", timeout=60) as response:
        return json.load(response)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


health = get_json("/healthz")
require(health.get("status") == "ok", f"Health check failed: {health}")
print(f"healthz: ok ({health.get('service')})")

dataset = get_json("/api/v1/nienna/dataset")
require(dataset.get("rows", 0) > 0, f"Unexpected dataset payload: {dataset}")
require(dataset.get("columns_count", 0) > 0, f"Unexpected dataset payload: {dataset}")
print(f"dataset: rows={dataset['rows']} columns={dataset['columns_count']}")

print("smoke: ok")
PY
