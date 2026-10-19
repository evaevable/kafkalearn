#!/usr/bin/env bash
set -euo pipefail

LAB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$LAB_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/single-node/compose.yaml"
PYTHON_BIN="${PYTHON_BIN:-python3}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
OUTPUT="$LAB_DIR/results/$RUN_ID"

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif docker-compose version >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "FAIL: docker compose v2 is required" >&2
  exit 1
fi

"${COMPOSE[@]}" -f "$COMPOSE_FILE" up -d --wait

"$PYTHON_BIN" "$LAB_DIR/app.py" run \
  --bootstrap 127.0.0.1:19092 \
  --compose-file "$COMPOSE_FILE" \
  --output "$OUTPUT"

echo "Results: $OUTPUT"
