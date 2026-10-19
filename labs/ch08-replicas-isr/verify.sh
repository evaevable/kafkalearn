#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LAB="$ROOT/labs/ch08-replicas-isr"
COMPOSE_FILE="$ROOT/infra/three-nodes/compose.yaml"
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -f "$COMPOSE_FILE")
  EXTRA=(--compose-file "$COMPOSE_FILE")
elif command -v docker-compose >/dev/null && [[ "$(docker-compose version --short)" == 2.* ]]; then
  COMPOSE=(docker-compose -f "$COMPOSE_FILE")
  EXTRA=(--compose-file "$COMPOSE_FILE" --compose-command "$(command -v docker-compose)")
else
  printf 'FAIL: Install Docker Compose v2.\n' >&2
  exit 1
fi
PYTHON_BIN="${PYTHON_BIN:-python3}"
command -v "$PYTHON_BIN" >/dev/null || { printf 'FAIL: Set PYTHON_BIN to the prepared venv interpreter.\n' >&2; exit 1; }
"$PYTHON_BIN" -c 'import sys; assert sys.version_info >= (3, 9); import confluent_kafka; from importlib.metadata import version; assert version("confluent-kafka") == "2.13.2", "Install pinned requirements.txt first"'
docker info >/dev/null 2>&1
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
PREFIX="shopflow.orders.ch08.$RUN_ID"
RESULTS="$LAB/results/$RUN_ID"
mkdir -p "$RESULTS"
printf 'PREFIX=%s\nBROKERS=127.0.0.1:19192,127.0.0.1:29292,127.0.0.1:39392\n' "$PREFIX" > "$RESULTS/run.env"
trap 'printf "Data volumes retained. Results: %s\n" "$RESULTS"' EXIT
printf 'NOTICE: Starts a three-broker local cluster and stops exactly ONE broker on purpose.\n'
printf 'No topic deletion, no disk/power failure, no two-broker outage.\n'
"${COMPOSE[@]}" up -d --wait --wait-timeout 240
"$PYTHON_BIN" "$LAB/app.py" self-test
"$PYTHON_BIN" "$LAB/app.py" run --prefix "$PREFIX" --output "$RESULTS" "${EXTRA[@]}"
printf 'PASS: chapter 08 observations completed.\n'
"${COMPOSE[@]}" stop
printf 'Cluster stopped, volumes retained. Restart: docker compose -f infra/three-nodes/compose.yaml up -d --wait\n'
