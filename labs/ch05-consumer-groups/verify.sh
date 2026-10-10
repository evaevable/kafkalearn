#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LAB="$ROOT/labs/ch05-consumer-groups"
COMPOSE_FILE="$ROOT/infra/single-node/compose.yaml"
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
TOPIC="shopflow.orders.ch05.$RUN_ID"
GROUP_PREFIX="shopflow.ch05.$RUN_ID"
RESULTS="$LAB/results/$RUN_ID"
mkdir -p "$RESULTS"
printf 'TOPIC=%s\nINVENTORY_GROUP=%s.inventory\nANALYTICS_GROUP=%s.analytics\n' \
  "$TOPIC" "$GROUP_PREFIX" "$GROUP_PREFIX" > "$RESULTS/run.env"
trap 'printf "Topic and group metadata retained. Results: %s\n" "$RESULTS"' EXIT
printf 'NOTICE: Launches 6 local workers; do not run with chapter 02 recreation.\n'
"${COMPOSE[@]}" up -d --wait --wait-timeout 180
"$PYTHON_BIN" "$LAB/app.py" self-test
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --topic "$TOPIC" --partitions 3 --replication-factor 1
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --describe --topic "$TOPIC" > "$RESULTS/topic.txt"
"$PYTHON_BIN" "$LAB/app.py" run --topic "$TOPIC" --group-prefix "$GROUP_PREFIX" \
  --bootstrap 127.0.0.1:19092 --output "$RESULTS" \
  "${EXTRA[@]}"
printf 'PASS: chapter 05 acceptance checks completed.\n'
