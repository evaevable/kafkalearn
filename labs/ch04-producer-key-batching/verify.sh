#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LAB="$ROOT/labs/ch04-producer-key-batching"
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -f "$ROOT/infra/single-node/compose.yaml")
elif command -v docker-compose >/dev/null && [[ "$(docker-compose version --short)" == 2.* ]]; then
  COMPOSE=(docker-compose -f "$ROOT/infra/single-node/compose.yaml")
else
  printf 'FAIL: Install Docker Compose v2.\n' >&2
  exit 1
fi
PYTHON_BIN="${PYTHON_BIN:-python3}"
command -v "$PYTHON_BIN" >/dev/null || { printf 'FAIL: Set PYTHON_BIN to the prepared venv interpreter.\n' >&2; exit 1; }
"$PYTHON_BIN" -c 'import sys; assert sys.version_info >= (3, 9); import confluent_kafka; from importlib.metadata import version; assert version("confluent-kafka") == "2.13.2", "Install pinned requirements.txt first"'
docker info >/dev/null 2>&1
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
PREFIX="shopflow.orders.ch04.$RUN_ID"
RESULTS="$LAB/results/$RUN_ID"
mkdir -p "$RESULTS"
printf 'PREFIX=%s\n' "$PREFIX" > "$RESULTS/run.env"
trap 'printf "Topics retained. Results: %s\n" "$RESULTS"' EXIT
printf 'NOTICE: Local single-broker lab; avoid concurrent chapter 02 recreation.\n'
"${COMPOSE[@]}" up -d --wait --wait-timeout 180
"$PYTHON_BIN" "$LAB/app.py" self-test
for mode in keys serial burst; do
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --create --topic "$PREFIX.$mode" \
    --partitions 3 --replication-factor 1
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --describe --topic "$PREFIX.$mode" \
    > "$RESULTS/$mode.topic.txt"
done
"$PYTHON_BIN" "$LAB/app.py" run --bootstrap 127.0.0.1:19092 \
  --prefix "$PREFIX" --output "$RESULTS"
printf 'PASS: chapter 04 acceptance checks completed.\n'
