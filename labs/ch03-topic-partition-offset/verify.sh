#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LAB="$ROOT/labs/ch03-topic-partition-offset"
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -f "$ROOT/infra/single-node/compose.yaml")
elif command -v docker-compose >/dev/null && [[ "$(docker-compose version --short)" == 2.* ]]; then
  COMPOSE=(docker-compose -f "$ROOT/infra/single-node/compose.yaml")
else
  printf 'FAIL: Install Docker Compose v2.\n' >&2
  exit 1
fi
PYTHON_BIN="${PYTHON_BIN:-python3}"
command -v "$PYTHON_BIN" >/dev/null || { printf 'FAIL: Set PYTHON_BIN to Python 3.9+.\n' >&2; exit 1; }
"$PYTHON_BIN" -c 'import sys; assert sys.version_info >= (3, 9)'
docker info >/dev/null 2>&1
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
TOPIC="shopflow.orders.ch03.$RUN_ID"
RESULTS="$LAB/results/$RUN_ID"
mkdir -p "$RESULTS"
printf 'TOPIC=%s\n' "$TOPIC" > "$RESULTS/run.env"
BIN="/opt/kafka/bin"
kafka() { "${COMPOSE[@]}" exec -T kafka "$@"; }
trap 'printf "Topic retained. Results: %s\n" "$RESULTS"' EXIT
printf 'NOTICE: Local teaching lab; do not run concurrently with the chapter 02 recreation lab.\n'
"${COMPOSE[@]}" up -d --wait --wait-timeout 180
"$PYTHON_BIN" "$LAB/lab.py" self-test
"$PYTHON_BIN" "$LAB/lab.py" generate "$RESULTS"
kafka "$BIN/kafka-topics.sh" --bootstrap-server kafka:9092 \
  --create --topic "$TOPIC" --partitions 3 --replication-factor 1
kafka "$BIN/kafka-topics.sh" --bootstrap-server kafka:9092 \
  --describe --topic "$TOPIC" > "$RESULTS/topic.txt"
printf 'Publishing 6 keyed events with one console producer...\n'
kafka "$BIN/kafka-console-producer.sh" --bootstrap-server kafka:9092 \
  --topic "$TOPIC" --reader-property parse.key=true --reader-property 'key.separator=|' \
  --command-property acks=all --command-property enable.idempotence=true \
  < "$RESULTS/input.txt"
for p in 0 1 2; do
  printf 'Reading partition %s from offset 0...\n' "$p"
  kafka "$BIN/kafka-console-consumer.sh" --bootstrap-server kafka:9092 \
    --topic "$TOPIC" --partition "$p" --offset 0 --max-messages 2 --timeout-ms 30000 \
    --formatter-property print.partition=true --formatter-property print.offset=true \
    --formatter-property print.key=true --formatter-property print.value=true \
    > "$RESULTS/partition-$p.txt"
done
kafka "$BIN/kafka-get-offsets.sh" --bootstrap-server kafka:9092 \
  --topic "$TOPIC" --time -1 > "$RESULTS/end-offsets.txt"
printf 'Locating the second event in partition 1 directly...\n'
kafka "$BIN/kafka-console-consumer.sh" --bootstrap-server kafka:9092 \
  --topic "$TOPIC" --partition 1 --offset 1 --max-messages 1 --timeout-ms 30000 \
  > "$RESULTS/seek-partition-1-offset-1.jsonl"
"$PYTHON_BIN" "$LAB/lab.py" verify "$RESULTS" "$TOPIC"
printf 'PASS: chapter 03 acceptance checks completed.\n'
