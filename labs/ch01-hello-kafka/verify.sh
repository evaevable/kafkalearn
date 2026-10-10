#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -f "$ROOT/infra/single-node/compose.yaml")
elif command -v docker-compose >/dev/null && [[ "$(docker-compose version --short)" == 2.* ]]; then
  COMPOSE=(docker-compose -f "$ROOT/infra/single-node/compose.yaml")
else
  printf 'FAIL: Install Docker Compose v2 (plugin or standalone).\n' >&2
  exit 1
fi
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
TOPIC="shopflow.orders.ch01.$RUN_ID"
INVENTORY_GROUP="shopflow.inventory.ch01.$RUN_ID"
ANALYTICS_GROUP="shopflow.analytics.ch01.$RUN_ID"
RESULTS="$ROOT/labs/ch01-hello-kafka/results/$RUN_ID"
BIN="/opt/kafka/bin"

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
kafka() { "${COMPOSE[@]}" exec -T kafka "$@"; }

command -v docker >/dev/null || fail "Install Docker and Docker Compose v2 first."
docker info >/dev/null 2>&1 || fail "Docker daemon is not running."
"${COMPOSE[@]}" exec -T kafka "$BIN/kafka-topics.sh" \
  --bootstrap-server kafka:9092 --list >/dev/null 2>&1 || \
  fail "Kafka is not ready. Run: docker compose -f infra/single-node/compose.yaml up -d --wait"
mkdir -p "$RESULTS"
printf 'TOPIC=%s\nINVENTORY_GROUP=%s\nANALYTICS_GROUP=%s\n' \
  "$TOPIC" "$INVENTORY_GROUP" "$ANALYTICS_GROUP" > "$RESULTS/run.env"

trap 'printf "Experiment data and topic are retained. Results: %s\n" "$RESULTS"' EXIT

printf 'Creating isolated topic: %s\n' "$TOPIC"
kafka "$BIN/kafka-topics.sh" --bootstrap-server kafka:9092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
kafka "$BIN/kafka-topics.sh" --bootstrap-server kafka:9092 \
  --describe --topic "$TOPIC" > "$RESULTS/topic.txt"

printf 'Producing 3 order events...\n'
kafka "$BIN/kafka-console-producer.sh" --bootstrap-server kafka:9092 \
  --topic "$TOPIC" --command-property acks=all < "$ROOT/labs/ch01-hello-kafka/events.jsonl"

consume_group() {
  local group="$1" output="$2"
  kafka "$BIN/kafka-console-consumer.sh" --bootstrap-server kafka:9092 \
    --topic "$TOPIC" --group "$group" --from-beginning \
    --max-messages 3 --timeout-ms 30000 > "$output"
}

check_events() {
  local output="$1" label="$2"
  cmp -s "$ROOT/labs/ch01-hello-kafka/events.jsonl" "$output" || \
    fail "$label differs from the 3 source events. Inspect: $output"
  printf 'PASS: %s read the same 3 events in partition order.\n' "$label"
}

printf 'Reading with independent inventory group...\n'
consume_group "$INVENTORY_GROUP" "$RESULTS/inventory.jsonl"
check_events "$RESULTS/inventory.jsonl" "inventory group"

printf 'Reading with independent analytics group...\n'
consume_group "$ANALYTICS_GROUP" "$RESULTS/analytics.jsonl"
check_events "$RESULTS/analytics.jsonl" "analytics group"

printf 'Replaying partition 0 directly from offset 0 (no group reset)...\n'
kafka "$BIN/kafka-console-consumer.sh" --bootstrap-server kafka:9092 \
  --topic "$TOPIC" --partition 0 --offset 0 \
  --max-messages 3 --timeout-ms 30000 > "$RESULTS/replay.jsonl"
check_events "$RESULTS/replay.jsonl" "direct replay"

printf 'Reading again with visible partition and offset...\n'
kafka "$BIN/kafka-console-consumer.sh" --bootstrap-server kafka:9092 \
  --topic "$TOPIC" --partition 0 --offset 0 \
  --max-messages 3 --timeout-ms 30000 \
  --formatter-property print.partition=true --formatter-property print.offset=true \
  --formatter-property print.value=true > "$RESULTS/offsets.txt"

printf 'PASS: chapter 01 acceptance checks completed.\n'
printf 'Offset observations: %s\n' "$RESULTS/offsets.txt"
