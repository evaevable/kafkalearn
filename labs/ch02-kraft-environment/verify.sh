#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LAB="$ROOT/labs/ch02-kraft-environment"
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -f "$ROOT/infra/single-node/compose.yaml")
elif command -v docker-compose >/dev/null && [[ "$(docker-compose version --short)" == 2.* ]]; then
  COMPOSE=(docker-compose -f "$ROOT/infra/single-node/compose.yaml")
else
  printf 'FAIL: Install Docker Compose v2.\n' >&2
  exit 1
fi
PYTHON_BIN="${PYTHON_BIN:-python3}"
command -v "$PYTHON_BIN" >/dev/null || { printf 'FAIL: Set PYTHON_BIN to Python 3.9+ executable.\n' >&2; exit 1; }
"$PYTHON_BIN" -c 'import sys; assert sys.version_info >= (3, 9)'
docker info >/dev/null 2>&1
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
TOPIC="shopflow.orders.ch02.$RUN_ID"
RESULTS="$LAB/results/$RUN_ID"
mkdir -p "$RESULTS"
printf 'TOPIC=%s\n' "$TOPIC" > "$RESULTS/run.env"
BIN="/opt/kafka/bin"
kafka() { "${COMPOSE[@]}" exec -T kafka "$@"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
trap 'printf "Topic and volume retained. Results: %s\n" "$RESULTS"' EXIT

printf 'NOTICE: This lab recreates the shared local Kafka container, interrupting other labs.\n'
printf 'No topics or volumes are deleted. Do not use this script against production.\n'
"${COMPOSE[@]}" up -d --wait --wait-timeout 180
"$PYTHON_BIN" "$LAB/probe_metadata.py" --self-test

snapshot() {
  local stage="$1" id
  id="$("${COMPOSE[@]}" ps -q kafka)"
  [[ -n "$id" ]] || fail "No Kafka container found."
  docker inspect "$id" > "$RESULTS/$stage.container.json"
  kafka "$BIN/kafka-metadata-quorum.sh" --bootstrap-server kafka:9092 \
    describe --status > "$RESULTS/$stage.quorum.txt"
  kafka "$BIN/kafka-metadata-quorum.sh" --bootstrap-server kafka:9092 \
    describe --replication > "$RESULTS/$stage.replication.txt"
  kafka "$BIN/kafka-topics.sh" --bootstrap-server kafka:9092 \
    --describe --topic "$TOPIC" > "$RESULTS/$stage.topic.txt"
  kafka cat /var/lib/kafka/data/meta.properties > "$RESULTS/$stage.meta.properties"
  kafka ls -1 /var/lib/kafka/data > "$RESULTS/$stage.data-dir.txt"
  kafka "$BIN/kafka-broker-api-versions.sh" --bootstrap-server kafka:9092 \
    > "$RESULTS/$stage.internal-api.txt"
  "$PYTHON_BIN" "$LAB/probe_metadata.py" > "$RESULTS/$stage.external-metadata.json"
  kafka "$BIN/kafka-console-consumer.sh" --bootstrap-server kafka:9092 \
    --topic "$TOPIC" --partition 0 --offset 0 --max-messages 1 --timeout-ms 30000 \
    > "$RESULTS/$stage.event.jsonl"
  cmp -s "$RESULTS/source.jsonl" "$RESULTS/$stage.event.jsonl" || \
    fail "$stage event differs from the source."
}

kafka "$BIN/kafka-topics.sh" --bootstrap-server kafka:9092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
printf '{"event_id":"evt-ch02-%s","event_type":"order.created","order_id":"order-ch02-%s","schema_version":1}\n' \
  "$RUN_ID" "$RUN_ID" > "$RESULTS/source.jsonl"
kafka "$BIN/kafka-console-producer.sh" --bootstrap-server kafka:9092 \
  --topic "$TOPIC" --command-property acks=all < "$RESULTS/source.jsonl"
snapshot before
printf 'PASS: Metadata, internal/external endpoints, and initial event verified.\n'

printf 'Recreating only Kafka, retaining the named data volume...\n'
"${COMPOSE[@]}" up -d --force-recreate --no-deps --wait --wait-timeout 180 kafka
snapshot after

"$PYTHON_BIN" - "$RESULTS" "$TOPIC" <<'PY'
import json
import pathlib
import re
import sys
root = pathlib.Path(sys.argv[1])
topic = sys.argv[2]
expected_cluster = "MkU3OEVBNTcwNTJENDM2Qk"

def read(name):
    return (root / name).read_text()

def volume(stage):
    mounts = json.loads(read(stage + ".container.json"))[0]["Mounts"]
    match = [m for m in mounts if m["Destination"] == "/var/lib/kafka/data"]
    assert len(match) == 1 and match[0]["Type"] == "volume"
    return match[0]["Name"]

def field(text, name):
    value = re.search(r"\b" + re.escape(name) + r":\s*(\S+)", text)
    assert value, (name, text)
    return value.group(1)

before = json.loads(read("before.container.json"))[0]
after = json.loads(read("after.container.json"))[0]
assert before["Id"] != after["Id"], "Container must actually be recreated"
assert volume("before") == volume("after") == "kafkalearn_kafka-data"
assert read("before.meta.properties") == read("after.meta.properties")
assert "cluster.id=" + expected_cluster in read("after.meta.properties")
assert "node.id=1" in read("after.meta.properties")
assert field(read("before.topic.txt"), "TopicId") == field(read("after.topic.txt"), "TopicId")
for stage in ("before", "after"):
    status = read(stage + ".quorum.txt")
    assert field(status, "ClusterId") == expected_cluster
    assert field(status, "LeaderId") == "1"
    voters = re.search(r"^CurrentVoters:\s*(\[.*\])$", status, re.M)
    assert voters and [v["id"] for v in json.loads(voters.group(1))] == [1]
    description = read(stage + ".topic.txt")
    assert field(description, "PartitionCount") == "1"
    assert field(description, "ReplicationFactor") == "1"
    assert topic + "-0" in read(stage + ".data-dir.txt")
    assert "__cluster_metadata-0" in read(stage + ".data-dir.txt")
    assert "kafka:9092 (id: 1" in read(stage + ".internal-api.txt")
    assert read(stage + ".event.jsonl") == read("source.jsonl")
print("PASS: Container ID changed; named data volume and meta.properties unchanged.")
print("PASS: Cluster identity, topic identity, metadata directory, and order event persisted.")
print("PASS: Single-voter quorum and both listener paths verified before/after recreation.")
PY
printf 'PASS: chapter 02 acceptance checks completed.\n'
