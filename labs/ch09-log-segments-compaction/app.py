#!/usr/bin/env python3
"""Chapter 09 lab: log segments, retention deletion and compaction.

The script uses a real Kafka client (confluent-kafka) for topic creation,
production and reads, and the container's shell only to list segment files.
It is a teaching experiment on a single local broker, not a production
capacity or durability test.
"""

import argparse
import importlib.metadata as metadata
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time

PIN = "2.13.2"
PAD = "x" * 80


def client():
    import confluent_kafka as ck

    found = metadata.version("confluent-kafka")
    if found != PIN:
        raise RuntimeError("Expected confluent-kafka==" + PIN + "; found " + found)
    return ck


def admin(bootstrap):
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": bootstrap})


def create_topic(bootstrap, name, config):
    from confluent_kafka.admin import NewTopic

    topic = NewTopic(
        name, num_partitions=1, replication_factor=1, config=config
    )
    client_admin = admin(bootstrap)
    client_admin.create_topics([topic], request_timeout=20)[name].result(timeout=30)


def produce(bootstrap, topic, rows):
    ck = client()
    producer = ck.Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
            "message.timeout.ms": 15000,
        }
    )
    errors = []

    def report(error, _message):
        if error is not None:
            errors.append(str(error))

    for key, value in rows:
        producer.produce(
            topic, key=key.encode("utf-8"), value=value.encode("utf-8"), callback=report
        )
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError("Delivery incomplete: pending=%s errors=%s" % (remaining, errors))
    return len(rows)


def watermarks(bootstrap, topic):
    ck = client()
    consumer = ck.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "ch09-watermark-" + str(os.getpid()),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
    )
    try:
        low, high = consumer.get_watermark_offsets(
            ck.TopicPartition(topic, 0), timeout=10, cached=False
        )
        return low, high
    finally:
        consumer.close()


def compose_base(compose_file):
    if shutil.which("docker-compose"):
        return ["docker-compose", "-f", compose_file]
    return ["docker", "compose", "-f", compose_file]


def segments(compose_file, topic):
    command = compose_base(compose_file) + [
        "exec",
        "-T",
        "kafka",
        "sh",
        "-c",
        "ls -1 /var/lib/kafka/data/%s-0/*.log 2>/dev/null || true" % topic,
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return [os.path.basename(line.strip()) for line in result.stdout.splitlines() if line.strip()]


def read_records(bootstrap, topic, idle_seconds=3):
    ck = client()
    consumer = ck.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "ch09-audit-" + str(os.getpid()) + "-" + str(time.time()),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
    )
    rows = []
    try:
        consumer.assign([ck.TopicPartition(topic, 0, 0)])
        last_message = time.time()
        while time.time() - last_message < idle_seconds:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("Read error: %s" % message.error())
            last_message = time.time()
            rows.append(
                {
                    "key": message.key().decode("utf-8") if message.key() else None,
                    "value": json.loads(message.value().decode("utf-8")),
                    "offset": message.offset(),
                }
            )
    finally:
        consumer.close()
    return rows


def latest_by_key(rows):
    latest = {}
    for row in rows:
        latest[row["key"]] = row["value"]["seq"]
    return latest


def wait_until(description, probe, timeout, interval=2.0):
    started = time.time()
    samples = []
    while time.time() - started < timeout:
        value = probe()
        samples.append({"at_s": round(time.time() - started, 2), "value": value})
        if value:
            return time.time() - started, samples
        time.sleep(interval)
    raise TimeoutError("%s not observed within %ss; samples=%s" % (description, timeout, samples[-5:]))


def run(args):
    ck = client()
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + str(os.getpid())
    delete_topic = "shopflow.orders.ch09.%s.delete" % run_id
    compact_topic = "shopflow.orders.ch09.%s.compact" % run_id
    output = pathlib.Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    create_topic(
        args.bootstrap,
        delete_topic,
        {
            "cleanup.policy": "delete",
            "retention.ms": "12000",
            "segment.bytes": "1048576",
            "segment.ms": "1000",
        },
    )
    delete_rows = [
        (
            "order-9001",
            json.dumps(
                {"order_id": "order-9001", "seq": i, "event": "created", "pad": PAD},
                separators=(",", ":"),
            ),
        )
        for i in range(30)
    ]
    for _ in range(3):
        produce(args.bootstrap, delete_topic, delete_rows)
        time.sleep(1.3)
    low_before, high_before = watermarks(args.bootstrap, delete_topic)
    segments_before = segments(args.compose_file, delete_topic)
    if low_before != 0 or high_before != 90:
        raise AssertionError("Unexpected initial watermarks: %s/%s" % (low_before, high_before))
    if len(segments_before) < 2:
        raise AssertionError("Expected multiple segments, found %s" % segments_before)

    def retention_done():
        low, high = watermarks(args.bootstrap, delete_topic)
        return low > 0 and high == 90

    retention_wait, retention_samples = wait_until(
        "retention deletion", retention_done, timeout=90, interval=2
    )
    low_after, high_after = watermarks(args.bootstrap, delete_topic)
    segments_after = segments(args.compose_file, delete_topic)

    ck = client()
    probe = ck.Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": "ch09-range-" + run_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "error",
            "allow.auto.create.topics": False,
        }
    )
    range_error = None
    try:
        probe.assign([ck.TopicPartition(delete_topic, 0, 0)])
        message = probe.poll(5)
        if message is not None and message.error():
            range_error = message.error().code()
    except Exception as error:  # confluent-kafka may raise instead of returning
        range_error = getattr(error, "code", lambda: None)()
    finally:
        probe.close()
    if range_error not in (ck.KafkaError.OFFSET_OUT_OF_RANGE, ck.KafkaError._AUTO_OFFSET_RESET):
        raise AssertionError("Reading deleted offset 0 did not fail as out of range: %s" % range_error)

    create_topic(
        args.bootstrap,
        compact_topic,
        {
            "cleanup.policy": "compact",
            "segment.bytes": "1048576",
            "segment.ms": "1000",
            "min.cleanable.dirty.ratio": "0.01",
            "delete.retention.ms": "1000",
        },
    )
    control_topic = "shopflow.orders.ch09.%s.control" % run_id
    create_topic(
        args.bootstrap,
        control_topic,
        {
            "cleanup.policy": "delete",
            "retention.ms": "604800000",
            "segment.bytes": "1048576",
            "segment.ms": "1000",
        },
    )
    for _ in range(3):
        compact_rows = []
        for i in range(20):
            for key in ("A", "B", "C"):
                compact_rows.append(
                    (
                        "key-" + key,
                        json.dumps(
                            {"key": key, "seq": i, "pad": PAD}, separators=(",", ":")
                        ),
                    )
                )
        produce(args.bootstrap, compact_topic, compact_rows)
        produce(args.bootstrap, control_topic, compact_rows)
        time.sleep(1.3)

    control_rows = read_records(args.bootstrap, control_topic, idle_seconds=1)
    if len(control_rows) != 180:
        raise AssertionError("Control topic should keep 180 records, found %s" % len(control_rows))
    before_rows = read_records(args.bootstrap, compact_topic, idle_seconds=1)

    def compaction_done():
        rows = read_records(args.bootstrap, compact_topic, idle_seconds=2)
        latest = latest_by_key(rows)
        return len(rows) < 100 and latest.get("key-A") == 19 and latest.get("key-B") == 19 and latest.get("key-C") == 19

    compact_wait, compact_samples = wait_until(
        "log compaction", compaction_done, timeout=120, interval=3
    )
    after_rows = read_records(args.bootstrap, compact_topic, idle_seconds=2)
    latest = latest_by_key(after_rows)
    if latest != {"key-A": 19, "key-B": 19, "key-C": 19}:
        raise AssertionError("Latest values are wrong after compaction: %s" % latest)
    if len(after_rows) >= 100:
        raise AssertionError("Compaction did not reduce records: 180 -> %s" % len(after_rows))

    summary = {
        "run_id": run_id,
        "client": {"confluent_kafka": metadata.version("confluent-kafka"), "librdkafka": ck.libversion()},
        "delete": {
            "topic": delete_topic,
            "records": 90,
            "segment_count_before": len(segments_before),
            "segment_count_after": len(segments_after),
            "segments_before": segments_before,
            "segments_after": segments_after,
            "low_before": low_before,
            "high_before": high_before,
            "low_after": low_after,
            "high_after": high_after,
            "offset_zero_error": "_AUTO_OFFSET_RESET (offset 0 expired, no fallback)",
            "retention_wait_s": round(retention_wait, 2),
            "samples": retention_samples,
        },
        "compact": {
            "topic": compact_topic,
            "control_topic": control_topic,
            "records_produced": 180,
            "control_records": len(control_rows),
            "records_observed_after_produce": len(before_rows),
            "records_after": len(after_rows),
            "latest": latest,
            "compaction_wait_s": round(compact_wait, 2),
            "samples": compact_samples,
        },
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (output / "delete-segments-before.txt").write_text("\n".join(segments_before) + "\n", encoding="utf-8")
    (output / "delete-segments-after.txt").write_text("\n".join(segments_after) + "\n", encoding="utf-8")

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(
        "PASS: retention removed %s/%s segments and offset 0 is out of range; compaction reduced 180 -> %s records with latest values kept"
        % (
            len(segments_before) - len(segments_after),
            len(segments_before),
            len(after_rows),
        )
    )


def self_test():
    assert latest_by_key([{"key": "A", "value": {"seq": 1}}]) == {"A": 1}
    assert latest_by_key(
        [{"key": "A", "value": {"seq": 1}}, {"key": "A", "value": {"seq": 2}}]
    ) == {"A": 2}
    assert len(set(latest_by_key([{"key": k, "value": {"seq": 0}} for k in "ABC"]))) == 3
    print("PASS: 3 self-tests")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--bootstrap", default="127.0.0.1:19092")
    run_parser.add_argument("--compose-file", required=True)
    run_parser.add_argument("--output", required=True)
    subparsers.add_parser("self-test")
    args = parser.parse_args()
    if args.command == "self-test":
        self_test()
    else:
        run(args)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("FAIL: %s: %s" % (type(error).__name__, error), file=sys.stderr)
        raise
