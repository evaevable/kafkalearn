#!/usr/bin/env python3
"""Real Python Kafka producer lab; uses confluent-kafka==2.13.2.

Direct-assignment consumer below is only a read-back assertion helper.
No group subscription/rebalance, business side effects, or offset commits.
"""
import argparse
import collections
import json
import pathlib
import sys
import time
import unittest

PIN = "2.13.2"
KEYS = {0: "order-3008", 1: "order-3001", 2: "order-3000"}
COUNT = 120


def serialize(event):
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def key_events():
    return [
        (KEYS[p], {"event_id": KEYS[p] + "-" + str(sequence),
                   "order_id": KEYS[p], "event_type": kind, "schema_version": 1})
        for sequence, kind in enumerate(("order.created", "order.paid"))
        for p in range(3)]


def bulk_events():
    return [(KEYS[1], {"event_id": "bulk-" + str(n), "order_id": KEYS[1],
                       "event_type": "order.observed", "sequence": n,
                       "padding": "x" * 512, "schema_version": 1})
            for n in range(COUNT)]


def enqueue(producer, topic, key, value, callback, wait_seconds=5):
    """Retry ONLY BufferError: this record was not accepted into local queue."""
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            producer.produce(topic, key=key.encode("utf-8"), value=value, on_delivery=callback)
            return
        except BufferError:
            if time.monotonic() >= deadline:
                raise RuntimeError("Local producer queue stayed full; record was not enqueued")
            producer.poll(min(0.1, max(0.0, deadline - time.monotonic())))


def validate_completion(remaining, deliveries, errors, expected):
    if remaining:
        raise RuntimeError("Flush deadline expired; pending=" + str(remaining)
                           + ". Delivery outcome is unresolved, not proof of no broker write.")
    if errors:
        raise RuntimeError("Delivery errors: " + repr(errors))
    if len(deliveries) != expected:
        raise RuntimeError("Missing delivery callbacks")


def summarize_stats(samples, topic):
    batches = records = max_batch = produce_requests = txmsgs = 0
    for sample in samples:
        window = sample.get("topics", {}).get(topic, {}).get("batchcnt", {})
        batches += window.get("cnt", 0)
        records += window.get("sum", 0)
        max_batch = max(max_batch, window.get("max", 0))
        # req counters are cumulative: take maximum per snapshot, do not add snapshots.
        produce_requests = max(produce_requests, sum(
            broker.get("req", {}).get("Produce", 0)
            for broker in sample.get("brokers", {}).values()))
        txmsgs = max(txmsgs, sample.get("txmsgs", 0))
    return {"statistics_samples": len(samples), "observed_batches": batches,
            "observed_batch_records": records, "max_records_per_batch": max_batch,
            "mean_records_per_batch": records / batches if batches else None,
            "produce_requests": produce_requests, "txmsgs": txmsgs}


def load_client():
    try:
        import confluent_kafka as ck
    except ImportError as exc:
        raise RuntimeError("Install requirements.txt in an isolated venv; set PYTHON_BIN") from exc
    from importlib.metadata import version
    installed = version("confluent-kafka")
    if installed != PIN:
        raise RuntimeError("Expected confluent-kafka==" + PIN + "; found " + installed)
    return ck


def produce_profile(ck, bootstrap, topic, events, per_record_flush):
    delivered, errors, samples = [], [], []
    configuration = {
        "bootstrap.servers": bootstrap, "client.id": "kafkalearn-ch04",
        "partitioner": "murmur2_random", "acks": "all", "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5, "message.timeout.ms": 15000,
        "linger.ms": 200, "batch.size": 65536, "batch.num.messages": 1000,
        "compression.type": "gzip", "queue.buffering.max.messages": 10000,
        "queue.buffering.max.kbytes": 10240, "statistics.interval.ms": 100,
        "stats_cb": lambda raw: samples.append(json.loads(raw))}
    producer = ck.Producer(configuration)
    metadata = producer.list_topics(topic, timeout=10)
    entry = metadata.topics.get(topic)
    if not entry or entry.error or set(entry.partitions) != {0, 1, 2}:
        raise RuntimeError("Create a fresh 3-partition topic before running")
    if any(part.leader != 1 or part.replicas != [1] for part in entry.partitions.values()):
        raise RuntimeError("Expected single-broker / single-replica learning environment")

    def callback(event_id):
        def on_delivery(err, msg):
            if err is not None:
                errors.append({"event_id": event_id, "error": str(err)})
            else:
                delivered.append({"event_id": event_id, "topic": msg.topic(),
                                  "partition": msg.partition(), "offset": msg.offset()})
        return on_delivery

    start = time.perf_counter()
    try:
        for key, event in events:
            enqueue(producer, topic, key, serialize(event), callback(event["event_id"]))
            producer.poll(0)  # Serve Python callbacks; network progress also runs in background.
            if per_record_flush:
                pending = producer.flush(20)
                if pending or errors:
                    raise RuntimeError("Per-record delivery failed or stayed pending")
        # Observe callbacks already served; this is not the broker's network completion count.
        callbacks_before_final_flush = len(delivered)
        pending = producer.flush(20)
        elapsed = time.perf_counter() - start
        validate_completion(pending, delivered, errors, len(events))
        # Collect a post-send statistics tick without counting this wait as send elapsed time.
        until = time.monotonic() + 0.3
        while time.monotonic() < until:
            producer.poll(min(0.05, max(0.0, until - time.monotonic())))
    except Exception:
        # Bounded best-effort drain; errors still propagate, never convert failure into success.
        producer.flush(2)
        raise
    stats = summarize_stats(samples, topic)
    if not stats["observed_batches"] or stats["txmsgs"] < len(events):
        raise RuntimeError("Statistics incomplete; inspect local load instead of inventing batch results")
    return {"topic": topic, "mode": "serial" if per_record_flush else "burst",
            "record_count": len(events), "callbacks_before_final_flush": callbacks_before_final_flush,
            "delivery_successes": len(delivered), "delivery_errors": errors,
            "remaining_after_flush": pending, "send_elapsed_seconds": elapsed,
            "records_per_second": len(events) / elapsed, "stats": stats,
            "statistics_snapshots": samples, "deliveries": delivered,
            "producer_configuration": {k: v for k, v in configuration.items() if k != "stats_cb"},
            "versions": {"python_client": PIN, "binding_version": ck.version(), "librdkafka": ck.libversion()[0]}}


def read_back(ck, bootstrap, topic, events, deliveries):
    consumer = ck.Consumer({"bootstrap.servers": bootstrap,
                            "group.id": "ch04-direct-readback-" + topic,
                            "enable.auto.commit": False, "enable.auto.offset.store": False,
                            "allow.auto.create.topics": False})
    rows = []
    try:
        watermarks = {p: consumer.get_watermark_offsets(ck.TopicPartition(topic, p), timeout=10)
                      for p in range(3)}
        if any(low != 0 for low, high in watermarks.values()) or sum(high for low, high in watermarks.values()) != len(events):
            raise RuntimeError("Topic must contain exactly this experiment's fresh records")
        consumer.assign([ck.TopicPartition(topic, p, 0) for p in range(3)])
        deadline = time.monotonic() + 20
        while len(rows) < len(events) and time.monotonic() < deadline:
            msg = consumer.poll(min(0.5, max(0.0, deadline - time.monotonic())))
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            rows.append({"partition": msg.partition(), "offset": msg.offset(),
                         "key": msg.key().decode("utf-8") if msg.key() is not None else None,
                         "event": json.loads(msg.value().decode("utf-8"))})
    finally:
        consumer.close()  # No subscribe/join group and no offset commits in this chapter.
    wanted = {event["event_id"]: (key, event) for key, event in events}
    actual = {r["event"]["event_id"]: (r["key"], r["event"]) for r in rows}
    if len(rows) != len(events) or len(actual) != len(rows) or actual != wanted:
        raise RuntimeError("Read-back key/event content differs or contains duplicates")
    delivery_coords = {d["event_id"]: (d["partition"], d["offset"]) for d in deliveries}
    for row in rows:
        if delivery_coords.get(row["event"]["event_id"]) != (row["partition"], row["offset"]):
            raise RuntimeError("Callback coordinate differs from actual stored record")
    for p, key in KEYS.items():
        expected_ids = [event["event_id"] for k, event in events if k == key]
        partition_rows = sorted([r for r in rows if r["partition"] == p], key=lambda r: r["offset"])
        if [r["key"] for r in partition_rows] != [key] * len(expected_ids):
            raise RuntimeError("Explicit murmur2_random route differs from chapter 03 mapping")
        if [r["event"]["event_id"] for r in partition_rows] != expected_ids:
            raise RuntimeError("Per-key partition order differs from enqueue order")
        if [r["offset"] for r in partition_rows] != list(range(len(expected_ids))):
            raise RuntimeError("Expected contiguous offsets in this fresh non-transactional topic")
    return sorted(rows, key=lambda r: (r["partition"], r["offset"]))


def run(bootstrap, prefix, output):
    ck = load_client()
    output.mkdir(parents=True, exist_ok=True)
    profiles = {}
    for name, events, serial in (("keys", key_events(), False),
                                ("serial", bulk_events(), True), ("burst", bulk_events(), False)):
        topic = prefix + "." + name
        result = produce_profile(ck, bootstrap, topic, events, serial)
        result["readback"] = read_back(ck, bootstrap, topic, events, result["deliveries"])
        (output / (name + ".json")).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        profiles[name] = result
        print("PASS:", name, result["delivery_successes"], "deliveries and read-back records match")
    serial_stats = profiles["serial"]["stats"]
    burst_stats = profiles["burst"]["stats"]
    summary = {name: {k: result[k] for k in ("record_count", "delivery_successes",
               "remaining_after_flush", "callbacks_before_final_flush", "send_elapsed_seconds",
               "records_per_second", "stats", "versions")} for name, result in profiles.items()}
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    if not burst_stats["max_records_per_batch"] > 1:
        raise RuntimeError("Burst run did not observe multi-record batches")
    if not burst_stats["produce_requests"] < serial_stats["produce_requests"]:
        raise RuntimeError("Request comparison inconclusive in this run; summary retained")
    print("PASS: Explicit Python key routing matches chapter 03 Java CLI mapping")
    print("PASS: Burst observes multi-record batches and fewer Produce requests than serial")
    print(json.dumps(summary, indent=2))
    print("NOTE: Local learning comparison, not a production benchmark; callbacks are not business completion")


class FakeProducer:
    def __init__(self, full_once=False):
        self.full_once = full_once
        self.calls = self.polls = 0

    def produce(self, *args, **kwargs):
        self.calls += 1
        if self.full_once and self.calls == 1:
            raise BufferError("full")

    def poll(self, timeout):
        self.polls += 1


class Tests(unittest.TestCase):
    def test_json_bytes(self):
        self.assertEqual(json.loads(serialize({"name": "订单"})), {"name": "订单"})

    def test_six_unique_events(self):
        self.assertEqual(len({e["event_id"] for k, e in key_events()}), 6)

    def test_enqueue(self):
        p = FakeProducer()
        enqueue(p, "topic", "key", b"value", lambda e, m: None)
        self.assertEqual(p.calls, 1)

    def test_backpressure_retry(self):
        p = FakeProducer(True)
        enqueue(p, "topic", "key", b"value", lambda e, m: None)
        self.assertEqual((p.calls, p.polls), (2, 1))

    def test_backpressure_deadline(self):
        with self.assertRaises(RuntimeError):
            enqueue(FakeProducer(True), "topic", "key", b"value", None, wait_seconds=0)

    def test_pending_rejected(self):
        with self.assertRaises(RuntimeError):
            validate_completion(1, [], [], 1)

    def test_delivery_error_rejected(self):
        with self.assertRaises(RuntimeError):
            validate_completion(0, [], ["timeout"], 1)

    def test_missing_callback_rejected(self):
        with self.assertRaises(RuntimeError):
            validate_completion(0, [], [], 1)

    def test_stats_windows_and_cumulative_counters(self):
        samples = [{"topics": {"t": {"batchcnt": {"cnt": 1, "sum": 5, "max": 5}}},
                    "brokers": {"b": {"req": {"Produce": 1}}}, "txmsgs": 5},
                   {"topics": {"t": {"batchcnt": {"cnt": 1, "sum": 3, "max": 3}}},
                    "brokers": {"b": {"req": {"Produce": 2}}}, "txmsgs": 8}]
        stats = summarize_stats(samples, "t")
        self.assertEqual((stats["observed_batches"], stats["mean_records_per_batch"],
                          stats["produce_requests"]), (2, 4, 2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("self-test")
    command = sub.add_parser("run")
    command.add_argument("--bootstrap", default="127.0.0.1:19092")
    command.add_argument("--prefix", required=True)
    command.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()
    if args.action == "self-test":
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    try:
        run(args.bootstrap, args.prefix, args.output)
        return 0
    except Exception as exc:
        print("FAIL:", str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
