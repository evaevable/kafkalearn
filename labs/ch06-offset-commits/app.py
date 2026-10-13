#!/usr/bin/env python3
"""Real same-group restart and offset commit lab, not a production consumer.

First-phase child deliberately exits via os._exit(42), skipping close/finally.
Only this child is interrupted: no broker kill, disk failure, or real inventory.
Side effects are append-only local JSONL observations, not an atomic database.
"""
import argparse
import collections
import json
import os
import pathlib
import subprocess
import sys
import unittest
import time

PIN = "2.13.2"
SCENARIOS = ("read_only", "commit_first", "process_first", "process_then_commit")


def client():
    from importlib.metadata import version
    import confluent_kafka as ck
    if version("confluent-kafka") != PIN:
        raise RuntimeError("Prepare a venv with confluent-kafka==" + PIN)
    return ck


def fixtures():
    return [{"event_id": "evt-ch06-" + str(n), "order_id": "order-6001",
             "event_type": kind, "sequence": n, "schema_version": 1}
            for n, kind in enumerate(("order.created", "order.paid", "order.packed"))]


def save(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(json.dumps(data, indent=2) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def append_effect(path, stage, msg):
    """Record a simulated non-idempotent action; intentionally no deduplication."""
    row = {"stage": stage, "topic": msg.topic(), "partition": msg.partition(),
           "offset": msg.offset(), "event": json.loads(msg.value().decode("utf-8"))}
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    return row


def check_partition_result(parts, topic, offset):
    if parts is None or len(parts) != 1:
        raise RuntimeError("Expected a synchronous single-partition commit result")
    part = parts[0]
    error = getattr(part, "error", getattr(part, "err", None))
    if error or part.topic != topic or part.partition != 0 or part.offset != offset:
        raise RuntimeError("Commit failed or returned unexpected topic/partition/offset: " + repr(parts))


def commit_message(consumer, msg):
    result = consumer.commit(message=msg, asynchronous=False)
    check_partition_result(result, msg.topic(), msg.offset() + 1)
    return msg.offset() + 1


def config(bootstrap, group):
    return {"bootstrap.servers": bootstrap, "group.id": group,
            "group.protocol": "classic", "partition.assignment.strategy": "range",
            "enable.auto.commit": False, "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest", "allow.auto.create.topics": False,
            "session.timeout.ms": 6000, "heartbeat.interval.ms": 1000,
            "max.poll.interval.ms": 60000, "socket.timeout.ms": 10000}


def broker_offset(consumer, ck, topic):
    parts = consumer.committed([ck.TopicPartition(topic, 0)], timeout=10)
    if len(parts) != 1 or parts[0].error:
        raise RuntimeError("Could not query committed offset")
    value = parts[0].offset
    if value == ck.OFFSET_INVALID:
        return None
    if value < 0:
        raise RuntimeError("Unexpected logical committed offset " + str(value))
    return value


def worker(args):
    ck = client()
    consumer = ck.Consumer(config(args.bootstrap, args.group))
    report = {"scenario": args.scenario, "stage": args.stage, "group": args.group,
              "topic": args.topic, "pid": os.getpid(), "reads": [], "effects": [],
              "commits": [], "lifecycle": [], "closed": False}
    path = args.output / (args.stage + ".json")
    try:
        report["committed_at_start"] = broker_offset(consumer, ck, args.topic)
        if args.stage == "first" and report["committed_at_start"] is not None:
            raise RuntimeError("First phase requires a brand new group")

        def on_assign(c, parts):
            report["lifecycle"].append({"kind": "assign", "partitions": [p.partition for p in parts]})

        def on_revoke(c, parts):
            report["lifecycle"].append({"kind": "revoke", "partitions": [p.partition for p in parts]})

        consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            msg = consumer.poll(0.2)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            event = json.loads(msg.value().decode("utf-8"))
            if msg.topic() != args.topic or msg.partition() != 0 or event != fixtures()[msg.offset()]:
                raise RuntimeError("Unexpected record or offset in the fresh lab topic")
            report["reads"].append({"offset": msg.offset(), "event": event})
            if args.stage == "first":
                if msg.offset() != 0:
                    raise RuntimeError("First phase must read record zero")
                positions = consumer.position([ck.TopicPartition(args.topic, 0)])
                report["position_after_read"] = positions[0].offset
                report["committed_after_read"] = broker_offset(consumer, ck, args.topic)
                if args.scenario == "commit_first":
                    report["commits"].append(commit_message(consumer, msg))
                elif args.scenario in ("process_first", "process_then_commit"):
                    report["effects"].append(append_effect(args.output / "effects.jsonl", "first", msg))
                    if args.scenario == "process_then_commit":
                        report["commits"].append(commit_message(consumer, msg))
                report["committed_at_cut"] = broker_offset(consumer, ck, args.topic)
                report["intended_exit_code"] = 42
                save(path, report)
                # Precise failure point: do not call close(), finally, or a last auto commit.
                os._exit(42)
            report["effects"].append(append_effect(args.output / "effects.jsonl", "resume", msg))
            report["commits"].append(commit_message(consumer, msg))
            if msg.offset() == 2:
                report["committed_at_end"] = broker_offset(consumer, ck, args.topic)
                break
        else:
            raise RuntimeError("Consumer poll/rejoin deadline reached")
    finally:
        consumer.close()  # Auto commit is disabled; no unprocessed-position commit here.
        report["closed"] = True
        save(path, report)
    return 0


def inspect_committed(ck, bootstrap, group, topic):
    observer = ck.Consumer(config(bootstrap, group))
    try:
        # committed() only: this observer never subscribes, assigns, or joins the group.
        return broker_offset(observer, ck, topic)
    finally:
        observer.close()


def query_cli(compose, group, output, stage):
    command = compose + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                         "--bootstrap-server", "kafka:9092", "--describe", "--group", group]
    result = subprocess.run(command, capture_output=True, text=True, timeout=20)
    (output / (stage + ".cli.txt")).write_text(result.stdout + result.stderr, encoding="utf-8")
    if result.returncode:
        raise RuntimeError("CLI offset query failed")


def publish(ck, bootstrap, topic):
    producer = ck.Producer({"bootstrap.servers": bootstrap, "acks": "all",
                            "enable.idempotence": True, "message.timeout.ms": 15000})
    metadata = producer.list_topics(topic, timeout=10)
    entry = metadata.topics.get(topic)
    if not entry or entry.error or set(entry.partitions) != {0} or entry.partitions[0].replicas != [1]:
        raise RuntimeError("Expected fresh one-partition, one-replica lab topic")
    deliveries, errors = [], []
    for event in fixtures():
        def callback(err, msg, event_id=event["event_id"]):
            if err:
                errors.append(str(err))
            else:
                deliveries.append({"event_id": event_id, "partition": msg.partition(), "offset": msg.offset()})
        producer.produce(topic, partition=0, key=event["order_id"].encode(),
                         value=json.dumps(event).encode(), on_delivery=callback)
        producer.poll(0)
    if producer.flush(20) or errors or len(deliveries) != 3:
        raise RuntimeError("Not all three records were confirmed")
    if {d["event_id"]: d["offset"] for d in deliveries} != {e["event_id"]: n for n, e in enumerate(fixtures())}:
        raise RuntimeError("Topic was not empty or delivery coordinates differ")
    return deliveries


def replay(ck, bootstrap, topic):
    consumer = ck.Consumer(config(bootstrap, "ch06-audit-" + topic))
    rows = []
    try:
        low, high = consumer.get_watermark_offsets(ck.TopicPartition(topic, 0), timeout=10)
        if (low, high) != (0, 3):
            raise RuntimeError("Unexpected log boundary; expected source records remain at 0..2")
        consumer.assign([ck.TopicPartition(topic, 0, 0)])  # Audit only; not any recovery worker.
        deadline = time.monotonic() + 15
        while len(rows) < 3 and time.monotonic() < deadline:
            msg = consumer.poll(0.2)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(str(msg.error()))
            rows.append({"offset": msg.offset(), "event": json.loads(msg.value().decode())})
    finally:
        consumer.close()
    if rows != [{"offset": n, "event": e} for n, e in enumerate(fixtures())]:
        raise RuntimeError("Original records did not survive consumer commits")
    return rows


def verify_case(scenario, first, resumed, effects, observations):
    saved = 1 if scenario in ("commit_first", "process_then_commit") else None
    start = 1 if saved == 1 else 0
    expected_counts = {0: 0 if scenario == "commit_first" else 2 if scenario == "process_first" else 1,
                       1: 1, 2: 1}
    if first["committed_at_start"] is not None or first["committed_after_read"] is not None:
        raise ValueError("Reading must not implicitly commit a bookmark")
    if first["position_after_read"] != 1 or [r["offset"] for r in first["reads"]] != [0]:
        raise ValueError("Read position must advance to 1 after record 0")
    if first["closed"] or first["committed_at_cut"] != saved:
        raise ValueError("Incorrect failure point or commit state")
    if observations != {"before": None, "after_cut": saved, "after_resume": 3}:
        raise ValueError("Independent broker commit observations differ")
    if resumed["pid"] == first["pid"] or not resumed["closed"]:
        raise ValueError("Recovery must use a distinct process and close normally")
    if resumed["group"] != first["group"] or resumed["committed_at_start"] != saved:
        raise ValueError("Recovery must continue the same group's saved state")
    if [r["offset"] for r in resumed["reads"]] != list(range(start, 3)):
        raise ValueError("Recovery did not begin at the expected committed/fallback offset")
    if resumed["commits"] != list(range(start + 1, 4)) or resumed["committed_at_end"] != 3:
        raise ValueError("Recovery commits must save next offsets through 3")
    for row in effects:
        if row["partition"] != 0 or row["event"] != fixtures()[row["offset"]]:
            raise ValueError("Side-effect record differs from the consumed source")
    actual_counts = collections.Counter(row["offset"] for row in effects)
    if {n: actual_counts[n] for n in range(3)} != expected_counts:
        raise ValueError("Did not reproduce expected missing/duplicate simulated processing")
    return {"committed_after_cut": saved, "first_offset_after_restart": start,
            "final_committed_offset": 3,
            "processing_counts_by_offset": expected_counts,
            "first_process_reads": [r["offset"] for r in first["reads"]],
            "resumed_process_reads": [r["offset"] for r in resumed["reads"]]}


def invoke_worker(args, scenario, group, output, stage):
    command = [sys.executable, str(pathlib.Path(__file__).resolve()), "worker",
               "--bootstrap", args.bootstrap, "--topic", args.topic, "--group", group,
               "--scenario", scenario, "--stage", stage, "--output", str(output)]
    with (output / (stage + ".log")).open("w", encoding="utf-8") as stream:
        # Foreground child, no shell detachment; subprocess enforces a finite lifetime.
        proc = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, timeout=85)
    expected = 42 if stage == "first" else 0
    if proc.returncode != expected:
        raise RuntimeError("Worker " + stage + " exit=" + str(proc.returncode) + "; inspect its log")
    return json.loads((output / (stage + ".json")).read_text())


def run(args):
    ck = client()
    args.output.mkdir(parents=True, exist_ok=True)
    compose = [args.compose_command] if args.compose_command else ["docker", "compose"]
    compose += ["-f", str(args.compose_file)]
    save(args.output / "source.json", fixtures())
    save(args.output / "producer-deliveries.json", publish(ck, args.bootstrap, args.topic))
    summaries = {}
    for scenario in SCENARIOS:
        output = args.output / scenario
        output.mkdir(exist_ok=False)
        group = args.group_prefix + "." + scenario
        before = inspect_committed(ck, args.bootstrap, group, args.topic)
        first = invoke_worker(args, scenario, group, output, "first")
        after_cut = inspect_committed(ck, args.bootstrap, group, args.topic)
        query_cli(compose, group, output, "after_cut")
        resumed = invoke_worker(args, scenario, group, output, "resume")
        after_resume = inspect_committed(ck, args.bootstrap, group, args.topic)
        query_cli(compose, group, output, "after_resume")
        effects = [json.loads(line) for line in (output / "effects.jsonl").read_text().splitlines()]
        observations = {"before": before, "after_cut": after_cut, "after_resume": after_resume}
        save(output / "broker-committed.json", observations)
        summary = verify_case(scenario, first, resumed, effects, observations)
        summary["group"] = group
        summaries[scenario] = summary
        save(output / "summary.json", summary)
        print("PASS:", scenario, "restart begins at", summary["first_offset_after_restart"],
              "processing counts", summary["processing_counts_by_offset"], flush=True)
    save(args.output / "original-records-replay.json", replay(ck, args.bootstrap, args.topic))
    result = {"topic": args.topic, "produced_records": 3, "scenarios": summaries,
              "original_records_retained": True, "auto_commit": False, "auto_offset_store": False,
              "python_client": PIN, "librdkafka": ck.libversion()[0],
              "failure_injection": "Own consumer child os._exit(42), not broker/power failure",
              "side_effect": "Local append-only JSONL, no real inventory/database"}
    save(args.output / "summary.json", result)
    print(json.dumps(result, indent=2))
    print("PASS: Original records retained; all groups commit 3 even though early commit misses processing")


class FakePartition:
    topic = "t"
    partition = 0
    offset = 1
    error = None


class Tests(unittest.TestCase):
    def synthetic(self, scenario):
        saved = 1 if scenario in ("commit_first", "process_then_commit") else None
        start = 1 if saved else 0
        first = {"committed_at_start": None, "committed_after_read": None, "position_after_read": 1,
                 "reads": [{"offset": 0}], "closed": False, "committed_at_cut": saved,
                 "pid": 1, "group": "g"}
        resumed = {"pid": 2, "closed": True, "group": "g", "committed_at_start": saved,
                   "reads": [{"offset": n} for n in range(start, 3)],
                   "commits": list(range(start + 1, 4)), "committed_at_end": 3}
        offsets = ([0] if scenario in ("process_first", "process_then_commit") else []) + list(range(start, 3))
        effects = [{"offset": n, "partition": 0, "event": fixtures()[n]} for n in offsets]
        return first, resumed, effects, {"before": None, "after_cut": saved, "after_resume": 3}

    def test_read_only(self):
        verify_case("read_only", *self.synthetic("read_only"))

    def test_commit_first(self):
        result = verify_case("commit_first", *self.synthetic("commit_first"))
        self.assertEqual(result["processing_counts_by_offset"][0], 0)

    def test_process_first(self):
        result = verify_case("process_first", *self.synthetic("process_first"))
        self.assertEqual(result["processing_counts_by_offset"][0], 2)

    def test_process_then_commit(self):
        verify_case("process_then_commit", *self.synthetic("process_then_commit"))

    def test_wrong_restart_rejected(self):
        first, resumed, effects, observations = self.synthetic("commit_first")
        resumed["reads"] = [{"offset": 0}]
        with self.assertRaises(ValueError):
            verify_case("commit_first", first, resumed, effects, observations)

    def test_commit_next_offset(self):
        check_partition_result([FakePartition()], "t", 1)
        with self.assertRaises(RuntimeError):
            check_partition_result([FakePartition()], "t", 0)

    def test_commit_error_rejected(self):
        p = FakePartition()
        p.error = "coordinator error"
        with self.assertRaises(RuntimeError):
            check_partition_result([p], "t", 1)

    def test_no_sync_result_rejected(self):
        with self.assertRaises(RuntimeError):
            check_partition_result(None, "t", 1)

    def test_absent_commit_not_zero(self):
        first, resumed, effects, observations = self.synthetic("read_only")
        observations["after_cut"] = 0
        with self.assertRaises(ValueError):
            verify_case("read_only", first, resumed, effects, observations)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("self-test")
    for action in ("run", "worker"):
        p = sub.add_parser(action)
        p.add_argument("--bootstrap", default="127.0.0.1:19092")
        p.add_argument("--topic", required=True)
        p.add_argument("--output", required=True, type=pathlib.Path)
        if action == "run":
            p.add_argument("--group-prefix", required=True)
            p.add_argument("--compose-file", required=True, type=pathlib.Path)
            p.add_argument("--compose-command")
        else:
            p.add_argument("--group", required=True)
            p.add_argument("--scenario", choices=SCENARIOS, required=True)
            p.add_argument("--stage", choices=("first", "resume"), required=True)
    args = parser.parse_args()
    if args.action == "self-test":
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    try:
        if args.action == "worker":
            return worker(args)
        run(args)
        return 0
    except Exception as exc:
        print("FAIL:", str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
