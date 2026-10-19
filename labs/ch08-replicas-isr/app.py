#!/usr/bin/env python3
"""Chapter 08 lab: three KRaft brokers, replicas, leader election, ISR and acks.

The script stops exactly ONE broker (the leader of partition 0) with a graceful
container stop, keeps the 2-of-3 controller quorum alive, then restarts it.
It is not a power-loss, disk-failure, or two-broker outage test.
"""
import argparse
import json
import os
import pathlib
import subprocess
import sys
import time
import unittest

PIN = "2.13.2"
BROKERS = (1, 2, 3)


def client():
    from importlib.metadata import version
    import confluent_kafka as ck
    if version("confluent-kafka") != PIN:
        raise RuntimeError("Prepare a venv with confluent-kafka==" + PIN)
    return ck


def save(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def main_records(tag, per_partition):
    rows = []
    for p in range(3):
        for n in range(per_partition):
            event_id = "evt-ch08-%s-p%d-%d" % (tag, p, n)
            rows.append({"partition": p, "event_id": event_id,
                         "event": {"event_id": event_id, "order_id": "order-800%d" % p,
                                   "event_type": "order.created", "tag": tag}})
    return rows


def small_records(tag):
    event_id = "evt-ch08-small-" + tag
    return [{"partition": 0, "event_id": event_id,
             "event": {"event_id": event_id, "order_id": "order-8009",
                       "event_type": "order.created", "tag": tag}}]


def validate_baseline(described, broker_ids=BROKERS):
    main = described["main"]["partitions"]
    if sorted(main) != ["0", "1", "2"]:
        raise ValueError("Main topic must have three partitions")
    for p in main.values():
        if sorted(p["replicas"]) != sorted(broker_ids) or sorted(p["isr"]) != sorted(broker_ids):
            raise ValueError("Each main partition needs replicas and ISR on all three brokers")
        if p["leader"] not in broker_ids:
            raise ValueError("Unknown leader id")
    small = described["small"]["partitions"]
    if sorted(small) != ["0"]:
        raise ValueError("Small topic must have one partition")
    part = small["0"]
    if len(part["replicas"]) != 2 or len(part["isr"]) != 2:
        raise ValueError("Small topic must start with two replicas in ISR")
    return part


def validate_after_stop(described, stopped, remaining):
    main = described["main"]["partitions"]
    for p in main.values():
        if p["leader"] == stopped:
            raise ValueError("Stopped broker must not lead any main partition")
        if sorted(p["isr"]) != sorted(remaining):
            raise ValueError("Main ISR must shrink to the two surviving brokers")
    small = described["small"]["partitions"]["0"]
    if stopped not in small["replicas"]:
        raise ValueError("Small topic replica set must include the stopped broker")
    expected_small = sorted(b for b in small["replicas"] if b != stopped)
    if sorted(small["isr"]) != expected_small:
        raise ValueError("Small topic ISR must shrink to its surviving replica")
    return {"new_leader_p0": main["0"]["leader"], "main_isr_size": len(main["0"]["isr"]),
            "small_isr": sorted(small["isr"])}


def expect_small_write_rejected(delivered, errors, remaining):
    if delivered:
        raise ValueError("Small topic should reject writes when its ISR is below min.insync.replicas")
    if not errors:
        raise ValueError("Expected a delivery error, not a silent success or hang")
    text = " ".join(str(item.get("error", "")) for item in errors)
    if "NOT_ENOUGH_REPLICAS" not in text:
        raise ValueError("Expected NOT_ENOUGH_REPLICAS, got: " + text)
    return text


def validate_recovered(described):
    main = described["main"]["partitions"]
    if any(sorted(p["isr"]) != sorted(BROKERS) for p in main.values()):
        raise ValueError("Main ISR must return to three brokers")
    small = described["small"]["partitions"]["0"]
    if len(small["isr"]) != 2:
        raise ValueError("Small topic ISR must return to two replicas")
    return {"main_isr_size": len(main["0"]["isr"]), "small_isr": sorted(small["isr"])}


def create_topic(admin, name, partitions=None, replication=None, replica_assignment=None, config=None):
    from confluent_kafka.admin import NewTopic
    if replica_assignment is not None:
        # This client requires an explicit partition count alongside the assignment.
        topic = NewTopic(name, num_partitions=len(replica_assignment),
                         replica_assignment=replica_assignment, config=config or {})
    else:
        topic = NewTopic(name, num_partitions=partitions, replication_factor=replication,
                         config=config or {})
    admin.create_topics([topic], request_timeout=20)[name].result(timeout=30)


def describe_topic(ck, bootstrap, name):
    # A fresh producer forces a fresh metadata fetch (including leader and ISR).
    producer = ck.Producer({"bootstrap.servers": bootstrap, "socket.timeout.ms": 10000})
    metadata = producer.list_topics(timeout=10)
    topic = metadata.topics.get(name)
    if topic is None or topic.error is not None:
        raise RuntimeError("Topic metadata unavailable: " + name)
    return {"topic": name,
            "partitions": {str(pid): {"leader": part.leader,
                                      "replicas": sorted(part.replicas),
                                      "isr": sorted(part.isrs)}
                           for pid, part in topic.partitions.items()}}


def wait_until(check, deadline_seconds, label):
    last_error = "not started"
    until = time.monotonic() + deadline_seconds
    while time.monotonic() < until:
        try:
            result = check()
            if result:
                return result
        except Exception as exc:
            last_error = str(exc)
        time.sleep(0.5)
    raise RuntimeError("Timed out waiting for " + label + ": " + last_error)


def produce(ck, bootstrap, topic, records, idempotent=True, retries=None):
    configuration = {"bootstrap.servers": bootstrap, "acks": "all",
                     "enable.idempotence": idempotent, "message.timeout.ms": 8000,
                     "socket.timeout.ms": 10000, "partitioner": "murmur2_random"}
    if retries is not None:
        configuration["retries"] = retries
    producer = ck.Producer(configuration)
    delivered, errors = [], []
    for record in records:
        def callback(err, msg, event_id=record["event_id"]):
            if err is not None:
                errors.append({"event_id": event_id, "error": str(err)})
            else:
                delivered.append({"event_id": event_id, "partition": msg.partition(),
                                  "offset": msg.offset()})
        producer.produce(topic, partition=record["partition"],
                         key=record["event"]["order_id"].encode("utf-8"),
                         value=json.dumps(record["event"]).encode("utf-8"),
                         on_delivery=callback)
        producer.poll(0)
    remaining = producer.flush(15)
    return delivered, errors, remaining


def read_all(ck, bootstrap, topic, expected, partitions=3, deadline=30):
    consumer = ck.Consumer({"bootstrap.servers": bootstrap,
                            "group.id": "ch08-audit-" + topic,
                            "enable.auto.commit": False, "enable.auto.offset.store": False,
                            "auto.offset.reset": "earliest", "allow.auto.create.topics": False,
                            "socket.timeout.ms": 10000})
    rows = []
    try:
        consumer.assign([ck.TopicPartition(topic, p, 0) for p in range(partitions)])
        until = time.monotonic() + deadline
        while len(rows) < expected and time.monotonic() < until:
            message = consumer.poll(0.3)
            if message is None:
                continue
            if message.error():
                if message.error().code() == ck.KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))
            rows.append({"partition": message.partition(), "offset": message.offset(),
                         "event_id": json.loads(message.value().decode("utf-8"))["event_id"]})
    finally:
        consumer.close()
    return rows


def compose(args, *command):
    base = [args.compose_command] if args.compose_command else ["docker", "compose"]
    base += ["-f", str(args.compose_file)] + list(command)
    result = subprocess.run(base, capture_output=True, text=True, timeout=180)
    if result.returncode:
        raise RuntimeError("Compose command failed: " + " ".join(command)
                           + "\n" + result.stdout + result.stderr)
    return (result.stdout + result.stderr).strip()


def run(args):
    ck = client()
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": args.bootstrap})

    def describe(name):
        return describe_topic(ck, args.bootstrap, name)

    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    if (output / "summary.json").exists():
        raise RuntimeError("Use a fresh output directory for each run")
    main_topic = args.prefix + ".main"
    small_topic = args.prefix + ".small"
    stopped, restarted = None, False
    try:
        create_topic(admin, main_topic, 3, 3)

        def main_ready():
            described = describe(main_topic)
            return described if len(described["partitions"]) == 3 else None

        baseline = wait_until(main_ready, 30, "main topic metadata")
        leader = baseline["partitions"]["0"]["leader"]
        other = next(b for b in BROKERS if b != leader)
        create_topic(admin, small_topic, -1, -1, replica_assignment=[[leader, other]],
                     config={"min.insync.replicas": "2"})
        described = {"main": describe(main_topic),
                     "small": describe(small_topic)}
        small_part = validate_baseline(described)
        save(output / "baseline.json", described)
        print("PASS: main replicas", described["main"]["partitions"]["0"]["replicas"],
              "ISR", described["main"]["partitions"]["0"]["isr"],
              "| small replicas", small_part["replicas"], flush=True)

        delivered, errors, remaining = produce(ck, args.bootstrap, main_topic, main_records("before", 2))
        if remaining or errors or len(delivered) != 6:
            raise RuntimeError("Six baseline records must be acknowledged by acks=all")
        small_ok, small_errors, small_remaining = produce(ck, args.bootstrap, small_topic,
                                                          small_records("before"))
        if small_remaining or small_errors or len(small_ok) != 1:
            raise RuntimeError("Baseline small-topic write must succeed")
        before_main = read_all(ck, args.bootstrap, main_topic, 6, partitions=3)
        before_small = read_all(ck, args.bootstrap, small_topic, 1, partitions=1)
        save(output / "before.json", {"main_reads": before_main, "small_reads": before_small})
        print("PASS: before failure main=6 small=1 records readable", flush=True)

        stop_start = time.monotonic()
        compose(args, "stop", "kafka-%d" % leader)
        stopped = leader
        remaining_ids = sorted(b for b in BROKERS if b != leader)
        stopped_state = wait_until(
            lambda: validate_after_stop(
                {"main": describe(main_topic),
                 "small": describe(small_topic)}, leader, remaining_ids),
            90, "leader election and ISR shrink")
        election_seconds = time.monotonic() - stop_start
        save(output / "after_stop.json", {"describe": {"main": describe(main_topic),
                                                       "small": describe(small_topic)},
                                          "stopped_broker": leader,
                                          "election_seconds": round(election_seconds, 2)})
        print("PASS: broker", leader, "stopped; new leader for P0 is",
              stopped_state["new_leader_p0"], "after", round(election_seconds, 2), "s", flush=True)

        delivered2, errors2, remaining2 = produce(ck, args.bootstrap, main_topic, main_records("after", 1))
        if remaining2 or errors2 or len(delivered2) != 3:
            raise RuntimeError("Main topic must keep accepting acks=all writes with 2 of 3 replicas")
        small_bad, small_bad_errors, small_bad_remaining = produce(
            ck, args.bootstrap, small_topic, small_records("blocked"), idempotent=False, retries=0)
        error_text = expect_small_write_rejected(small_bad, small_bad_errors, small_bad_remaining)
        after_main = read_all(ck, args.bootstrap, main_topic, 9, partitions=3)
        after_small = read_all(ck, args.bootstrap, small_topic, 1, partitions=1)
        save(output / "during_outage.json", {"main_reads": after_main, "small_reads": after_small,
                                             "small_write_error": error_text,
                                             "main_writes_accepted": len(delivered2)})
        print("PASS: main still writable and readable (9 records); small write rejected:", error_text, flush=True)

        restart_start = time.monotonic()
        compose(args, "start", "kafka-%d" % leader)
        restarted = True
        recovered = wait_until(
            lambda: validate_recovered({"main": describe(main_topic),
                                        "small": describe(small_topic)}),
            120, "ISR recovery")
        recovery_seconds = time.monotonic() - restart_start
        final_small, final_errors, final_remaining = produce(ck, args.bootstrap, small_topic,
                                                             small_records("after"))
        if final_remaining or final_errors or len(final_small) != 1:
            raise RuntimeError("Small topic must accept writes after the replica returns")
        final_small_reads = read_all(ck, args.bootstrap, small_topic, 2, partitions=1)
        save(output / "recovered.json", {"describe": {"main": describe(main_topic),
                                                      "small": describe(small_topic)},
                                         "recovery_seconds": round(recovery_seconds, 2),
                                         "small_reads": final_small_reads})
        print("PASS: broker", leader, "rejoined; ISR restored in", round(recovery_seconds, 2), "s", flush=True)

        summary = {"topics": {"main": main_topic, "small": small_topic},
                   "stopped_broker": leader, "surviving_brokers": remaining_ids,
                   "timings": {"leader_election_seconds": round(election_seconds, 2),
                               "isr_recovery_seconds": round(recovery_seconds, 2)},
                   "counts": {"main_before": len(before_main), "main_during": len(after_main),
                              "small_before": len(before_small), "small_during": len(after_small),
                              "small_final": len(final_small_reads)},
                   "small_write_error": error_text,
                   "python_client": PIN, "librdkafka": ck.libversion()[0]}
        save(output / "summary.json", summary)
        print(json.dumps(summary, indent=2))
        print("PASS: chapter 08 observations complete")
    finally:
        if stopped is not None and not restarted:
            try:
                compose(args, "start", "kafka-%d" % stopped)
            except Exception as exc:
                print("CLEANUP: could not restart broker", stopped, exc, file=sys.stderr)


class Tests(unittest.TestCase):
    def baseline(self):
        return {"main": {"partitions": {str(p): {"leader": p + 1, "replicas": [1, 2, 3],
                                                 "isr": [1, 2, 3]} for p in range(3)}},
                "small": {"partitions": {"0": {"leader": 1, "replicas": [1, 2], "isr": [1, 2]}}}}

    def test_baseline_ok(self):
        self.assertEqual(validate_baseline(self.baseline())["replicas"], [1, 2])

    def test_baseline_missing_isr_rejected(self):
        described = self.baseline()
        described["main"]["partitions"]["1"]["isr"] = [1, 2]
        with self.assertRaises(ValueError):
            validate_baseline(described)

    def test_after_stop_ok(self):
        described = {"main": {"partitions": {str(p): {"leader": (p % 2) + 2,
                                                      "replicas": [1, 2, 3], "isr": [2, 3]}
                                             for p in range(3)}},
                     "small": {"partitions": {"0": {"leader": 2, "replicas": [1, 2], "isr": [2]}}}}
        out = validate_after_stop(described, 1, [2, 3])
        self.assertEqual(out["small_isr"], [2])

    def test_stopped_leader_rejected(self):
        described = {"main": {"partitions": {"0": {"leader": 1, "replicas": [1, 2, 3], "isr": [2, 3]}}},
                     "small": {"partitions": {"0": {"leader": 2, "replicas": [1, 2], "isr": [2]}}}}
        with self.assertRaises(ValueError):
            validate_after_stop(described, 1, [2, 3])

    def test_small_write_rejected(self):
        text = expect_small_write_rejected([], [{"error": "Broker: Not enough in-sync replicas "
                                                          "(NOT_ENOUGH_REPLICAS)"}], 0)
        self.assertIn("NOT_ENOUGH_REPLICAS", text)

    def test_silent_success_rejected(self):
        with self.assertRaises(ValueError):
            expect_small_write_rejected([{"event_id": "x"}], [], 0)
        with self.assertRaises(ValueError):
            expect_small_write_rejected([], [{"error": "Local: Message timed out"}], 0)

    def test_recovered(self):
        described = {"main": {"partitions": {str(p): {"isr": [1, 2, 3]} for p in range(3)}},
                     "small": {"partitions": {"0": {"isr": [1, 2]}}}}
        self.assertEqual(validate_recovered(described)["main_isr_size"], 3)

    def test_fixtures(self):
        self.assertEqual(len(main_records("before", 2)), 6)
        self.assertEqual([r["partition"] for r in main_records("after", 1)], [0, 1, 2])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("self-test")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--bootstrap", default="127.0.0.1:19192,127.0.0.1:29292,127.0.0.1:39392")
    run_parser.add_argument("--prefix", required=True)
    run_parser.add_argument("--output", required=True, type=pathlib.Path)
    run_parser.add_argument("--compose-file", required=True, type=pathlib.Path)
    run_parser.add_argument("--compose-command")
    args = parser.parse_args()
    if args.action == "self-test":
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    try:
        run(args)
        return 0
    except Exception as exc:
        print("FAIL:", str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
