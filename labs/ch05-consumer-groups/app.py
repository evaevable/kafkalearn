#!/usr/bin/env python3
"""Real classic Consumer Group experiment with 6 independent worker processes.

No offset commits or business side effects. Stable-stage evidence only;
cleanup member departures are not a rebalance/failure recovery experiment.
"""
import argparse
import collections
import copy
import json
import os
import pathlib
import subprocess
import sys
import time
import unittest

PIN = "2.13.2"
KEYS = {0: "order-3008", 1: "order-3001", 2: "order-3000"}
MEMBERS = {"inventory": 4, "analytics": 2}


def client():
    from importlib.metadata import version
    import confluent_kafka as ck
    if version("confluent-kafka") != PIN:
        raise RuntimeError("Install confluent-kafka==" + PIN + " in a venv first")
    return ck


def write_json(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(temporary, path)  # Readers never observe partial worker JSON.


def fixtures():
    return [{"partition": p, "offset": sequence, "key": KEYS[p],
             "event": {"event_id": KEYS[p] + "-ch05-" + str(sequence),
                       "order_id": KEYS[p], "event_type": kind,
                       "sequence": sequence, "schema_version": 1}}
            for sequence, kind in enumerate(("order.created", "order.paid",
                                             "order.packed", "order.shipped"))
            for p in range(3)]


def validate_assignment(group, expected_clients, topic):
    if group["state"] != "STABLE" or group["assignor"] != "range":
        raise ValueError("Group is not stable with the expected range assignor")
    members = group["members"]
    names = [member["client_id"] for member in members]
    if set(names) != set(expected_clients) or len(names) != len(expected_clients):
        raise ValueError("Unexpected or missing group members")
    assigned = [(part["topic"], part["partition"]) for member in members for part in member["assignment"]]
    if sorted(assigned) != [(topic, p) for p in range(3)]:
        raise ValueError("Partitions must be completely covered once, without overlap")
    return {member["client_id"]: sorted(part["partition"] for part in member["assignment"])
            for member in members}


def validate_readings(group, reports, topic):
    assignment = validate_assignment(group, reports.keys(), topic)
    wanted = fixtures()
    observed = []
    for name, report in reports.items():
        if report["error"] or report["assignment"] != assignment[name]:
            raise ValueError("Worker failed or assignment changed during the stable-stage check")
        if any(row["partition"] not in assignment[name] for row in report["records"]):
            raise ValueError("Record was read by a worker not assigned that partition")
        observed.extend(report["records"])
    if len(observed) != len(wanted):
        raise ValueError("Expected 12 reads in each group")
    if sorted(observed, key=lambda row: (row["partition"], row["offset"])) != sorted(
            wanted, key=lambda row: (row["partition"], row["offset"])):
        raise ValueError("Missing, duplicate, or incorrect key/coordinate/event")
    # Check order as actually observed per worker, not by sorting away errors.
    for p in range(3):
        owner = next(name for name, parts in assignment.items() if p in parts)
        rows = [r for r in reports[owner]["records"] if r["partition"] == p]
        if [r["offset"] for r in rows] != [0, 1, 2, 3]:
            raise ValueError("Partition order is not 0,1,2,3")
    return {"member_count": len(reports), "total_reads": len(observed),
            "worker_counts": {name: len(report["records"]) for name, report in reports.items()},
            "assignment": assignment}


def worker(args):
    ck = client()
    report = {"client_id": args.name, "group_id": args.group, "topic": args.topic, "pid": os.getpid(),
              "assignment": [], "lifecycle": [], "records": [], "error": None,
              "closed": False, "poll_count": 0}
    consumer = ck.Consumer({"bootstrap.servers": args.bootstrap,
                            "group.id": args.group, "client.id": args.name,
                            "group.protocol": "classic", "partition.assignment.strategy": "range",
                            "auto.offset.reset": "earliest", "enable.auto.commit": False,
                            "enable.auto.offset.store": False, "allow.auto.create.topics": False,
                            "session.timeout.ms": 10000, "heartbeat.interval.ms": 1000,
                            "max.poll.interval.ms": 60000})

    def assigned(c, partitions):
        report["lifecycle"].append({"kind": "assign", "partitions": [p.partition for p in partitions]})
        # Let librdkafka do its default assignment; do not turn subscribe into manual assign.

    def revoked(c, partitions):
        report["lifecycle"].append({"kind": "revoke", "partitions": [p.partition for p in partitions]})

    try:
        consumer.subscribe([args.topic], on_assign=assigned, on_revoke=revoked)
        deadline = time.monotonic() + 180
        while not args.stop.exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("Worker lifetime limit reached")
            message = consumer.poll(0.1)
            report["poll_count"] += 1
            report["assignment"] = sorted(p.partition for p in consumer.assignment())
            if message is not None:
                if message.error():
                    raise RuntimeError(str(message.error()))
                if message.topic() != args.topic:
                    raise RuntimeError("Received unexpected topic")
                report["records"].append({"partition": message.partition(), "offset": message.offset(),
                                          "key": message.key().decode("utf-8"),
                                          "event": json.loads(message.value().decode("utf-8"))})
            write_json(args.report, report)
    except Exception as exc:
        report["error"] = str(exc)
    finally:
        consumer.close()  # Auto commit disabled; no commit() call anywhere in this lab.
        report["closed"] = True
        write_json(args.report, report)
    return 1 if report["error"] else 0


def describe_groups(admin, group_ids):
    futures = admin.describe_consumer_groups(list(group_ids.values()), request_timeout=5)
    result = {}
    for label, group_id in group_ids.items():
        description = futures[group_id].result(timeout=6)
        result[label] = {"group_id": description.group_id, "state": description.state.name,
                         "assignor": description.partition_assignor,
                         "coordinator_id": description.coordinator.id,
                         "members": [{"member_id": member.member_id, "client_id": member.client_id,
                                      "host": member.host,
                                      "assignment": [{"topic": p.topic, "partition": p.partition}
                                                     for p in member.assignment.topic_partitions]}
                                     for member in description.members]}
    return result


def read_reports(paths):
    return {label: {name: json.loads(path.read_text(encoding="utf-8")) for name, path in members.items()}
            for label, members in paths.items()}


def ensure_workers(processes):
    for name, proc in processes.items():
        if proc.poll() is not None:
            raise RuntimeError("Worker exited prematurely: " + name)


def wait_stable(admin, group_ids, paths, processes, topic):
    deadline = time.monotonic() + 60
    previous = None
    last_error = "not started"
    while time.monotonic() < deadline:
        ensure_workers(processes)
        try:
            groups = describe_groups(admin, group_ids)
            reports = read_reports(paths)
            signature = {}
            for label in MEMBERS:
                assignments = validate_assignment(groups[label], paths[label].keys(), topic)
                signature[label] = assignments
                for name, report in reports[label].items():
                    if report["error"] or report["assignment"] != assignments[name] or not report["poll_count"]:
                        raise ValueError("Worker poll/assignment not ready")
                    if report["records"]:
                        raise ValueError("Fresh lab topic unexpectedly contains records")
            if signature == previous:
                return groups
            previous = signature
        except Exception as exc:
            previous = None
            last_error = str(exc)
        # Actual IPC/group convergence needs paced checks; no fixed startup sleep.
        time.sleep(0.1)
    raise RuntimeError("Group convergence deadline: " + last_error)


def capture_cli(compose, group_ids, output, stage):
    for label, group_id in group_ids.items():
        base = compose + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                          "--bootstrap-server", "kafka:9092", "--describe", "--group", group_id]
        for suffix, options in (("members", ["--members", "--verbose"]), ("state", ["--state"])):
            result = subprocess.run(base + options, capture_output=True, text=True, timeout=20)
            (output / (stage + "." + label + "." + suffix + ".txt")).write_text(
                result.stdout + result.stderr, encoding="utf-8")
            if result.returncode:
                raise RuntimeError("CLI group inspection failed; result retained")


def publish(ck, bootstrap, topic):
    producer = ck.Producer({"bootstrap.servers": bootstrap, "partitioner": "murmur2_random",
                            "acks": "all", "enable.idempotence": True, "message.timeout.ms": 15000})
    deliveries, errors = [], []
    for record in fixtures():
        def callback(err, msg, event_id=record["event"]["event_id"]):
            if err is not None:
                errors.append(str(err))
            else:
                deliveries.append({"event_id": event_id, "partition": msg.partition(), "offset": msg.offset()})
        producer.produce(topic, key=record["key"].encode("utf-8"),
                         value=json.dumps(record["event"], separators=(",", ":")).encode("utf-8"),
                         on_delivery=callback)
        producer.poll(0)
    remaining = producer.flush(20)
    if remaining or errors or len(deliveries) != 12:
        raise RuntimeError("Producer did not confirm all twelve records")
    expected = {r["event"]["event_id"]: (r["partition"], r["offset"]) for r in fixtures()}
    actual = {r["event_id"]: (r["partition"], r["offset"]) for r in deliveries}
    if expected != actual:
        raise RuntimeError("Producer key route or coordinates differ from expected")
    return deliveries


def run(args):
    ck = client()
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    metadata = admin.list_topics(args.topic, timeout=10)
    entry = metadata.topics.get(args.topic)
    if not entry or entry.error or set(entry.partitions) != {0, 1, 2} or any(
            p.leader != 1 or p.replicas != [1] for p in entry.partitions.values()):
        raise RuntimeError("Create a 3-partition/1-replica local topic before running")
    args.output.mkdir(parents=True, exist_ok=True)
    group_ids = {label: args.group_prefix + "." + label for label in MEMBERS}
    paths = {label: {label + "-" + str(n + 1): args.output / (label + "-" + str(n + 1) + ".json")
                     for n in range(count)} for label, count in MEMBERS.items()}
    processes, logs = {}, []
    stop = args.output / "stop-workers"
    if stop.exists() or any(path.exists() for members in paths.values() for path in members.values()):
        raise RuntimeError("Use a new output directory/group/topic for each run")
    compose = [args.compose_command] if args.compose_command else ["docker", "compose"]
    compose += ["-f", str(args.compose_file)]
    result = None
    try:
        for label, members in paths.items():
            for name, path in members.items():
                log = (args.output / (name + ".log")).open("w", encoding="utf-8")
                logs.append(log)
                command = [sys.executable, str(pathlib.Path(__file__).resolve()), "worker",
                           "--bootstrap", args.bootstrap, "--topic", args.topic,
                           "--group", group_ids[label], "--name", name,
                           "--report", str(path), "--stop", str(stop)]
                processes[name] = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        before = wait_stable(admin, group_ids, paths, processes, args.topic)
        write_json(args.output / "before.groups.json", before)
        capture_cli(compose, group_ids, args.output, "before")
        current = describe_groups(admin, group_ids)
        for label in MEMBERS:
            if validate_assignment(before[label], paths[label], args.topic) != validate_assignment(
                    current[label], paths[label], args.topic):
                raise RuntimeError("Assignment changed before publish")
        baseline_reports = read_reports(paths)
        for label in MEMBERS:
            assignments = validate_assignment(current[label], paths[label], args.topic)
            if any(report["assignment"] != assignments[name] or report["error"] or report["records"]
                   for name, report in baseline_reports[label].items()):
                raise RuntimeError("Workers no longer match the ready-to-publish stable state")
        write_json(args.output / "before.worker-reports.json", baseline_reports)
        print("PASS: Stable subscriptions: inventory 4 members, analytics 2 members, complete exclusive partition coverage")
        deliveries = publish(ck, args.bootstrap, args.topic)
        write_json(args.output / "producer-deliveries.json", deliveries)
        deadline = time.monotonic() + 40
        while True:
            ensure_workers(processes)
            reports = read_reports(paths)
            for members in reports.values():
                if any(report["error"] for report in members.values()):
                    raise RuntimeError("Worker reported an error")
            counts = {label: sum(len(report["records"]) for report in members.values())
                      for label, members in reports.items()}
            if any(n > 12 for n in counts.values()):
                raise RuntimeError("Duplicate reads in stable-stage experiment")
            if all(n == 12 for n in counts.values()):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Consumption deadline: " + repr(counts))
            time.sleep(0.1)
        capture_cli(compose, group_ids, args.output, "after")
        after = describe_groups(admin, group_ids)
        reports = read_reports(paths)
        summaries = {}
        for label in MEMBERS:
            if validate_assignment(before[label], paths[label], args.topic) != validate_assignment(
                    after[label], paths[label], args.topic):
                raise RuntimeError("Assignment changed during the published-record stage")
            if any(report["lifecycle"] != baseline_reports[label][name]["lifecycle"]
                   for name, report in reports[label].items()):
                raise RuntimeError("A rebalance occurred during the record stage; not stable-stage evidence")
            summaries[label] = validate_readings(after[label], reports[label], args.topic)
        if sorted(summaries["inventory"]["worker_counts"].values()) != [0, 4, 4, 4]:
            raise RuntimeError("Expected one idle inventory member")
        if sorted(summaries["analytics"]["worker_counts"].values()) != [4, 8]:
            raise RuntimeError("Expected analytics 1/2 partition split")
        write_json(args.output / "after.groups.json", after)
        write_json(args.output / "stable.worker-reports.json", reports)
        result = {"topic": args.topic, "produced_records": 12, "total_reads_across_groups": 24,
                  "group_protocol": "classic", "assignor": "range", "offset_commits": False,
                  "groups": summaries, "python_client": PIN, "librdkafka": ck.libversion()[0]}
        write_json(args.output / "summary.json", result)
        print("PASS: Each group independently read all 12 records; group-internal records are complete and non-overlapping")
        print("PASS: inventory counts 4/4/4/0; analytics counts 8/4; actual member names may vary")
        print(json.dumps(result, indent=2))
    finally:
        stop.touch()
        cleanup_errors = []
        for name, proc in processes.items():
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
                cleanup_errors.append(name + ": did not close within deadline")
            if proc.returncode != 0:
                cleanup_errors.append(name + ": exit=" + str(proc.returncode))
        for log in logs:
            log.close()
        if cleanup_errors:
            raise RuntimeError("Worker cleanup errors: " + repr(cleanup_errors))
    if result is None:
        raise RuntimeError("Experiment did not finish")
    print("PASS: Workers closed; cleanup departures are not a failure-recovery test")


class Tests(unittest.TestCase):
    def group(self):
        return {"state": "STABLE", "assignor": "range", "members": [
            {"client_id": "w" + str(p), "assignment": [{"topic": "t", "partition": p}]}
            for p in range(3)] + [{"client_id": "w3", "assignment": []}]}

    def reports(self):
        return {"w" + str(p): {"error": None, "assignment": [p] if p < 3 else [],
                               "records": [r for r in fixtures() if r["partition"] == p]}
                for p in range(4)}

    def test_fixtures(self):
        self.assertEqual(len({r["event"]["event_id"] for r in fixtures()}), 12)

    def test_idle_member(self):
        summary = validate_readings(self.group(), self.reports(), "t")
        self.assertEqual(sorted(summary["worker_counts"].values()), [0, 4, 4, 4])

    def test_overlap_rejected(self):
        group = self.group()
        group["members"][3]["assignment"] = [{"topic": "t", "partition": 0}]
        with self.assertRaises(ValueError):
            validate_assignment(group, self.reports(), "t")

    def test_missing_partition_rejected(self):
        group = self.group()
        group["members"][0]["assignment"] = []
        with self.assertRaises(ValueError):
            validate_assignment(group, self.reports(), "t")

    def test_duplicate_rejected(self):
        reports = self.reports()
        reports["w0"]["records"][1] = copy.deepcopy(reports["w0"]["records"][0])
        with self.assertRaises(ValueError):
            validate_readings(self.group(), reports, "t")

    def test_wrong_owner_rejected(self):
        reports = self.reports()
        reports["w3"]["records"] = [reports["w0"]["records"].pop()]
        with self.assertRaises(ValueError):
            validate_readings(self.group(), reports, "t")

    def test_reversed_order_rejected(self):
        reports = self.reports()
        reports["w0"]["records"].reverse()
        with self.assertRaises(ValueError):
            validate_readings(self.group(), reports, "t")

    def test_unstable_group_rejected(self):
        group = self.group()
        group["state"] = "PREPARING_REBALANCING"
        with self.assertRaises(ValueError):
            validate_assignment(group, self.reports(), "t")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("self-test")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--bootstrap", default="127.0.0.1:19092")
    run_parser.add_argument("--topic", required=True)
    run_parser.add_argument("--group-prefix", required=True)
    run_parser.add_argument("--output", required=True, type=pathlib.Path)
    run_parser.add_argument("--compose-file", required=True, type=pathlib.Path)
    run_parser.add_argument("--compose-command", help="Standalone docker-compose binary; default uses docker compose")
    worker_parser = sub.add_parser("worker")
    worker_parser.add_argument("--bootstrap", required=True)
    worker_parser.add_argument("--topic", required=True)
    worker_parser.add_argument("--group", required=True)
    worker_parser.add_argument("--name", required=True)
    worker_parser.add_argument("--report", required=True, type=pathlib.Path)
    worker_parser.add_argument("--stop", required=True, type=pathlib.Path)
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
