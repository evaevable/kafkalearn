#!/usr/bin/env python3
"""Real classic-group rebalance lab: member join, hard crash, graceful leave.

Only this lab's own consumer children are interrupted (no broker kill, no
topic deletion). Records are read and reported, never committed; the goal is
to observe assignment handover, rebalance timing, and re-reading after a
handover without a bookmark.
"""
import argparse
import collections
import json
import os
import pathlib
import subprocess
import sys
import time
import unittest

PIN = "2.13.2"
PARTITIONS = 3
WORKERS = ("member-a", "member-b", "member-b2")


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


def fixtures():
    return [{"partition": p, "offset": n,
             "event": {"event_id": "evt-ch07-p%d-%d" % (p, n),
                       "order_id": "order-700%d" % p,
                       "event_type": ("order.created", "order.paid")[n],
                       "sequence": n, "schema_version": 1}}
            for p in range(PARTITIONS) for n in range(2)]


def worker_config(bootstrap, group, name):
    return {"bootstrap.servers": bootstrap, "group.id": group, "client.id": name,
            "group.protocol": "classic", "partition.assignment.strategy": "range",
            "enable.auto.commit": False, "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest", "allow.auto.create.topics": False,
            "session.timeout.ms": 6000, "heartbeat.interval.ms": 1000,
            "max.poll.interval.ms": 30000, "socket.timeout.ms": 10000}


def worker(args):
    ck = client()
    consumer = ck.Consumer(worker_config(args.bootstrap, args.group, args.name))
    control = args.control
    crash_file = control / ("crash-" + args.name)
    stop_file = control / ("stop-" + args.name)
    report = {"name": args.name, "group": args.group, "topic": args.topic,
              "pid": os.getpid(), "lifecycle": [], "records": [],
              "assignment": [], "closed": False, "hard_exit": False, "error": None}
    save(args.report, report)

    def on_assign(current, parts):
        report["lifecycle"].append({"kind": "assign",
                                    "partitions": sorted(p.partition for p in parts),
                                    "t": time.time()})
        save(args.report, report)

    def on_revoke(current, parts):
        report["lifecycle"].append({"kind": "revoke",
                                    "partitions": sorted(p.partition for p in parts),
                                    "t": time.time()})
        save(args.report, report)

    try:
        consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)
        while True:
            if crash_file.exists():
                report["hard_exit"] = True
                save(args.report, report)
                os._exit(42)  # Simulated sudden death: no close(), no finally.
            if stop_file.exists():
                break
            message = consumer.poll(0.1)
            current = sorted(p.partition for p in consumer.assignment())
            if current != report["assignment"]:
                report["assignment"] = current
                save(args.report, report)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            if message.topic() != args.topic:
                raise RuntimeError("Unexpected topic")
            report["records"].append({"partition": message.partition(),
                                      "offset": message.offset(),
                                      "event": json.loads(message.value().decode("utf-8")),
                                      "t": time.time()})
            save(args.report, report)
    except Exception as exc:
        report["error"] = str(exc)
    finally:
        if not report["hard_exit"]:
            try:
                consumer.close()  # Graceful path: sends leave, triggers fast rebalance.
            except Exception as exc:
                report["error"] = report["error"] or str(exc)
            report["closed"] = report["error"] is None
        save(args.report, report)
    return 1 if report["error"] else 0


def describe_group(admin, group):
    future = admin.describe_consumer_groups([group], request_timeout=5)[group]
    description = future.result(timeout=6)
    return {"state": description.state.name, "assignor": description.partition_assignor,
            "members": [{"client_id": member.client_id,
                         "assignment": sorted(p.partition for p in member.assignment.topic_partitions)}
                        for member in description.members]}


def validate_members(members, expected_names):
    names = sorted(member["client_id"] for member in members)
    if names != sorted(expected_names):
        raise ValueError("Unexpected member set: " + repr(names))
    assigned = [p for member in members for p in member["assignment"]]
    if sorted(assigned) != list(range(PARTITIONS)):
        raise ValueError("Partitions must cover 0..2 exactly once")
    return {member["client_id"]: member["assignment"] for member in members}


def read_report(path):
    return json.loads(path.read_text(encoding="utf-8"))


def wait_stable(admin, group, procs, reports, expected, deadline):
    last, last_error = None, "not started"
    while time.monotonic() < deadline:
        for name in expected:
            if procs[name].poll() is not None:
                raise RuntimeError("Worker exited early: " + name)
        try:
            description = describe_group(admin, group)
            if description["state"] != "STABLE":
                raise ValueError("Group state: " + description["state"])
            server = validate_members(description["members"], expected)
            local = {}
            for name in expected:
                member_report = read_report(reports[name])
                if member_report.get("error"):
                    raise ValueError("Worker error: " + name)
                local[name] = member_report["assignment"]
            if any(server[name] != local[name] for name in expected):
                raise ValueError("Local assignment differs from broker view")
            signature = (tuple(sorted((n, tuple(server[n])) for n in server)),
                         tuple(sorted((n, tuple(local[n])) for n in local)))
            if signature == last:
                return server
            last = signature
        except Exception as exc:
            last, last_error = None, str(exc)
        # Group convergence needs paced checks; no busy loop and no fixed sleep guess.
        time.sleep(0.2)
    raise RuntimeError("Stability deadline reached: " + last_error)


def wait_reads(report_path, minimum, deadline):
    while time.monotonic() < deadline:
        try:
            report = read_report(report_path)
        except Exception:
            time.sleep(0.2)
            continue
        if report.get("error"):
            raise RuntimeError("Worker error: " + report["error"])
        if len(report["records"]) >= minimum:
            return
        time.sleep(0.2)
    raise RuntimeError("Read deadline reached; wanted " + str(minimum) + " records")


def wait_exit(proc, timeout, label):
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        raise RuntimeError("Worker did not exit in time: " + label)


def check_fresh_topic(ck, bootstrap, topic):
    consumer = ck.Consumer({"bootstrap.servers": bootstrap,
                            "group.id": "ch07-audit-" + topic,
                            "allow.auto.create.topics": False})
    try:
        for p in range(PARTITIONS):
            low, high = consumer.get_watermark_offsets(ck.TopicPartition(topic, p), timeout=10)
            if (low, high) != (0, 0):
                raise RuntimeError("Topic is not fresh; use a new unique topic")
    finally:
        consumer.close()


def produce(ck, bootstrap, topic):
    producer = ck.Producer({"bootstrap.servers": bootstrap, "acks": "all",
                            "enable.idempotence": True, "message.timeout.ms": 15000})
    metadata = producer.list_topics(topic, timeout=10)
    entry = metadata.topics.get(topic)
    if not entry or entry.error or set(entry.partitions) != set(range(PARTITIONS)) \
            or any(p.replicas != [1] for p in entry.partitions.values()):
        raise RuntimeError("Expected a fresh 3-partition, 1-replica topic")
    delivered, errors = [], []
    for record in fixtures():
        def callback(err, msg, event_id=record["event"]["event_id"]):
            if err:
                errors.append(str(err))
            else:
                delivered.append({"event_id": event_id, "partition": msg.partition(),
                                  "offset": msg.offset()})
        producer.produce(topic, partition=record["partition"],
                         key=record["event"]["order_id"].encode("utf-8"),
                         value=json.dumps(record["event"]).encode("utf-8"),
                         on_delivery=callback)
        producer.poll(0)
    if producer.flush(20) or errors or len(delivered) != len(fixtures()):
        raise RuntimeError("Not all six records were confirmed")
    expected = {r["event"]["event_id"]: (r["partition"], r["offset"]) for r in fixtures()}
    actual = {d["event_id"]: (d["partition"], d["offset"]) for d in delivered}
    if expected != actual:
        raise RuntimeError("Delivered coordinates differ from fixtures")
    return delivered


def revoke_assign_gaps(lifecycle):
    gaps = []
    for index, event in enumerate(lifecycle):
        if event["kind"] == "revoke" and index + 1 < len(lifecycle) \
                and lifecycle[index + 1]["kind"] == "assign":
            gaps.append(round(lifecycle[index + 1]["t"] - event["t"], 3))
    return gaps


def count_multi_reads(reports):
    counts = collections.Counter()
    owners = collections.defaultdict(set)
    for name, report in reports.items():
        for row in report["records"]:
            key = (row["partition"], row["offset"])
            counts[key] += 1
            owners[key].add(name)
    return {"records_read_at_least_twice": sum(1 for value in counts.values() if value >= 2),
            "records_read_by_multiple_workers": sum(1 for names in owners.values() if len(names) >= 2),
            "total_reads": sum(counts.values())}


def check_timings(timings):
    if not timings["crash_recovery_seconds"] > timings["graceful_recovery_seconds"]:
        raise ValueError("Expected crash recovery to take longer than a graceful leave")
    if timings["crash_recovery_seconds"] < 2.0:
        raise ValueError("Crash recovery must wait for the session timeout")


def verify_lab(reports, exit_codes, timings, final_server):
    if exit_codes.get("member-a") != 0 or exit_codes.get("member-b") != 42 \
            or exit_codes.get("member-b2") != 0:
        raise ValueError("Unexpected worker exit codes: " + repr(exit_codes))
    for name, report in reports.items():
        if report.get("error"):
            raise ValueError("Worker error in " + name + ": " + report["error"])
    if reports["member-b"].get("hard_exit") is not True or reports["member-b"].get("closed") is not False:
        raise ValueError("member-b must have crashed via os._exit without a close()")
    if reports["member-a"].get("closed") is not True or reports["member-b2"].get("closed") is not True:
        raise ValueError("member-a and member-b2 must close gracefully")
    all_six = {(r["partition"], r["offset"]) for r in fixtures()}
    a_seen = {(row["partition"], row["offset"]) for row in reports["member-a"]["records"]}
    if not all_six.issubset(a_seen):
        raise ValueError("member-a should have read all six records while it was alone")
    for name in ("member-b", "member-b2"):
        rows = reports[name]["records"]
        if len(rows) < 2:
            raise ValueError(name + " should re-read its partitions from the earliest offset")
        owned = set(reports[name]["assignment"])
        if any(row["partition"] not in owned for row in rows):
            raise ValueError(name + " read a partition it did not own")
    multi = count_multi_reads(reports)
    if multi["records_read_by_multiple_workers"] < 2:
        raise ValueError("Expected records to be read by more than one worker after rebalance")
    check_timings(timings)
    gaps = revoke_assign_gaps(reports["member-a"]["lifecycle"])
    if not gaps:
        raise ValueError("member-a should have revoke->assign transitions")
    if final_server != {"member-a": list(range(PARTITIONS))}:
        raise ValueError("Final stable state must be member-a owning all partitions")
    return {"timings": timings, "gaps_member_a_seconds": gaps, "multi_read": multi,
            "reads": {name: len(report["records"]) for name, report in reports.items()},
            "lifecycle_events": {name: len(report["lifecycle"]) for name, report in reports.items()}}


def run(args):
    ck = client()
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    control = output
    for leftover in ("crash-member-b", "stop-member-a", "stop-member-b2", "summary.json"):
        if (control / leftover).exists():
            raise RuntimeError("Use a fresh output directory for each run")
    compose = [args.compose_command] if args.compose_command else ["docker", "compose"]
    compose += ["-f", str(args.compose_file)]

    check_fresh_topic(ck, args.bootstrap, args.topic)
    save(output / "source.json", fixtures())
    save(output / "producer-deliveries.json", produce(ck, args.bootstrap, args.topic))

    reports = {name: output / (name + ".json") for name in WORKERS}
    procs, logs, result = {}, [], None

    def start(name):
        log = (output / (name + ".log")).open("w", encoding="utf-8")
        logs.append(log)
        command = [sys.executable, str(pathlib.Path(__file__).resolve()), "worker",
                   "--bootstrap", args.bootstrap, "--topic", args.topic, "--group", args.group,
                   "--name", name, "--report", str(reports[name]), "--control", str(control)]
        procs[name] = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)

    def cli_snapshot(stage):
        for suffix, options in (("members", ["--members", "--verbose"]), ("state", ["--state"])):
            command = compose + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                                 "--bootstrap-server", "kafka:9092", "--describe", "--group", args.group]
            completed = subprocess.run(command + options, capture_output=True, text=True, timeout=20)
            (output / (stage + ".cli-" + suffix + ".txt")).write_text(
                completed.stdout + completed.stderr, encoding="utf-8")
            if completed.returncode:
                raise RuntimeError("CLI inspection failed at " + stage)

    def all_reports():
        return {name: read_report(path) for name, path in reports.items()}

    try:
        # Phase 1: one member owns everything.
        start("member-a")
        wait_stable(admin, args.group, procs, reports, ["member-a"], time.monotonic() + 30)
        cli_snapshot("p1")
        wait_reads(reports["member-a"], len(fixtures()), time.monotonic() + 20)

        # Phase 2: a second member joins; partitions are re-split.
        join_start = time.monotonic()
        start("member-b")
        server_join = wait_stable(admin, args.group, procs, reports,
                                  ["member-a", "member-b"], time.monotonic() + 30)
        join_seconds = time.monotonic() - join_start
        cli_snapshot("p2")
        wait_reads(reports["member-b"], 2, time.monotonic() + 20)
        time.sleep(1.0)  # Let the re-reading of moved partitions settle before measuring.

        # Phase 3: member-b dies without cleanup; the group must wait for the session timeout.
        crash_start = time.monotonic()
        (control / "crash-member-b").touch()
        wait_exit(procs["member-b"], 15, "member-b")
        if procs["member-b"].returncode != 42:
            raise RuntimeError("member-b should exit 42")
        wait_stable(admin, args.group, procs, reports, ["member-a"], time.monotonic() + 45)
        crash_seconds = time.monotonic() - crash_start
        cli_snapshot("p3")

        # Phase 4: a fresh member joins, then leaves gracefully; compare recovery times.
        start("member-b2")
        wait_stable(admin, args.group, procs, reports, ["member-a", "member-b2"],
                    time.monotonic() + 30)
        wait_reads(reports["member-b2"], 2, time.monotonic() + 20)
        graceful_start = time.monotonic()
        (control / "stop-member-b2").touch()
        wait_exit(procs["member-b2"], 15, "member-b2")
        if procs["member-b2"].returncode != 0:
            raise RuntimeError("member-b2 should exit 0")
        server_final = wait_stable(admin, args.group, procs, reports, ["member-a"],
                                   time.monotonic() + 30)
        graceful_seconds = time.monotonic() - graceful_start
        cli_snapshot("p4")

        # Stop the last member gracefully, then verify the whole observation.
        (control / "stop-member-a").touch()
        wait_exit(procs["member-a"], 15, "member-a")
        exit_codes = {name: procs[name].returncode for name in procs}
        timings = {"join_seconds": round(join_seconds, 2),
                   "crash_recovery_seconds": round(crash_seconds, 2),
                   "graceful_recovery_seconds": round(graceful_seconds, 2)}
        summary = verify_lab(all_reports(), exit_codes, timings, server_final)
        save(output / "summary.json", summary)
        result = summary
        print("PASS: join rebalance split partitions", server_join, flush=True)
        print("PASS: crash recovery", timings["crash_recovery_seconds"], "s vs graceful",
              timings["graceful_recovery_seconds"], "s", flush=True)
        print("PASS: records read by multiple workers:",
              summary["multi_read"]["records_read_by_multiple_workers"], flush=True)
        print(json.dumps(summary, indent=2))
    finally:
        for name in WORKERS:
            if name in procs and procs[name].poll() is None:
                (control / ("stop-" + name)).touch()
        problems = []
        for name, proc in procs.items():
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
                problems.append(name + " needed termination")
        for log in logs:
            log.close()
        if problems:
            raise RuntimeError("Cleanup problems: " + repr(problems))
    if result is None:
        raise RuntimeError("Experiment did not finish")
    print("PASS: chapter 07 observations complete")


class Tests(unittest.TestCase):
    def test_fixtures(self):
        rows = fixtures()
        self.assertEqual(len(rows), 6)
        self.assertEqual([r["offset"] for r in rows if r["partition"] == 0], [0, 1])

    def test_validate_ok(self):
        members = [{"client_id": "a", "assignment": [0, 1]},
                   {"client_id": "b", "assignment": [2]}]
        self.assertEqual(validate_members(members, ["a", "b"]),
                         {"a": [0, 1], "b": [2]})

    def test_validate_overlap_rejected(self):
        members = [{"client_id": "a", "assignment": [0, 1]},
                   {"client_id": "b", "assignment": [1, 2]}]
        with self.assertRaises(ValueError):
            validate_members(members, ["a", "b"])

    def test_validate_missing_rejected(self):
        members = [{"client_id": "a", "assignment": [0, 1]}]
        with self.assertRaises(ValueError):
            validate_members(members, ["a"])

    def test_multi_read_counting(self):
        def report(name, rows):
            return {"name": name, "records": [{"partition": p, "offset": o} for p, o in rows]}
        out = count_multi_reads({"a": report("a", [(0, 0), (0, 1)]),
                                 "b": report("b", [(0, 0), (2, 0)])})
        self.assertEqual(out["records_read_by_multiple_workers"], 1)
        self.assertEqual(out["total_reads"], 4)

    def test_gaps(self):
        events = [{"kind": "assign", "t": 1.0}, {"kind": "revoke", "t": 2.0},
                  {"kind": "assign", "t": 2.5}]
        self.assertEqual(revoke_assign_gaps(events), [0.5])

    def test_timing_order(self):
        check_timings({"crash_recovery_seconds": 6.0, "graceful_recovery_seconds": 1.0})
        with self.assertRaises(ValueError):
            check_timings({"crash_recovery_seconds": 1.0, "graceful_recovery_seconds": 1.5})

    def test_crash_floor(self):
        with self.assertRaises(ValueError):
            check_timings({"crash_recovery_seconds": 0.5, "graceful_recovery_seconds": 0.2})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("self-test")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--bootstrap", default="127.0.0.1:19092")
    run_parser.add_argument("--topic", required=True)
    run_parser.add_argument("--group", required=True)
    run_parser.add_argument("--output", required=True, type=pathlib.Path)
    run_parser.add_argument("--compose-file", required=True, type=pathlib.Path)
    run_parser.add_argument("--compose-command")
    worker_parser = sub.add_parser("worker")
    worker_parser.add_argument("--bootstrap", required=True)
    worker_parser.add_argument("--topic", required=True)
    worker_parser.add_argument("--group", required=True)
    worker_parser.add_argument("--name", required=True)
    worker_parser.add_argument("--report", required=True, type=pathlib.Path)
    worker_parser.add_argument("--control", required=True, type=pathlib.Path)
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
