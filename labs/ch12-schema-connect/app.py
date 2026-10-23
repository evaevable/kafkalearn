#!/usr/bin/env python3
"""Chapter 12 lab: schema evolution and a real Kafka Connect file pipeline.

Part A sends v1/v2/v3 JSON events and checks them with an old and a new
consumer, plus a small compatibility checker.

Part B starts connect-standalone in a container, reads a file through
FileStreamSource into Kafka, and writes it back through FileStreamSink.
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
NETWORK = "kafkalearn_default"


def check_client():
    import confluent_kafka as ck

    found = metadata.version("confluent-kafka")
    if found != PIN:
        raise RuntimeError("Expected confluent-kafka==" + PIN + "; found " + found)
    return ck


def admin(bootstrap):
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": bootstrap})


def create_topic(bootstrap, name):
    from confluent_kafka.admin import NewTopic

    topic = NewTopic(name, num_partitions=1, replication_factor=1)
    client_admin = admin(bootstrap)
    client_admin.create_topics([topic], request_timeout=20)[name].result(timeout=30)


def produce(bootstrap, topic, rows):
    ck = check_client()
    producer = ck.Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 15000,
        }
    )
    errors = []

    def report(error, _message):
        if error is not None:
            errors.append(str(error))

    for key, value in rows:
        producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(value, separators=(",", ":")).encode("utf-8"),
            callback=report,
        )
        producer.poll(0)
    pending = producer.flush(30)
    if pending or errors:
        raise RuntimeError("Delivery incomplete: pending=%s errors=%s" % (pending, errors))


def read_events(bootstrap, topic, idle_seconds=2):
    ck = check_client()
    consumer = ck.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "ch12-audit-%s-%s" % (os.getpid(), time.time()),
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
    )
    rows = []
    try:
        consumer.assign([ck.TopicPartition(topic, 0, 0)])
        last = time.time()
        while time.time() - last < idle_seconds:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("Read error: %s" % message.error())
            last = time.time()
            rows.append(json.loads(message.value().decode("utf-8")))
    finally:
        consumer.close()
    return rows


def read_raw(bootstrap, topic, idle_seconds=3):
    ck = check_client()
    consumer = ck.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "ch12-connect-audit-%s-%s" % (os.getpid(), time.time()),
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
    )
    rows = []
    try:
        consumer.assign([ck.TopicPartition(topic, 0, 0)])
        last = time.time()
        while time.time() - last < idle_seconds:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("Read error: %s" % message.error())
            last = time.time()
            rows.append(message.value().decode("utf-8"))
    finally:
        consumer.close()
    return rows


def old_consumer(event):
    if "amount" not in event:
        raise ValueError("missing amount")
    return {
        "order_id": event["order_id"],
        "amount": event["amount"],
        "currency": event.get("currency", "CNY"),
    }


def new_consumer(event):
    if "currency" not in event:
        raise ValueError("missing currency")
    total = event.get("total", event.get("amount"))
    if total is None:
        raise ValueError("missing total")
    return {"order_id": event["order_id"], "total": total, "currency": event["currency"]}


def compatibility(old, new):
    problems = []
    for name, spec in old.items():
        if name not in new:
            problems.append("removed field: " + name)
            continue
        if spec["type"] != new[name]["type"]:
            problems.append("type changed: " + name)
        if new[name].get("required") and not spec.get("required"):
            problems.append("optional became required: " + name)
    for name, spec in new.items():
        if name not in old and spec.get("required") and "default" not in spec:
            problems.append("added required without default: " + name)
    return problems


def docker_network():
    try:
        output = subprocess.check_output(
            [
                "docker",
                "inspect",
                "kafkalearn-kafka-1",
                "--format",
                "{{range $name, $value := .NetworkSettings.Networks}}{{$name}}\n{{end}}",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        names = [line.strip() for line in output.splitlines() if line.strip()]
        return names[0] if names else NETWORK
    except Exception:
        return NETWORK


def write_connect_configs(directory, topic):
    (directory / "worker.properties").write_text(
        "\n".join(
            [
                "bootstrap.servers=kafka:9092",
                "key.converter=org.apache.kafka.connect.storage.StringConverter",
                "value.converter=org.apache.kafka.connect.storage.StringConverter",
                "offset.storage.file.filename=/data/connect.offsets",
                "offset.flush.interval.ms=1000",
                "plugin.path=/opt/kafka/libs",
                "rest.port=8083",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (directory / "source.properties").write_text(
        "\n".join(
            [
                "name=ch12-file-source",
                "connector.class=FileStreamSource",
                "tasks.max=1",
                "file=/data/input.txt",
                "topic=" + topic,
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (directory / "sink.properties").write_text(
        "\n".join(
            [
                "name=ch12-file-sink",
                "connector.class=FileStreamSink",
                "tasks.max=1",
                "file=/data/output.txt",
                "topics=" + topic,
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def run_connect(bootstrap, topic, directory, timeout=120):
    write_connect_configs(directory, topic)
    container = "kafkalearn-connect-" + str(os.getpid())
    command = [
        "docker",
        "run",
        "-d",
        "--name",
        container,
        "--network",
        docker_network(),
        "-v",
        "%s:/data" % directory,
        "-v",
        "%s:/opt/connect" % directory,
        "apache/kafka:4.3.1",
        "/opt/kafka/bin/connect-standalone.sh",
        "/opt/connect/worker.properties",
        "/opt/connect/source.properties",
        "/opt/connect/sink.properties",
    ]
    subprocess.run(command, check=True, capture_output=True, text=True)
    started = time.time()
    try:
        output_file = directory / "output.txt"
        while time.time() - started < timeout:
            running = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", container],
                capture_output=True,
                text=True,
            ).stdout.strip()
            lines = []
            if output_file.exists():
                lines = [line for line in output_file.read_text(encoding="utf-8").splitlines() if line.strip()]
            if len(lines) >= 3:
                return {"container": container, "elapsed_s": round(time.time() - started, 2), "sink_lines": lines}
            if running != "true":
                logs = subprocess.run(["docker", "logs", container], capture_output=True, text=True).stdout[-2000:]
                raise RuntimeError("Connect exited early:\n" + logs)
            time.sleep(2)
        logs = subprocess.run(["docker", "logs", container], capture_output=True, text=True).stdout[-2000:]
        raise TimeoutError("Connect sink did not write 3 lines within %ss:\n%s" % (timeout, logs))
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True, text=True)


def run(args):
    ck = check_client()
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + str(os.getpid())
    schema_topic = "shopflow.orders.ch12.%s.events" % run_id
    connect_topic = "shopflow.connect.ch12.%s" % run_id
    output = pathlib.Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    create_topic(args.bootstrap, schema_topic)
    create_topic(args.bootstrap, connect_topic)

    events = [
        {"schema_version": 1, "order_id": "order-1", "amount": 100},
        {"schema_version": 2, "order_id": "order-2", "amount": 200, "currency": "CNY"},
        {"schema_version": 3, "order_id": "order-3", "total": 300, "currency": "CNY"},
    ]
    produce(args.bootstrap, schema_topic, [(event["order_id"], event) for event in events])
    read_back = read_events(args.bootstrap, schema_topic)

    matrix = []
    for event in read_back:
        row = {"schema_version": event["schema_version"]}
        try:
            old_consumer(event)
            row["old_consumer"] = "ok"
        except ValueError as error:
            row["old_consumer"] = "fail: " + str(error)
        try:
            new_consumer(event)
            row["new_consumer"] = "ok"
        except ValueError as error:
            row["new_consumer"] = "fail: " + str(error)
        matrix.append(row)

    v1 = {
        "order_id": {"type": "string", "required": True},
        "amount": {"type": "int", "required": True},
    }
    v2 = {
        "order_id": {"type": "string", "required": True},
        "amount": {"type": "int", "required": True},
        "currency": {"type": "string", "required": False, "default": "CNY"},
    }
    v3 = {
        "order_id": {"type": "string", "required": True},
        "total": {"type": "int", "required": True},
        "currency": {"type": "string", "required": True},
    }
    compatibility_result = {
        "v1_to_v2": compatibility(v1, v2),
        "v2_to_v3": compatibility(v2, v3),
        "v1_to_v3": compatibility(v1, v3),
    }
    if compatibility_result["v1_to_v2"]:
        raise AssertionError("Adding an optional field should be compatible: %s" % compatibility_result["v1_to_v2"])
    if not compatibility_result["v2_to_v3"]:
        raise AssertionError("Renaming amount to total must be reported as breaking")

    connect_directory = output / "connect"
    if connect_directory.exists():
        shutil.rmtree(connect_directory)
    connect_directory.mkdir(parents=True)
    input_lines = ["order-1,100,CNY", "order-2,200,CNY", "order-3,300,CNY"]
    (connect_directory / "input.txt").write_text("\n".join(input_lines) + "\n", encoding="utf-8")

    connect = run_connect(args.bootstrap, connect_topic, connect_directory)
    topic_rows = read_raw(args.bootstrap, connect_topic)
    if sorted(topic_rows) != sorted(input_lines):
        raise AssertionError("Connect source records mismatch: %s" % topic_rows)
    if sorted(connect["sink_lines"]) != sorted(input_lines):
        raise AssertionError("Connect sink lines mismatch: %s" % connect["sink_lines"])

    summary = {
        "run_id": run_id,
        "client": {
            "confluent_kafka": metadata.version("confluent-kafka"),
            "librdkafka": ck.libversion(),
        },
        "schema": {
            "topic": schema_topic,
            "events": events,
            "matrix": matrix,
            "compatibility": compatibility_result,
        },
        "connect": {
            "topic": connect_topic,
            "connector": "FileStreamSource -> Kafka -> FileStreamSink",
            "input_lines": len(input_lines),
            "topic_records": len(topic_rows),
            "sink_lines": len(connect["sink_lines"]),
            "elapsed_s": connect["elapsed_s"],
            "container": connect["container"],
        },
    }
    (output / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(
        "PASS: v1/v2 readable by the old consumer, v2/v3 by the new one; Connect moved 3 lines file -> topic -> file"
    )


def self_test():
    v1 = {"order_id": {"type": "string", "required": True}, "amount": {"type": "int", "required": True}}
    v2 = dict(v1)
    v2["currency"] = {"type": "string", "required": False, "default": "CNY"}
    v3 = {
        "order_id": {"type": "string", "required": True},
        "total": {"type": "int", "required": True},
        "currency": {"type": "string", "required": True},
    }
    assert compatibility(v1, v2) == []
    assert any("removed field: amount" in problem for problem in compatibility(v2, v3))
    assert any("added required without default: total" in problem for problem in compatibility(v1, v3))
    print("PASS: 3 self-tests")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--bootstrap", default="127.0.0.1:19092")
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
