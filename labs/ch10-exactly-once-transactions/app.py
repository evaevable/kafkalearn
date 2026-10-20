#!/usr/bin/env python3
"""Chapter 10 lab: delivery semantics, idempotence and Kafka transactions.

Single-broker teaching experiment. It shows what read_committed hides, how
transactional offsets make a consume-transform-produce loop atomic inside
Kafka, and why producer idempotence is not business-level deduplication.
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


def check_client():
    import confluent_kafka as ck

    found = metadata.version("confluent-kafka")
    if found != PIN:
        raise RuntimeError("Expected confluent-kafka==" + PIN + "; found " + found)
    return ck


def admin(bootstrap):
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": bootstrap})


def create_topic(bootstrap, name, partitions=1, config=None):
    from confluent_kafka.admin import NewTopic

    topic = NewTopic(
        name,
        num_partitions=partitions,
        replication_factor=1,
        config=config or {},
    )
    client_admin = admin(bootstrap)
    client_admin.create_topics([topic], request_timeout=20)[name].result(timeout=30)


def producer(bootstrap, transactional_id=None):
    ck = check_client()
    config = {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "message.timeout.ms": 15000,
        "linger.ms": 0,
    }
    if transactional_id:
        config["transactional.id"] = transactional_id
    return ck.Producer(config)


def send_all(instance, topic, rows):
    errors = []

    def report(error, _message):
        if error is not None:
            errors.append(str(error))

    for key, value in rows:
        instance.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(value, separators=(",", ":")).encode("utf-8"),
            callback=report,
        )
        instance.poll(0)
    pending = instance.flush(30)
    if pending or errors:
        raise RuntimeError("Delivery incomplete: pending=%s errors=%s" % (pending, errors))


def read_all(bootstrap, topic, isolation, idle_seconds=3):
    ck = check_client()
    consumer = ck.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "ch10-audit-%s-%s" % (os.getpid(), time.time()),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
            "isolation.level": isolation,
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
            rows.append(
                {
                    "offset": message.offset(),
                    "key": message.key().decode("utf-8") if message.key() else None,
                    "value": json.loads(message.value().decode("utf-8")),
                }
            )
    finally:
        consumer.close()
    return rows


def run(args):
    ck = check_client()
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + str(os.getpid())
    tx_topic = "shopflow.orders.ch10.%s.tx" % run_id
    dupes_topic = "shopflow.orders.ch10.%s.dupes" % run_id
    source_topic = "shopflow.orders.ch10.%s.source" % run_id
    sink_topic = "shopflow.orders.ch10.%s.sink" % run_id
    group = "shopflow.ch10.eos.%s" % run_id
    output = pathlib.Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    for name in (tx_topic, dupes_topic, source_topic, sink_topic):
        create_topic(args.bootstrap, name)

    transactional = producer(args.bootstrap, "ch10-tx-" + run_id)
    transactional.init_transactions(30)
    transactional.begin_transaction()
    send_all(
        transactional,
        tx_topic,
        [("order-1001", {"order_id": "order-1001", "step": i, "state": "committed"}) for i in range(3)],
    )
    transactional.commit_transaction(30)

    transactional.begin_transaction()
    send_all(
        transactional,
        tx_topic,
        [("order-2002", {"order_id": "order-2002", "step": i, "state": "aborted"}) for i in range(2)],
    )
    transactional.abort_transaction(30)

    committed_view = read_all(args.bootstrap, tx_topic, "read_committed")
    uncommitted_view = read_all(args.bootstrap, tx_topic, "read_uncommitted")
    if [row["key"] for row in committed_view] != ["order-1001"] * 3:
        raise AssertionError("read_committed should see only the 3 committed records: %s" % committed_view)
    if len(uncommitted_view) != 5:
        raise AssertionError("read_uncommitted should see 5 records, found %s" % len(uncommitted_view))
    aborted_offsets = [row["offset"] for row in uncommitted_view if row["value"]["state"] == "aborted"]
    if aborted_offsets != [4, 5]:
        raise AssertionError("Unexpected aborted offsets: %s" % aborted_offsets)

    idempotent = producer(args.bootstrap)
    send_all(
        idempotent,
        dupes_topic,
        [
            ("same-key", {"business": "same-payload"}),
            ("same-key", {"business": "same-payload"}),
        ],
    )
    duplicate_view = read_all(args.bootstrap, dupes_topic, "read_committed")
    if len(duplicate_view) != 2:
        raise AssertionError("Two application sends should remain two records, found %s" % len(duplicate_view))

    source_rows = [("order-3001", {"order_id": "order-3001", "step": i}) for i in range(3)]
    send_all(producer(args.bootstrap), source_topic, source_rows)

    ck = check_client()
    consumer = ck.Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
            "isolation.level": "read_committed",
            "session.timeout.ms": 6000,
        }
    )
    consumed = []
    try:
        consumer.subscribe([source_topic])
        deadline = time.time() + 30
        while len(consumed) < 3 and time.time() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("Source read error: %s" % message.error())
            consumed.append(message)
        if len(consumed) != 3:
            raise AssertionError("Expected 3 source records, found %s" % len(consumed))

        eos = producer(args.bootstrap, "ch10-eos-" + run_id)
        eos.init_transactions(30)
        eos.begin_transaction()
        send_all(
            eos,
            sink_topic,
            [
                (
                    message.key().decode("utf-8"),
                    {"order_id": json.loads(message.value())["order_id"], "source_offset": message.offset()},
                )
                for message in consumed
            ],
        )
        next_offset = max(message.offset() for message in consumed) + 1
        eos.send_offsets_to_transaction(
            [ck.TopicPartition(source_topic, 0, next_offset)],
            consumer.consumer_group_metadata(),
            30,
        )
        eos.commit_transaction(30)
    finally:
        consumer.close()

    sink_rows = read_all(args.bootstrap, sink_topic, "read_committed")
    if len(sink_rows) != 3:
        raise AssertionError("Expected 3 sink records, found %s" % len(sink_rows))

    checker = ck.Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
    )
    try:
        checker.assign([ck.TopicPartition(source_topic, 0)])
        committed = checker.committed([ck.TopicPartition(source_topic, 0)], timeout=10)[0].offset
    finally:
        checker.close()
    if committed != 3:
        raise AssertionError("Expected committed offset 3, found %s" % committed)

    resume = ck.Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
            "isolation.level": "read_committed",
        }
    )
    resumed = []
    try:
        resume.subscribe([source_topic])
        deadline = time.time() + 6
        while time.time() < deadline:
            message = resume.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("Resume read error: %s" % message.error())
            resumed.append(message)
    finally:
        resume.close()
    if resumed:
        raise AssertionError("EOS consumer should resume after committed offset, re-read %s records" % len(resumed))

    summary = {
        "run_id": run_id,
        "client": {
            "confluent_kafka": metadata.version("confluent-kafka"),
            "librdkafka": ck.libversion(),
        },
        "transaction": {
            "topic": tx_topic,
            "committed_records": 3,
            "aborted_records": 2,
            "read_committed_records": len(committed_view),
            "read_uncommitted_records": len(uncommitted_view),
            "committed_offsets": [row["offset"] for row in committed_view],
            "aborted_offsets": aborted_offsets,
        },
        "idempotence": {
            "topic": dupes_topic,
            "application_sends": 2,
            "records_read": len(duplicate_view),
        },
        "eos": {
            "source_topic": source_topic,
            "sink_topic": sink_topic,
            "group": group,
            "source_records": 3,
            "sink_records": len(sink_rows),
            "committed_offset": committed,
            "resume_reread_records": len(resumed),
        },
    }
    (output / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(
        "PASS: read_committed saw 3 and read_uncommitted saw 5; idempotent producer still kept 2 application sends; "
        "EOS sink=3 and committed offset=3 with no resume re-read"
    )


def self_test():
    rows = [
        {"offset": 0, "value": {"state": "committed"}},
        {"offset": 1, "value": {"state": "committed"}},
        {"offset": 2, "value": {"state": "committed"}},
        {"offset": 3, "value": {"state": "aborted"}},
        {"offset": 4, "value": {"state": "aborted"}},
    ]
    assert [row["offset"] for row in rows if row["value"]["state"] == "aborted"] == [3, 4]
    assert max(row["offset"] for row in rows if row["value"]["state"] == "committed") + 1 == 3
    assert len([row for row in rows if row["value"]["state"] == "committed"]) == 3
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
