#!/usr/bin/env python3
"""Chapter 11 lab: dual-write gap, transactional outbox and idempotent consumer.

A real SQLite file is used for the business data, while Kafka receives events.
Crashes are simulated with exceptions at precise points; this is a teaching
model, not a database durability or Debezium test.
"""

import argparse
import importlib.metadata as metadata
import json
import os
import pathlib
import sqlite3
import sys
import time

PIN = "2.13.2"


def check_client():
    import confluent_kafka as ck

    found = metadata.version("confluent-kafka")
    if found != PIN:
        raise RuntimeError("Expected confluent-kafka==" + PIN + "; found " + found)
    return ck


def create_topic(bootstrap, name):
    from confluent_kafka.admin import AdminClient, NewTopic

    topic = NewTopic(name, num_partitions=1, replication_factor=1)
    client_admin = AdminClient({"bootstrap.servers": bootstrap})
    client_admin.create_topics([topic], request_timeout=20)[name].result(timeout=30)


def produce(bootstrap, topic, rows):
    ck = check_client()
    instance = ck.Producer(
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


def read_all(bootstrap, topic, idle_seconds=2):
    ck = check_client()
    consumer = ck.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "ch11-audit-%s-%s" % (os.getpid(), time.time()),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
            "isolation.level": "read_committed",
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


def connect(path):
    connection = sqlite3.connect(path, isolation_level=None)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def setup_database(connection):
    connection.executescript(
        """
        CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            status TEXT NOT NULL
        );
        CREATE TABLE outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            aggregate_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            published INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE processed_events (
            event_id TEXT PRIMARY KEY,
            consumer TEXT NOT NULL,
            processed_at TEXT NOT NULL
        );
        CREATE TABLE inventory (
            order_id TEXT PRIMARY KEY,
            deducted INTEGER NOT NULL
        );
        """
    )


class RelayCrash(RuntimeError):
    pass


def dual_write(connection, bootstrap, topic):
    db_only = "order-db-only"
    with connection:
        connection.execute("INSERT INTO orders VALUES (?, ?)", (db_only, "created"))
    # simulated crash after the database commit, before the event is sent
    db_only_events = 0

    ghost = "order-ghost"
    produce(
        bootstrap,
        topic,
        [(ghost, {"event_id": "evt-ghost", "order_id": ghost, "event_type": "order.created"})],
    )
    try:
        connection.execute("BEGIN")
        connection.execute("INSERT INTO orders VALUES (?, ?)", (ghost, "created"))
        raise RuntimeError("simulated crash before database commit")
    except RuntimeError:
        connection.rollback()

    ghost_rows = connection.execute(
        "SELECT COUNT(*) FROM orders WHERE order_id = ?", (ghost,)
    ).fetchone()[0]
    return {
        "db_only_order": db_only,
        "db_only_events": db_only_events,
        "ghost_event_order": ghost,
        "ghost_order_rows": ghost_rows,
    }


def create_order_with_outbox(connection, order_id, event_id):
    payload = json.dumps(
        {"event_id": event_id, "order_id": order_id, "event_type": "order.created"},
        separators=(",", ":"),
    )
    connection.execute("BEGIN IMMEDIATE")
    connection.execute("INSERT INTO orders VALUES (?, ?)", (order_id, "created"))
    connection.execute(
        "INSERT INTO outbox (event_id, aggregate_id, event_type, payload, published) VALUES (?, ?, ?, ?, 0)",
        (event_id, order_id, "order.created", payload),
    )
    connection.commit()


def relay_once(connection, bootstrap, topic, crash_before_mark):
    rows = connection.execute(
        "SELECT id, event_id, payload FROM outbox WHERE published = 0 ORDER BY id"
    ).fetchall()
    if not rows:
        return 0
    produce(
        bootstrap,
        topic,
        [(row[1], json.loads(row[2])) for row in rows],
    )
    if crash_before_mark:
        raise RelayCrash("simulated crash after publish, before published=1")
    connection.execute("BEGIN IMMEDIATE")
    for row in rows:
        connection.execute("UPDATE outbox SET published = 1 WHERE id = ?", (row[0],))
    connection.commit()
    return len(rows)


def consume_with_dedupe(connection, bootstrap, topic):
    rows = read_all(bootstrap, topic)
    applied = 0
    skipped = 0
    for event in rows:
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO processed_events VALUES (?, ?, ?)",
                (event["event_id"], "inventory", "now"),
            )
            connection.execute(
                "INSERT INTO inventory VALUES (?, 1) "
                "ON CONFLICT(order_id) DO UPDATE SET deducted = deducted + 1",
                (event["order_id"],),
            )
            connection.commit()
            applied += 1
        except sqlite3.IntegrityError:
            connection.rollback()
            skipped += 1
    deducted = connection.execute(
        "SELECT deducted FROM inventory WHERE order_id = ?", ("order-outbox-1",)
    ).fetchone()
    return {
        "delivered": len(rows),
        "naive_effects": len(rows),
        "applied": applied,
        "duplicates_skipped": skipped,
        "inventory_deducted": deducted[0] if deducted else 0,
    }


def run(args):
    ck = check_client()
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + str(os.getpid())
    dual_topic = "shopflow.orders.ch11.%s.dual" % run_id
    outbox_topic = "shopflow.orders.ch11.%s.outbox" % run_id
    output = pathlib.Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    database = output / "shopflow.db"
    if database.exists():
        database.unlink()
    connection = connect(str(database))
    setup_database(connection)

    create_topic(args.bootstrap, dual_topic)
    create_topic(args.bootstrap, outbox_topic)

    dual = dual_write(connection, args.bootstrap, dual_topic)
    dual_events = read_all(args.bootstrap, dual_topic)
    if len(dual_events) != 1 or dual_events[0]["order_id"] != "order-ghost":
        raise AssertionError("Expected exactly one ghost event, found %s" % dual_events)
    if dual["ghost_order_rows"] != 0:
        raise AssertionError("Ghost order should not exist in the database")

    create_order_with_outbox(connection, "order-outbox-1", "evt-outbox-1")
    order_count = connection.execute("SELECT COUNT(*) FROM orders WHERE order_id = 'order-outbox-1'").fetchone()[0]
    outbox_count = connection.execute("SELECT COUNT(*) FROM outbox WHERE event_id = 'evt-outbox-1'").fetchone()[0]
    if order_count != 1 or outbox_count != 1:
        raise AssertionError("Order and outbox row must be committed together")

    try:
        first_published = relay_once(connection, args.bootstrap, outbox_topic, crash_before_mark=True)
    except RelayCrash:
        first_published = 1
    second_published = relay_once(connection, args.bootstrap, outbox_topic, crash_before_mark=False)
    topic_events = read_all(args.bootstrap, outbox_topic)
    if len(topic_events) != 2:
        raise AssertionError("Relay retry should produce a duplicate, found %s events" % len(topic_events))

    consumer = consume_with_dedupe(connection, args.bootstrap, outbox_topic)
    if consumer["applied"] != 1 or consumer["duplicates_skipped"] != 1:
        raise AssertionError("Idempotent consumer should apply once and skip once: %s" % consumer)
    if consumer["inventory_deducted"] != 1:
        raise AssertionError("Inventory should be deducted once, found %s" % consumer["inventory_deducted"])

    published_flag = connection.execute(
        "SELECT published FROM outbox WHERE event_id = 'evt-outbox-1'"
    ).fetchone()[0]
    if published_flag != 1:
        raise AssertionError("Outbox row should be marked published after successful retry")

    summary = {
        "run_id": run_id,
        "client": {
            "confluent_kafka": metadata.version("confluent-kafka"),
            "librdkafka": ck.libversion(),
        },
        "dual_write": {
            "topic": dual_topic,
            **dual,
            "events_read": len(dual_events),
        },
        "outbox": {
            "topic": outbox_topic,
            "order_rows": order_count,
            "outbox_rows": outbox_count,
            "first_relay_published": first_published,
            "crash_stage": "after publish, before published=1",
            "second_relay_published": second_published,
            "topic_records": len(topic_events),
            "published_flag": published_flag,
        },
        "consumer": consumer,
    }
    (output / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    connection.close()
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(
        "PASS: dual write left one DB-only order and one ghost event; outbox relayed twice after a crash; "
        "idempotent consumer applied once and inventory stayed at 1"
    )


def self_test():
    events = [{"event_id": "e1"}, {"event_id": "e1"}, {"event_id": "e2"}]
    seen = set()
    applied = 0
    for event in events:
        if event["event_id"] in seen:
            continue
        seen.add(event["event_id"])
        applied += 1
    assert applied == 2
    assert len(events) - applied == 1
    assert len(set(event["event_id"] for event in events)) == 2
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
