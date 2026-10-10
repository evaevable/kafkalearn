#!/usr/bin/env python3
"""Standard-library fixtures and assertions, NOT a Kafka client.

Murmur2 mirrors Apache Kafka 4.3.1 Utils.murmur2 and Java keyed routing.
Only the console producer/consumer communicates with Kafka in this lab.
"""
import argparse
import collections
import json
import pathlib
import re
import sys
import unittest

MASK = 0xffffffff


def murmur2(data):
    size = len(data)
    h = (0x9747b28c ^ size) & MASK
    m = 0x5bd1e995
    for i in range(size // 4):
        k = int.from_bytes(data[i * 4:i * 4 + 4], "little")
        k = (k * m) & MASK
        k ^= k >> 24
        k = (k * m) & MASK
        h = ((h * m) & MASK) ^ k
    tail = data[(size // 4) * 4:]
    if tail:
        h ^= int.from_bytes(tail, "little")
        h = (h * m) & MASK
    h ^= h >> 13
    h = (h * m) & MASK
    h ^= h >> 15
    return h & MASK


def partition(key):
    return (murmur2(key.encode("utf-8")) & 0x7fffffff) % 3


def fixtures():
    keys = {}
    for n in range(1000):
        key = "order-" + str(3000 + n)
        keys.setdefault(partition(key), key)
        if len(keys) == 3:
            break
    if set(keys) != {0, 1, 2}:
        raise ValueError("Could not find a key for each partition")
    records = []
    for sequence, event_type in enumerate(("order.created", "order.paid")):
        for p in range(3):
            key = keys[p]
            event = {"event_id": key + "-" + str(sequence), "order_id": key,
                     "event_type": event_type, "schema_version": 1}
            records.append({"partition": p, "offset": sequence, "key": key, "event": event})
    return records


def generate(root):
    root.mkdir(parents=True, exist_ok=True)
    records = fixtures()
    (root / "expected.json").write_text(json.dumps(records, indent=2) + "\n")
    (root / "input.txt").write_text("".join(
        r["key"] + "|" + json.dumps(r["event"], separators=(",", ":")) + "\n"
        for r in records))
    print("Generated 6 keyed events; selected key mapping:")
    print(json.dumps({str(r["partition"]): r["key"] for r in records}, indent=2))


def parse_records(text):
    result = []
    for line in text.splitlines():
        fields = line.split("\t", 3)
        if len(fields) != 4:
            raise ValueError("Expected Partition, Offset, Key, JSON: " + line)
        p = re.fullmatch(r"Partition:(\d+)", fields[0])
        offset = re.fullmatch(r"Offset:(\d+)", fields[1])
        if not p or not offset:
            raise ValueError("Invalid partition or offset labels: " + line)
        result.append({"partition": int(p.group(1)), "offset": int(offset.group(1)),
                       "key": fields[2], "event": json.loads(fields[3])})
    return result


def check_partition(actual, expected, p):
    wanted = sorted([r for r in expected if r["partition"] == p], key=lambda r: r["offset"])
    if actual != wanted:
        raise ValueError("Partition " + str(p) + " differs from expected keys, offsets, or events")


def verify(root, topic):
    expected = json.loads((root / "expected.json").read_text())
    if expected != fixtures():
        raise ValueError("Fixture manifest differs from the deterministic lab input")
    observed = []
    per_partition = {}
    for p in range(3):
        rows = parse_records((root / ("partition-" + str(p) + ".txt")).read_text())
        check_partition(rows, expected, p)
        per_partition[p] = rows
        observed.extend(rows)
    ids = [r["event"]["event_id"] for r in observed]
    if len(ids) != 6 or len(set(ids)) != 6:
        raise ValueError("Expected exactly six distinct event IDs, not copies in every partition")
    description = (root / "topic.txt").read_text()
    if not re.search(r"PartitionCount:\s*3\b", description) or not re.search(r"ReplicationFactor:\s*1\b", description):
        raise ValueError("Expected 3 partitions, 1 replica")
    described = re.findall(r"Partition:\s*(\d+)\s+Leader:\s*(\d+)\s+Replicas:\s*(\S+)\s+Isr:\s*(\S+)", description)
    if sorted(described) != [(str(p), "1", "1", "1") for p in range(3)]:
        raise ValueError("Expected all three partition leaders and replicas on broker 1")
    end_offsets = {}
    for line in (root / "end-offsets.txt").read_text().splitlines():
        name, p, offset = line.rsplit(":", 2)
        if name != topic or int(p) in end_offsets:
            raise ValueError("Unexpected topic or duplicate partition in offset output")
        end_offsets[int(p)] = int(offset)
    if end_offsets != {0: 2, 1: 2, 2: 2}:
        raise ValueError("Log end offsets must be next position 2, not last offset 1")
    seek = (root / "seek-partition-1-offset-1.jsonl").read_text().splitlines()
    if len(seek) != 1 or json.loads(seek[0]) != per_partition[1][1]["event"]:
        raise ValueError("Direct seek did not locate partition 1 / offset 1")
    traversals = {
        "partition_order_0_1_2": [r for p in (0, 1, 2) for r in per_partition[p]],
        "partition_order_2_1_0": [r for p in (2, 1, 0) for r in per_partition[p]]}
    if traversals["partition_order_0_1_2"] == traversals["partition_order_2_1_0"]:
        raise ValueError("Expected different cross-partition traversal examples")
    (root / "two-traversals.json").write_text(json.dumps(traversals, indent=2) + "\n")
    (root / "coordinates.json").write_text(json.dumps([
        {"topic": topic, **r} for r in observed], indent=2) + "\n")
    print("PASS: 3 partitions / 1 replica; all leaders on the single broker.")
    print("PASS: Exactly 6 events; each key stays in its partition, created before paid.")
    print("PASS: Each partition has offsets 0,1; log end offset is 2.")
    print("PASS: Partition 1 / offset 1 locates the expected paid event.")
    print("PASS: Two constructed traversals differ while preserving each partition's order.")
    print("NOTE: Traversals are examples, not a live consumer interleaving experiment.")


class Tests(unittest.TestCase):
    def test_empty_murmur(self):
        self.assertEqual(murmur2(b""), 0x106e08d9)

    def test_fixture_coverage(self):
        rows = fixtures()
        self.assertEqual(collections.Counter(r["partition"] for r in rows), {0: 2, 1: 2, 2: 2})
        self.assertTrue(all(partition(r["key"]) == r["partition"] for r in rows))

    def test_parser(self):
        self.assertEqual(parse_records('Partition:0\tOffset:1\torder-1\t{"n":2}\n')[0]["offset"], 1)

    def test_invalid_parser(self):
        with self.assertRaises(ValueError):
            parse_records("unexpected banner")

    def test_wrong_offset_rejected(self):
        rows = [dict(r) for r in fixtures() if r["partition"] == 0]
        rows[1]["offset"] = 99
        with self.assertRaises(ValueError):
            check_partition(rows, fixtures(), 0)

    def test_reversed_order_rejected(self):
        rows = [r for r in fixtures() if r["partition"] == 0]
        with self.assertRaises(ValueError):
            check_partition(list(reversed(rows)), fixtures(), 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("self-test")
    gen = sub.add_parser("generate")
    gen.add_argument("results", type=pathlib.Path)
    check = sub.add_parser("verify")
    check.add_argument("results", type=pathlib.Path)
    check.add_argument("topic")
    args = parser.parse_args()
    try:
        if args.action == "self-test":
            suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
            return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
        if args.action == "generate":
            generate(args.results)
        else:
            verify(args.results, args.topic)
        return 0
    except (OSError, ValueError, KeyError) as exc:
        print("FAIL: " + str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
