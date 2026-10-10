#!/usr/bin/env python3
"""Read-only PLAINTEXT Metadata v8 probe for this local Kafka lab.

Uses only the Python standard library. Not a producer, consumer, or general
Kafka client: no ApiVersions negotiation, TLS, SASL, or flexible versions.
Protocol source: Apache Kafka 4.3.1 MetadataRequest/MetadataResponse schemas.
"""
import argparse
import json
import socket
import struct
import sys
import unittest

MAX_FRAME = 4 * 1024 * 1024
CORRELATION_ID = 202


class ProtocolError(ValueError):
    pass


class Reader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def take(self, size):
        if size < 0 or self.pos + size > len(self.data):
            raise ProtocolError("Truncated or invalid Kafka response")
        value = self.data[self.pos:self.pos + size]
        self.pos += size
        return value

    def number(self, fmt):
        return struct.unpack(fmt, self.take(struct.calcsize(fmt)))[0]

    def string(self, nullable=False):
        size = self.number(">h")
        if size == -1 and nullable:
            return None
        if size < 0:
            raise ProtocolError("Invalid Kafka string length")
        return self.take(size).decode("utf-8")

    def count(self):
        count = self.number(">i")
        if not 0 <= count <= 100000:
            raise ProtocolError("Invalid Kafka array length")
        return count

    def int_array(self):
        return [self.number(">i") for _ in range(self.count())]


def recv_exact(sock, size):
    parts = bytearray()
    while len(parts) < size:
        chunk = sock.recv(size - len(parts))
        if not chunk:
            raise ProtocolError("Kafka closed the connection before the frame ended")
        parts.extend(chunk)
    return bytes(parts)


def parse_response(data):
    r = Reader(data)
    if r.number(">i") != CORRELATION_ID:
        raise ProtocolError("Response correlation ID differs from the request")
    throttle = r.number(">i")
    brokers = []
    for _ in range(r.count()):
        brokers.append({"node_id": r.number(">i"), "host": r.string(),
                        "port": r.number(">i"), "rack": r.string(nullable=True)})
    cluster_id = r.string(nullable=True)
    controller_id = r.number(">i")
    topics = []
    for _ in range(r.count()):
        topic = {"error_code": r.number(">h"), "name": r.string(),
                 "is_internal": bool(r.number(">b")), "partitions": []}
        for _ in range(r.count()):
            topic["partitions"].append({
                "error_code": r.number(">h"), "partition": r.number(">i"),
                "leader_id": r.number(">i"), "leader_epoch": r.number(">i"),
                "replicas": r.int_array(), "isr": r.int_array(),
                "offline_replicas": r.int_array()})
        r.number(">i")  # TopicAuthorizedOperations (v8).
        topics.append(topic)
    r.number(">i")  # ClusterAuthorizedOperations (v8).
    if r.pos != len(data):
        raise ProtocolError("Unexpected trailing bytes in Metadata v8 response")
    return {"api_version": 8, "cluster_id": cluster_id,
            "controller_id": controller_id, "brokers": brokers,
            "topics": topics, "throttle_time_ms": throttle}


def request_metadata(host, port, timeout):
    client_id = b"kafkalearn-ch02"
    header = struct.pack(">hhih", 3, 8, CORRELATION_ID, len(client_id)) + client_id
    # Empty topics array requests no topic metadata, with all creation/auth flags false.
    body = struct.pack(">ibbb", 0, 0, 0, 0)
    payload = header + body
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(struct.pack(">i", len(payload)) + payload)
        length = struct.unpack(">i", recv_exact(sock, 4))[0]
        if not 0 < length <= MAX_FRAME:
            raise ProtocolError("Invalid or oversized Kafka response frame")
        return parse_response(recv_exact(sock, length))


def validate_metadata(metadata, expected_cluster_id, expected_host, expected_port):
    if metadata["cluster_id"] != expected_cluster_id:
        raise ProtocolError("Unexpected cluster identity")
    if metadata["controller_id"] != 1:
        raise ProtocolError("Expected controller ID 1 in the single-node lab")
    brokers = metadata["brokers"]
    if len(brokers) != 1 or brokers[0]["node_id"] != 1:
        raise ProtocolError("Expected exactly one broker: node 1")
    if (brokers[0]["host"], brokers[0]["port"]) != (expected_host, expected_port):
        raise ProtocolError("Advertised broker endpoint differs from expected listener")


class ParserTests(unittest.TestCase):
    @staticmethod
    def sample():
        def text(value):
            raw = value.encode()
            return struct.pack(">h", len(raw)) + raw
        return (struct.pack(">iii", CORRELATION_ID, 0, 1) + struct.pack(">i", 1)
                + text("127.0.0.1") + struct.pack(">ih", 19092, -1)
                + text("MkU3OEVBNTcwNTJENDM2Qk")
                + struct.pack(">iii", 1, 0, -2147483648))

    def test_valid(self):
        result = parse_response(self.sample())
        validate_metadata(result, "MkU3OEVBNTcwNTJENDM2Qk", "127.0.0.1", 19092)
        self.assertEqual(result["topics"], [])

    def test_truncated(self):
        with self.assertRaises(ProtocolError):
            parse_response(self.sample()[:-1])

    def test_wrong_correlation(self):
        with self.assertRaises(ProtocolError):
            parse_response(struct.pack(">i", -1) + self.sample()[4:])

    def test_trailing_data(self):
        with self.assertRaises(ProtocolError):
            parse_response(self.sample() + b"unexpected")

    def test_wrong_endpoint(self):
        with self.assertRaises(ProtocolError):
            validate_metadata(parse_response(self.sample()),
                              "MkU3OEVBNTcwNTJENDM2Qk", "kafka", 9092)

    def test_negative_array(self):
        with self.assertRaises(ProtocolError):
            Reader(struct.pack(">i", -1)).count()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19092)
    parser.add_argument("--timeout", type=float, default=8)
    parser.add_argument("--expected-host", default="127.0.0.1")
    parser.add_argument("--expected-port", type=int, default=19092)
    parser.add_argument("--expected-cluster-id", default="MkU3OEVBNTcwNTJENDM2Qk")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(ParserTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if not 1 <= args.port <= 65535 or not 1 <= args.expected_port <= 65535 or args.timeout <= 0:
        parser.error("Ports must be 1..65535 and timeout must be positive")
    try:
        bootstrap = request_metadata(args.host, args.port, args.timeout)
        validate_metadata(bootstrap, args.expected_cluster_id,
                          args.expected_host, args.expected_port)
        endpoint = bootstrap["brokers"][0]
        followup = request_metadata(endpoint["host"], endpoint["port"], args.timeout)
        validate_metadata(followup, args.expected_cluster_id,
                          args.expected_host, args.expected_port)
        print(json.dumps({"bootstrap": bootstrap, "advertised_followup": followup},
                         ensure_ascii=False, indent=2))
        print("PASS: Kafka metadata and advertised endpoint verified", file=sys.stderr)
        return 0
    except (OSError, ValueError, struct.error) as exc:
        print("FAIL: metadata probe: " + str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
