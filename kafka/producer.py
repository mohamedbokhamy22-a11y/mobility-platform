"""
producer.py
===========
Kafka producer for the Real-Time Mobility Data Platform.

Reads the four CSV event files produced by generator.py and streams each
record as a JSON message into its corresponding Kafka topic, simulating a
live event feed with configurable replay speed.

Topics:
    ride_events
    driver_location_events
    payment_events
    cancellation_events

Usage:
    # Stream all events as fast as possible
    python producer.py --data-dir ../data_generator/output

    # Simulate real-time (1 event per second per topic)
    python producer.py --data-dir ../data_generator/output --delay 1.0

    # Stream only ride and payment events
    python producer.py --data-dir ../data_generator/output --topics ride_events payment_events
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError:
    print("kafka-python not installed. Run: pip3 install kafka-python")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = "localhost:9092"

TOPIC_FILES = {
    "ride_events":             "ride_events.csv",
    "driver_location_events":  "driver_location_events.csv",
    "payment_events":          "payment_events.csv",
    "cancellation_events":     "cancellation_events.csv",
}

# Key field used as the Kafka message key (enables partition affinity)
TOPIC_KEYS = {
    "ride_events":             "ride_id",
    "driver_location_events":  "driver_id",
    "payment_events":          "ride_id",
    "cancellation_events":     "ride_id",
}


# ---------------------------------------------------------------------------
# Producer setup
# ---------------------------------------------------------------------------

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",                    # wait for all replicas
        retries=3,
        linger_ms=5,                   # batch small messages for throughput
        compression_type="gzip",
    )


# ---------------------------------------------------------------------------
# Stream a single topic
# ---------------------------------------------------------------------------

def stream_topic(
    producer: KafkaProducer,
    topic: str,
    csv_path: Path,
    delay: float,
    limit: int | None,
) -> int:
    if not csv_path.exists():
        print(f"  [SKIP] {csv_path} not found")
        return 0

    key_field = TOPIC_KEYS[topic]
    sent = 0

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if limit and sent >= limit:
                break

            key   = row.get(key_field)
            value = {k: v if v != "" else None for k, v in row.items()}

            producer.send(topic, key=key, value=value)
            sent += 1

            if sent % 10_000 == 0:
                producer.flush()
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"  [{ts}] {topic}: {sent:,} messages sent")

            if delay > 0:
                time.sleep(delay)

    producer.flush()
    return sent


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(data_dir: str, topics: list[str], delay: float, limit: int | None) -> None:
    data_path = Path(data_dir)

    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    try:
        producer = build_producer()
    except NoBrokersAvailable:
        print(
            "\nERROR: No Kafka broker available at localhost:9092.\n"
            "Start Kafka first:\n"
            "  docker compose up -d\n"
        )
        sys.exit(1)

    print(f"Connected. Streaming {len(topics)} topic(s)...\n")

    totals = {}
    overall_start = time.time()

    for topic in topics:
        csv_file = data_path / TOPIC_FILES[topic]
        print(f"→ Streaming {topic} from {csv_file.name}")
        start = time.time()
        n = stream_topic(producer, topic, csv_file, delay, limit)
        elapsed = time.time() - start
        rate = n / elapsed if elapsed > 0 else 0
        totals[topic] = n
        print(f"  Done: {n:,} messages in {elapsed:.1f}s ({rate:,.0f} msg/s)\n")

    producer.close()

    overall_elapsed = time.time() - overall_start
    total_sent = sum(totals.values())
    print("=" * 60)
    print(f"Finished: {total_sent:,} total messages in {overall_elapsed:.1f}s")
    for topic, n in totals.items():
        print(f"  {topic:<30} {n:>10,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mobility Platform — Kafka Producer")
    parser.add_argument(
        "--data-dir", type=str, default="../data_generator/output",
        help="Directory containing the CSV event files"
    )
    parser.add_argument(
        "--topics", nargs="+", default=list(TOPIC_FILES.keys()),
        choices=list(TOPIC_FILES.keys()),
        help="Topics to stream (default: all four)"
    )
    parser.add_argument(
        "--delay", type=float, default=0.0,
        help="Seconds to sleep between messages per topic (0 = as fast as possible)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max messages to send per topic (default: all)"
    )
    args = parser.parse_args()

    run(args.data_dir, args.topics, args.delay, args.limit)
