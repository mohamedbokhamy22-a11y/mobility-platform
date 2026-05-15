"""
consumer.py
===========
Simple Kafka consumer to verify messages are flowing.
Prints a live sample from each topic and shows message counts.

Usage:
    python consumer.py                        # tail all 4 topics
    python consumer.py --topic ride_events    # tail one topic
    python consumer.py --limit 5             # print 5 messages per topic then exit
"""

import argparse
import json
import sys

try:
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
except ImportError:
    print("kafka-python not installed. Run: pip3 install kafka-python")
    sys.exit(1)

BOOTSTRAP_SERVERS = "localhost:9092"
ALL_TOPICS = [
    "ride_events",
    "driver_location_events",
    "payment_events",
    "cancellation_events",
]


def tail(topics: list[str], limit: int) -> None:
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            key_deserializer=lambda b: b.decode("utf-8") if b else None,
            consumer_timeout_ms=5000,       # stop polling after 5s of silence
            group_id="mobility-consumer-debug",
        )
    except NoBrokersAvailable:
        print("ERROR: No Kafka broker at localhost:9092. Start Kafka first.")
        sys.exit(1)

    counts: dict[str, int] = {t: 0 for t in topics}

    print(f"Tailing topics: {', '.join(topics)}\n")
    for msg in consumer:
        topic = msg.topic
        counts[topic] += 1
        if counts[topic] <= limit:
            print(f"[{topic}]  key={msg.key}  offset={msg.offset}")
            for k, v in msg.value.items():
                print(f"  {k}: {v}")
            print()

        if all(c >= limit for c in counts.values()):
            break

    consumer.close()
    print("\nMessage counts consumed:")
    for topic, n in counts.items():
        print(f"  {topic:<30} {n:>8,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mobility Platform — Kafka Consumer (debug)")
    parser.add_argument("--topics", nargs="+", default=ALL_TOPICS)
    parser.add_argument("--limit", type=int, default=3, help="Messages to print per topic")
    args = parser.parse_args()
    tail(args.topics, args.limit)
