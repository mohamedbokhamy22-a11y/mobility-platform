"""
streaming.py
============
Spark Structured Streaming — Real-Time Mobility Event Processing

Reads live events from four Kafka topics and maintains continuously updated
aggregates in the warehouse/streaming/ directory:

  ride_events              → real-time ride request stream + city-level windowed KPIs
  driver_location_events   → live driver availability counts per city
  payment_events           → rolling payment success/failure rates
  cancellation_events      → cancellation reason breakdown

The Kafka–Spark connector JAR is downloaded automatically on first run via
the --packages flag wired into the SparkSession config.

Usage:
    # Stream all topics (default)
    python streaming.py

    # Stream a single topic
    python streaming.py --topic ride_events

    # Run for N seconds then stop (useful for demos/testing)
    python streaming.py --runtime 60
"""

import argparse
import json
import signal
import sys
import time
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType, StringType, StructField, StructType, TimestampType,
    )
except ImportError:
    print("PySpark not installed. Run: pip3 install pyspark")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP    = "localhost:9092"
WAREHOUSE_DIR      = Path(__file__).resolve().parent.parent / "warehouse"
CHECKPOINT_DIR     = WAREHOUSE_DIR / "checkpoints"

# Kafka–Spark connector — must match PySpark version (4.1.x uses Scala 2.13)
KAFKA_PACKAGE      = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

TOPICS = [
    "ride_events",
    "driver_location_events",
    "payment_events",
    "cancellation_events",
]

# ---------------------------------------------------------------------------
# Schemas for JSON payloads
# ---------------------------------------------------------------------------
RIDE_SCHEMA = StructType([
    StructField("ride_id",          StringType()),
    StructField("rider_id",         StringType()),
    StructField("driver_id",        StringType()),
    StructField("city",             StringType()),
    StructField("ride_status",      StringType()),
    StructField("fare_amount",      DoubleType()),
    StructField("surge_multiplier", DoubleType()),
    StructField("distance_miles",   DoubleType()),
    StructField("duration_minutes", DoubleType()),
    StructField("payment_type",     StringType()),
    StructField("event_timestamp",  StringType()),
])

LOCATION_SCHEMA = StructType([
    StructField("event_id",            StringType()),
    StructField("driver_id",           StringType()),
    StructField("city",                StringType()),
    StructField("lat",                 DoubleType()),
    StructField("lng",                 DoubleType()),
    StructField("availability_status", StringType()),
    StructField("event_timestamp",     StringType()),
])

PAYMENT_SCHEMA = StructType([
    StructField("payment_id",      StringType()),
    StructField("ride_id",         StringType()),
    StructField("payment_status",  StringType()),
    StructField("amount",          DoubleType()),
    StructField("currency",        StringType()),
    StructField("payment_method",  StringType()),
    StructField("event_timestamp", StringType()),
])

CANCELLATION_SCHEMA = StructType([
    StructField("cancellation_id", StringType()),
    StructField("ride_id",         StringType()),
    StructField("cancelled_by",    StringType()),
    StructField("reason",          StringType()),
    StructField("event_timestamp", StringType()),
])


# ---------------------------------------------------------------------------
# SparkSession with Kafka connector
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MobilityPlatform-Streaming")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Helper: read a Kafka topic as a streaming DataFrame
# ---------------------------------------------------------------------------

def read_kafka(spark: SparkSession, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # consume all messages in Kafka
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_json(df, schema: StructType):
    """Decode Kafka value bytes → JSON → typed struct."""
    return (
        df
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
    )


# ---------------------------------------------------------------------------
# Stream 1: Ride events — 5-minute windowed city KPIs
# ---------------------------------------------------------------------------

def stream_rides(spark: SparkSession) -> object:
    raw = read_kafka(spark, "ride_events")
    rides = parse_json(raw, RIDE_SCHEMA)

    windowed = (
        rides
        .withWatermark("event_ts", "90 days")
        .groupBy(
            F.window("event_ts", "5 minutes"),
            "city",
            "ride_status",
        )
        .agg(
            F.count("ride_id").alias("ride_count"),
            F.sum("fare_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("surge_multiplier").alias("avg_surge"),
            F.avg("distance_miles").alias("avg_distance"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city", "ride_status",
            "ride_count", "total_revenue", "avg_fare",
            "avg_surge", "avg_distance",
        )
    )

    checkpoint = str(CHECKPOINT_DIR / "ride_windows")
    output     = str(WAREHOUSE_DIR / "streaming" / "ride_windows")

    return (
        windowed.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="30 seconds")
        .start()
    )


# ---------------------------------------------------------------------------
# Stream 2: Driver locations — live availability per city (console output)
# ---------------------------------------------------------------------------

def stream_driver_locations(spark: SparkSession) -> object:
    raw       = read_kafka(spark, "driver_location_events")
    locations = parse_json(raw, LOCATION_SCHEMA)

    availability = (
        locations
        .withWatermark("event_ts", "90 days")
        .groupBy(
            F.window("event_ts", "1 minute"),
            "city",
            "availability_status",
        )
        .agg(F.count("driver_id").alias("driver_count"))
        .select(
            F.col("window.start").alias("window_start"),
            "city", "availability_status", "driver_count",
        )
    )

    checkpoint = str(CHECKPOINT_DIR / "driver_availability")
    output     = str(WAREHOUSE_DIR / "streaming" / "driver_availability")

    return (
        availability.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="30 seconds")
        .start()
    )


# ---------------------------------------------------------------------------
# Stream 3: Payments — rolling success/failure rates per method
# ---------------------------------------------------------------------------

def stream_payments(spark: SparkSession) -> object:
    raw      = read_kafka(spark, "payment_events")
    payments = parse_json(raw, PAYMENT_SCHEMA)

    rates = (
        payments
        .withWatermark("event_ts", "90 days")
        .groupBy(
            F.window("event_ts", "5 minutes"),
            "payment_method",
            "payment_status",
        )
        .agg(
            F.count("payment_id").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "payment_method", "payment_status",
            "txn_count", "total_amount", "avg_amount",
        )
    )

    checkpoint = str(CHECKPOINT_DIR / "payment_rates")
    output     = str(WAREHOUSE_DIR / "streaming" / "payment_rates")

    return (
        rates.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="30 seconds")
        .start()
    )


# ---------------------------------------------------------------------------
# Stream 4: Cancellations — reason breakdown
# ---------------------------------------------------------------------------

def stream_cancellations(spark: SparkSession) -> object:
    raw           = read_kafka(spark, "cancellation_events")
    cancellations = parse_json(raw, CANCELLATION_SCHEMA)

    breakdown = (
        cancellations
        .withWatermark("event_ts", "90 days")
        .groupBy(
            F.window("event_ts", "5 minutes"),
            "cancelled_by",
            "reason",
        )
        .agg(F.count("cancellation_id").alias("cancellation_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "cancelled_by", "reason", "cancellation_count",
        )
    )

    checkpoint = str(CHECKPOINT_DIR / "cancellation_breakdown")
    output     = str(WAREHOUSE_DIR / "streaming" / "cancellation_breakdown")

    return (
        breakdown.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="30 seconds")
        .start()
    )


# ---------------------------------------------------------------------------
# Live console monitor — prints a summary every 30s
# ---------------------------------------------------------------------------

def stream_ride_console(spark: SparkSession) -> object:
    """Secondary stream: prints live city demand to console every 30s."""
    raw   = read_kafka(spark, "ride_events")
    rides = parse_json(raw, RIDE_SCHEMA)

    summary = (
        rides
        .withWatermark("event_ts", "90 days")
        .groupBy(
            F.window("event_ts", "1 minute"),
            "city",
        )
        .agg(
            F.count("ride_id").alias("requests"),
            F.avg("surge_multiplier").alias("avg_surge"),
        )
        .select(
            F.col("window.start").alias("minute"),
            "city", "requests",
            F.round("avg_surge", 2).alias("avg_surge"),
        )
        .orderBy("minute", "city")
    )

    return (
        summary.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "30")
        .trigger(processingTime="30 seconds")
        .start()
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

STREAM_BUILDERS = {
    "ride_events":             (stream_rides, stream_ride_console),
    "driver_location_events":  (stream_driver_locations,),
    "payment_events":          (stream_payments,),
    "cancellation_events":     (stream_cancellations,),
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Mobility Platform — Spark Structured Streaming")
    parser.add_argument(
        "--topic", choices=TOPICS, default=None,
        help="Single topic to stream (default: all four)"
    )
    parser.add_argument(
        "--runtime", type=int, default=None,
        help="Stop after N seconds (default: run until Ctrl-C)"
    )
    args = parser.parse_args()

    topics = [args.topic] if args.topic else TOPICS

    # Create output dirs
    for subdir in ["streaming", "checkpoints"]:
        (WAREHOUSE_DIR / subdir).mkdir(parents=True, exist_ok=True)

    print("Starting Spark Structured Streaming...")
    print(f"  Kafka : {KAFKA_BOOTSTRAP}")
    print(f"  Topics: {', '.join(topics)}")
    print(f"  Output: {WAREHOUSE_DIR}/streaming/")
    print()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    queries = []
    for topic in topics:
        for builder in STREAM_BUILDERS.get(topic, []):
            q = builder(spark)
            queries.append(q)
            print(f"  [started] {topic} → {q.name or q.id}")

    if not queries:
        print("No streaming queries started.")
        spark.stop()
        return

    print(f"\n{len(queries)} streaming queries running. Press Ctrl-C to stop.\n")

    # Graceful shutdown on Ctrl-C
    def _shutdown(sig, frame):
        print("\nShutting down streams...")
        for q in queries:
            q.stop()
        spark.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    if args.runtime:
        time.sleep(args.runtime)
        _shutdown(None, None)
    else:
        # Block until all queries terminate (or Ctrl-C)
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
