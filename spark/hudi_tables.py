"""
hudi_tables.py
==============
Delta Lake Table Setup — Real-Time Mobility Data Platform

Delta Lake is the industry-standard open table format used at Uber, Databricks,
and most large data platforms. It delivers the same capabilities as Apache HUDI:
  - Upserts (MERGE INTO) — update ride status / fare in-place, no duplicates
  - Deletes            — GDPR / driver account erasure
  - Incremental reads  — downstream jobs process only changed records (Change Data Feed)
  - Time travel        — query any prior snapshot by version or timestamp
  - ACID transactions  — concurrent reads + writes without corruption
  - Schema enforcement — reject records with wrong types or missing columns

Tables:
  delta_rides              (keyed on ride_id,          partitioned by city)
  delta_driver_locations   (keyed on event_id,         partitioned by city)   ← high write volume
  delta_payments           (keyed on payment_id,       partitioned by event_date)
  delta_cancellations      (keyed on cancellation_id,  partitioned by event_date)

Usage:
    python hudi_tables.py                 # create all tables + run full demo
    python hudi_tables.py --table rides   # single table
    python hudi_tables.py --skip-demo     # create tables only
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

try:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
except ImportError:
    print("Missing packages.  Run: pip3 install pyspark delta-spark")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR    = Path(__file__).resolve().parent.parent
SILVER_DIR  = BASE_DIR / "warehouse" / "silver"
DELTA_DIR   = BASE_DIR / "warehouse" / "delta"

# ---------------------------------------------------------------------------
# SparkSession with Delta extensions
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("MobilityPlatform-Delta")
        .master("local[*]")
        .config("spark.sql.extensions",          "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions",  "4")
        .config("spark.driver.memory",           "2g")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ---------------------------------------------------------------------------
# Table 1: Rides
# ---------------------------------------------------------------------------

def create_delta_rides(spark: SparkSession) -> None:
    print("\n[DELTA] Writing delta_rides...")
    df = spark.read.parquet(str(SILVER_DIR / "ride_events"))

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("city")
        .save(str(DELTA_DIR / "delta_rides"))
    )
    count = spark.read.format("delta").load(str(DELTA_DIR / "delta_rides")).count()
    print(f"  delta_rides : {count:,} records")


# ---------------------------------------------------------------------------
# Table 2: Driver locations
# ---------------------------------------------------------------------------

def create_delta_driver_locations(spark: SparkSession) -> None:
    print("\n[DELTA] Writing delta_driver_locations...")
    df = spark.read.parquet(str(SILVER_DIR / "driver_location_events"))

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("city")
        .save(str(DELTA_DIR / "delta_driver_locations"))
    )
    count = spark.read.format("delta").load(str(DELTA_DIR / "delta_driver_locations")).count()
    print(f"  delta_driver_locations : {count:,} records")


# ---------------------------------------------------------------------------
# Table 3: Payments
# ---------------------------------------------------------------------------

def create_delta_payments(spark: SparkSession) -> None:
    print("\n[DELTA] Writing delta_payments...")
    df = spark.read.parquet(str(SILVER_DIR / "payment_events"))

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .save(str(DELTA_DIR / "delta_payments"))
    )
    count = spark.read.format("delta").load(str(DELTA_DIR / "delta_payments")).count()
    print(f"  delta_payments : {count:,} records")


# ---------------------------------------------------------------------------
# Table 4: Cancellations
# ---------------------------------------------------------------------------

def create_delta_cancellations(spark: SparkSession) -> None:
    print("\n[DELTA] Writing delta_cancellations...")
    df = spark.read.parquet(str(SILVER_DIR / "cancellation_events"))

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .save(str(DELTA_DIR / "delta_cancellations"))
    )
    count = spark.read.format("delta").load(str(DELTA_DIR / "delta_cancellations")).count()
    print(f"  delta_cancellations : {count:,} records")


# ---------------------------------------------------------------------------
# Demo 1: Upsert — MERGE INTO marks in-progress rides as completed
# ---------------------------------------------------------------------------

def demo_upsert(spark: SparkSession) -> None:
    print("\n[DEMO] Upsert: marking in-progress rides as completed (MERGE INTO)...")
    from delta.tables import DeltaTable

    delta_path = str(DELTA_DIR / "delta_rides")
    dt = DeltaTable.forPath(spark, delta_path)

    # Build update payload: 500 rides flipped to completed
    updates = (
        spark.read.format("delta").load(delta_path)
        .filter(F.col("ride_status") == "in_progress")
        .limit(500)
        .withColumn("ride_status",  F.lit("completed"))
        .withColumn("fare_amount",  F.round(F.col("distance_miles") * 2.5 + 2.50, 2))
        .withColumn("dropoff_time", F.current_timestamp().cast("string"))
    )

    count = updates.count()
    if count == 0:
        print("  No in-progress rides found — skipping upsert demo")
        return

    print(f"  Merging {count} updated rides...")

    (
        dt.alias("target")
        .merge(
            updates.alias("source"),
            "target.ride_id = source.ride_id AND target.city = source.city"
        )
        .whenMatchedUpdate(set={
            "ride_status":  "source.ride_status",
            "fare_amount":  "source.fare_amount",
            "dropoff_time": "source.dropoff_time",
        })
        .execute()
    )

    completed_count = (
        spark.read.format("delta").load(delta_path)
        .filter(F.col("ride_status") == "completed")
        .count()
    )
    print(f"  Total completed rides after upsert: {completed_count:,}")


# ---------------------------------------------------------------------------
# Demo 2: Delete — GDPR erasure of one driver's location history
# ---------------------------------------------------------------------------

def demo_delete(spark: SparkSession) -> None:
    print("\n[DEMO] Delete: GDPR erasure — removing one driver's location history...")
    from delta.tables import DeltaTable

    delta_path = str(DELTA_DIR / "delta_driver_locations")
    dt = DeltaTable.forPath(spark, delta_path)

    driver_row = (
        spark.read.format("delta").load(delta_path)
        .select("driver_id").first()
    )
    if not driver_row:
        print("  Table empty — skipping delete demo")
        return

    driver_id = driver_row["driver_id"]
    before = (
        spark.read.format("delta").load(delta_path)
        .filter(F.col("driver_id") == driver_id)
        .count()
    )
    print(f"  Driver {driver_id}: {before} location pings to delete")

    dt.delete(F.col("driver_id") == driver_id)

    after = (
        spark.read.format("delta").load(delta_path)
        .filter(F.col("driver_id") == driver_id)
        .count()
    )
    print(f"  Deleted {before - after} records. Remaining: {after}")


# ---------------------------------------------------------------------------
# Demo 3: Time travel — read the table at version 0 (before any updates)
# ---------------------------------------------------------------------------

def demo_time_travel(spark: SparkSession) -> None:
    print("\n[DEMO] Time travel: reading delta_rides at version 0 (initial load)...")

    delta_path = str(DELTA_DIR / "delta_rides")

    v0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
    in_progress_v0 = v0.filter(F.col("ride_status") == "in_progress").count()

    current = spark.read.format("delta").load(delta_path)
    in_progress_now = current.filter(F.col("ride_status") == "in_progress").count()

    print(f"  in_progress rides at version 0   : {in_progress_v0:,}")
    print(f"  in_progress rides at current ver : {in_progress_now:,}")
    print(f"  Δ = {in_progress_v0 - in_progress_now:,} rides updated by MERGE")


# ---------------------------------------------------------------------------
# Demo 4: Delta history — audit log of all table operations
# ---------------------------------------------------------------------------

def demo_history(spark: SparkSession) -> None:
    print("\n[DEMO] Delta history (audit log for delta_rides):")
    from delta.tables import DeltaTable

    delta_path = str(DELTA_DIR / "delta_rides")
    dt = DeltaTable.forPath(spark, delta_path)

    (
        dt.history()
        .select("version", "timestamp", "operation", "operationParameters")
        .show(10, truncate=False)
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

TABLE_CREATORS = {
    "rides":            create_delta_rides,
    "driver_locations": create_delta_driver_locations,
    "payments":         create_delta_payments,
    "cancellations":    create_delta_cancellations,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Mobility Platform — Delta Lake Tables")
    parser.add_argument(
        "--table", choices=list(TABLE_CREATORS), default=None,
        help="Single table to create (default: all four)"
    )
    parser.add_argument(
        "--skip-demo", action="store_true",
        help="Create tables only; skip upsert / delete / time-travel demo"
    )
    args = parser.parse_args()

    DELTA_DIR.mkdir(parents=True, exist_ok=True)

    print("Starting Delta Lake table setup...")
    print(f"  Silver input : {SILVER_DIR}")
    print(f"  Delta output : {DELTA_DIR}")

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    tables = [args.table] if args.table else list(TABLE_CREATORS)
    t0 = time.time()

    for table in tables:
        TABLE_CREATORS[table](spark)

    if not args.skip_demo:
        demo_upsert(spark)
        demo_delete(spark)
        demo_time_travel(spark)
        demo_history(spark)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
