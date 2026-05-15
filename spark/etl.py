"""
etl.py
======
Spark Batch ETL — Bronze / Silver / Gold Lakehouse Pipeline

Reads raw CSV event files from the data generator and builds three layers:
  Bronze  — raw ingest, cast to typed schema, partition by event date
  Silver  — cleaned, validated, deduplicated records
  Gold    — five aggregated metric tables ready for analytics / BI

Usage:
    # Full pipeline (all layers)
    python etl.py

    # Single layer
    python etl.py --layer bronze
    python etl.py --layer silver
    python etl.py --layer gold

    # Custom paths
    python etl.py --input-dir ../data_generator/output --warehouse-dir ../warehouse
"""

import argparse
import sys
import time
from pathlib import Path

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType, IntegerType, StringType, TimestampType,
    )
except ImportError:
    print("PySpark not installed. Run: pip3 install pyspark")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_INPUT_DIR    = "data_generator/output"
DEFAULT_WAREHOUSE    = "warehouse"

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark(warehouse_dir: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("MobilityPlatform-ETL")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.shuffle.partitions", "8")   # small local dataset
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ===========================================================================
# BRONZE — raw ingest with typed schema
# ===========================================================================

def bronze_rides(spark: SparkSession, input_dir: str, warehouse: str) -> DataFrame:
    df = spark.read.csv(f"{input_dir}/ride_events.csv", header=True, inferSchema=False)

    df = (
        df
        .withColumn("fare_amount",      F.col("fare_amount").cast(DoubleType()))
        .withColumn("surge_multiplier", F.col("surge_multiplier").cast(DoubleType()))
        .withColumn("distance_miles",   F.col("distance_miles").cast(DoubleType()))
        .withColumn("duration_minutes", F.col("duration_minutes").cast(DoubleType()))
        .withColumn("pickup_lat",       F.col("pickup_lat").cast(DoubleType()))
        .withColumn("pickup_lng",       F.col("pickup_lng").cast(DoubleType()))
        .withColumn("dropoff_lat",      F.col("dropoff_lat").cast(DoubleType()))
        .withColumn("dropoff_lng",      F.col("dropoff_lng").cast(DoubleType()))
        .withColumn("request_time",     F.to_timestamp("request_time"))
        .withColumn("pickup_time",      F.to_timestamp("pickup_time"))
        .withColumn("dropoff_time",     F.to_timestamp("dropoff_time"))
        .withColumn("event_timestamp",  F.to_timestamp("event_timestamp"))
        .withColumn("event_date",       F.to_date("event_timestamp"))
        .withColumn("_ingested_at",     F.current_timestamp())
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{warehouse}/bronze/ride_events")
    )
    return df


def bronze_locations(spark: SparkSession, input_dir: str, warehouse: str) -> DataFrame:
    df = spark.read.csv(f"{input_dir}/driver_location_events.csv", header=True, inferSchema=False)

    df = (
        df
        .withColumn("lat",             F.col("lat").cast(DoubleType()))
        .withColumn("lng",             F.col("lng").cast(DoubleType()))
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_date",      F.to_date("event_timestamp"))
        .withColumn("_ingested_at",    F.current_timestamp())
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{warehouse}/bronze/driver_location_events")
    )
    return df


def bronze_payments(spark: SparkSession, input_dir: str, warehouse: str) -> DataFrame:
    df = spark.read.csv(f"{input_dir}/payment_events.csv", header=True, inferSchema=False)

    df = (
        df
        .withColumn("amount",          F.col("amount").cast(DoubleType()))
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_date",      F.to_date("event_timestamp"))
        .withColumn("_ingested_at",    F.current_timestamp())
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{warehouse}/bronze/payment_events")
    )
    return df


def bronze_cancellations(spark: SparkSession, input_dir: str, warehouse: str) -> DataFrame:
    df = spark.read.csv(f"{input_dir}/cancellation_events.csv", header=True, inferSchema=False)

    df = (
        df
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_date",      F.to_date("event_timestamp"))
        .withColumn("_ingested_at",    F.current_timestamp())
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{warehouse}/bronze/cancellation_events")
    )
    return df


def run_bronze(spark: SparkSession, input_dir: str, warehouse: str) -> dict:
    print("\n[BRONZE] Ingesting raw CSVs...")
    counts = {}

    rides = bronze_rides(spark, input_dir, warehouse)
    counts["ride_events"] = rides.count()
    print(f"  ride_events            : {counts['ride_events']:>10,}")

    locations = bronze_locations(spark, input_dir, warehouse)
    counts["driver_location_events"] = locations.count()
    print(f"  driver_location_events : {counts['driver_location_events']:>10,}")

    payments = bronze_payments(spark, input_dir, warehouse)
    counts["payment_events"] = payments.count()
    print(f"  payment_events         : {counts['payment_events']:>10,}")

    cancellations = bronze_cancellations(spark, input_dir, warehouse)
    counts["cancellation_events"] = cancellations.count()
    print(f"  cancellation_events    : {counts['cancellation_events']:>10,}")

    return counts


# ===========================================================================
# SILVER — clean, validate, deduplicate
# ===========================================================================

def silver_rides(spark: SparkSession, warehouse: str) -> DataFrame:
    df = spark.read.parquet(f"{warehouse}/bronze/ride_events")

    df = (
        df
        # Drop rows missing primary keys or status
        .filter(
            F.col("ride_id").isNotNull() &
            F.col("rider_id").isNotNull() &
            F.col("driver_id").isNotNull() &
            F.col("ride_status").isNotNull()
        )
        # Enforce valid enum values
        .filter(F.col("ride_status").isin("completed", "cancelled", "in_progress"))
        # Fare must be positive for completed rides
        .filter(
            (F.col("ride_status") != "completed") |
            (F.col("fare_amount") > 0)
        )
        # Distance sanity check
        .filter(F.col("distance_miles").isNull() | (F.col("distance_miles") > 0))
        # Deduplicate on ride_id (keep latest by event_timestamp)
        .withColumn(
            "_row_num",
            F.row_number().over(
                __window("ride_id", "event_timestamp", desc=True)
            )
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
        # Derived fields
        .withColumn(
            "trip_hour", F.hour("request_time")
        )
        .withColumn(
            "trip_day_of_week", F.dayofweek("request_time")
        )
        .withColumn(
            "is_surge", (F.col("surge_multiplier") > 1.0).cast("boolean")
        )
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{warehouse}/silver/ride_events")
    )
    return df


def silver_locations(spark: SparkSession, warehouse: str) -> DataFrame:
    df = spark.read.parquet(f"{warehouse}/bronze/driver_location_events")

    df = (
        df
        .filter(F.col("driver_id").isNotNull() & F.col("event_timestamp").isNotNull())
        .filter(
            (F.col("lat") >= -90)  & (F.col("lat") <= 90) &
            (F.col("lng") >= -180) & (F.col("lng") <= 180)
        )
        .filter(F.col("availability_status").isin("available", "on_trip", "offline"))
        .withColumn(
            "_row_num",
            F.row_number().over(__window("event_id", "event_timestamp", desc=True))
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{warehouse}/silver/driver_location_events")
    )
    return df


def silver_payments(spark: SparkSession, warehouse: str) -> DataFrame:
    df = spark.read.parquet(f"{warehouse}/bronze/payment_events")

    df = (
        df
        .filter(F.col("payment_id").isNotNull() & F.col("ride_id").isNotNull())
        .filter(F.col("amount") > 0)
        .filter(F.col("payment_status").isin("success", "failed", "pending", "refunded"))
        .withColumn(
            "_row_num",
            F.row_number().over(__window("payment_id", "event_timestamp", desc=True))
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{warehouse}/silver/payment_events")
    )
    return df


def silver_cancellations(spark: SparkSession, warehouse: str) -> DataFrame:
    df = spark.read.parquet(f"{warehouse}/bronze/cancellation_events")

    df = (
        df
        .filter(F.col("cancellation_id").isNotNull() & F.col("ride_id").isNotNull())
        .filter(F.col("cancelled_by").isin("rider", "driver", "system"))
        .withColumn(
            "_row_num",
            F.row_number().over(__window("cancellation_id", "event_timestamp", desc=True))
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{warehouse}/silver/cancellation_events")
    )
    return df


def __window(partition_col: str, order_col: str, desc: bool = False):
    from pyspark.sql.window import Window
    order = F.col(order_col).desc() if desc else F.col(order_col).asc()
    return Window.partitionBy(partition_col).orderBy(order)


def run_silver(spark: SparkSession, warehouse: str) -> dict:
    print("\n[SILVER] Cleaning and validating...")
    counts = {}

    rides = silver_rides(spark, warehouse)
    counts["ride_events"] = rides.count()
    print(f"  ride_events            : {counts['ride_events']:>10,}")

    locations = silver_locations(spark, warehouse)
    counts["driver_location_events"] = locations.count()
    print(f"  driver_location_events : {counts['driver_location_events']:>10,}")

    payments = silver_payments(spark, warehouse)
    counts["payment_events"] = payments.count()
    print(f"  payment_events         : {counts['payment_events']:>10,}")

    cancellations = silver_cancellations(spark, warehouse)
    counts["cancellation_events"] = cancellations.count()
    print(f"  cancellation_events    : {counts['cancellation_events']:>10,}")

    return counts


# ===========================================================================
# GOLD — aggregated analytics tables
# ===========================================================================

def gold_daily_ride_metrics(spark: SparkSession, warehouse: str) -> int:
    """Daily summary: volume, revenue, surge rate, avg fare per city."""
    rides = spark.read.parquet(f"{warehouse}/silver/ride_events")

    df = (
        rides
        .groupBy("event_date", "city", "ride_status")
        .agg(
            F.count("ride_id").alias("total_rides"),
            F.sum("fare_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("distance_miles").alias("avg_distance_miles"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            F.avg("surge_multiplier").alias("avg_surge_multiplier"),
            F.sum(F.col("is_surge").cast("int")).alias("surge_rides"),
        )
        .orderBy("event_date", "city")
    )

    df.write.mode("overwrite").parquet(f"{warehouse}/gold/daily_ride_metrics")
    return df.count()


def gold_driver_utilization(spark: SparkSession, warehouse: str) -> int:
    """Per-driver daily utilization: trips completed, total earnings, active hours."""
    rides = spark.read.parquet(f"{warehouse}/silver/ride_events")

    df = (
        rides
        .filter(F.col("ride_status") == "completed")
        .groupBy("event_date", "driver_id")
        .agg(
            F.count("ride_id").alias("completed_trips"),
            F.sum("fare_amount").alias("total_earnings"),
            F.avg("fare_amount").alias("avg_fare_per_trip"),
            F.sum("distance_miles").alias("total_distance_miles"),
            F.sum("duration_minutes").alias("total_duration_minutes"),
        )
        .withColumn(
            "active_hours",
            F.round(F.col("total_duration_minutes") / 60, 2)
        )
        .orderBy("event_date", F.col("completed_trips").desc())
    )

    df.write.mode("overwrite").parquet(f"{warehouse}/gold/driver_utilization")
    return df.count()


def gold_city_demand(spark: SparkSession, warehouse: str) -> int:
    """Hourly demand heatmap per city — useful for surge pricing models."""
    rides = spark.read.parquet(f"{warehouse}/silver/ride_events")

    df = (
        rides
        .groupBy("city", "trip_day_of_week", "trip_hour")
        .agg(
            F.count("ride_id").alias("total_requests"),
            F.avg("surge_multiplier").alias("avg_surge"),
            F.sum(F.col("is_surge").cast("int")).alias("surge_count"),
            F.avg("fare_amount").alias("avg_fare"),
        )
        .withColumn(
            "surge_rate_pct",
            F.round(F.col("surge_count") / F.col("total_requests") * 100, 2)
        )
        .orderBy("city", "trip_day_of_week", "trip_hour")
    )

    df.write.mode("overwrite").parquet(f"{warehouse}/gold/city_demand_heatmap")
    return df.count()


def gold_payment_summary(spark: SparkSession, warehouse: str) -> int:
    """Daily payment method breakdown and failure rates."""
    payments = spark.read.parquet(f"{warehouse}/silver/payment_events")

    df = (
        payments
        .groupBy("event_date", "payment_method", "payment_status")
        .agg(
            F.count("payment_id").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
        )
        .orderBy("event_date", "payment_method")
    )

    df.write.mode("overwrite").parquet(f"{warehouse}/gold/payment_summary")
    return df.count()


def gold_cancellation_analysis(spark: SparkSession, warehouse: str) -> int:
    """Daily cancellation breakdown by reason and cancelled-by."""
    cancellations = spark.read.parquet(f"{warehouse}/silver/cancellation_events")

    df = (
        cancellations
        .groupBy("event_date", "cancelled_by", "reason")
        .agg(
            F.count("cancellation_id").alias("cancellation_count"),
        )
        .orderBy("event_date", F.col("cancellation_count").desc())
    )

    df.write.mode("overwrite").parquet(f"{warehouse}/gold/cancellation_analysis")
    return df.count()


def run_gold(spark: SparkSession, warehouse: str) -> dict:
    print("\n[GOLD] Building analytics tables...")
    counts = {}

    counts["daily_ride_metrics"]     = gold_daily_ride_metrics(spark, warehouse)
    print(f"  daily_ride_metrics     : {counts['daily_ride_metrics']:>10,} rows")

    counts["driver_utilization"]     = gold_driver_utilization(spark, warehouse)
    print(f"  driver_utilization     : {counts['driver_utilization']:>10,} rows")

    counts["city_demand_heatmap"]    = gold_city_demand(spark, warehouse)
    print(f"  city_demand_heatmap    : {counts['city_demand_heatmap']:>10,} rows")

    counts["payment_summary"]        = gold_payment_summary(spark, warehouse)
    print(f"  payment_summary        : {counts['payment_summary']:>10,} rows")

    counts["cancellation_analysis"]  = gold_cancellation_analysis(spark, warehouse)
    print(f"  cancellation_analysis  : {counts['cancellation_analysis']:>10,} rows")

    return counts


# ===========================================================================
# Main
# ===========================================================================

def resolve_paths(input_dir: str, warehouse_dir: str) -> tuple[str, str]:
    # __file__ is spark/etl.py, so parent is spark/, parent.parent is mobility_platform/
    base = Path(__file__).resolve().parent.parent
    inp  = str((base / input_dir).resolve())     if not Path(input_dir).is_absolute()     else input_dir
    wh   = str((base / warehouse_dir).resolve()) if not Path(warehouse_dir).is_absolute() else warehouse_dir
    return inp, wh


def main() -> None:
    parser = argparse.ArgumentParser(description="Mobility Platform — Spark Batch ETL")
    parser.add_argument(
        "--layer", choices=["bronze", "silver", "gold", "all"], default="all",
        help="Which layer(s) to run (default: all)"
    )
    parser.add_argument(
        "--input-dir", default=DEFAULT_INPUT_DIR,
        help="Directory containing the CSV files from data_generator"
    )
    parser.add_argument(
        "--warehouse-dir", default=DEFAULT_WAREHOUSE,
        help="Root directory for Parquet output"
    )
    args = parser.parse_args()

    input_dir, warehouse = resolve_paths(args.input_dir, args.warehouse_dir)
    Path(warehouse).mkdir(parents=True, exist_ok=True)

    print(f"Input  : {input_dir}")
    print(f"Output : {warehouse}")

    spark = get_spark(warehouse)
    spark.sparkContext.setLogLevel("WARN")

    t0 = time.time()

    if args.layer in ("bronze", "all"):
        run_bronze(spark, input_dir, warehouse)

    if args.layer in ("silver", "all"):
        run_silver(spark, warehouse)

    if args.layer in ("gold", "all"):
        run_gold(spark, warehouse)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")

    spark.stop()


if __name__ == "__main__":
    main()
