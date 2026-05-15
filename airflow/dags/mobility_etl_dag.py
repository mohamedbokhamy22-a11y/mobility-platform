"""
mobility_etl_dag.py
===================
Daily Airflow DAG for the Real-Time Mobility Data Platform.

Pipeline:
  1. generate_data      — run generator.py to produce fresh CSV event files
  2. validate_inputs    — confirm all 4 CSVs exist and are non-empty
  3. spark_bronze       — ingest raw CSVs into Parquet (typed, partitioned)
  4. spark_silver       — clean, validate, deduplicate
  5. spark_gold         — build 5 analytics tables
  6. validate_outputs   — confirm gold layer Parquet dirs are non-empty
  7. notify_success     — log completion summary

Schedule: daily at 06:00 UTC
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Paths (inside the Docker container — mapped via volumes in docker-compose)
# ---------------------------------------------------------------------------
BASE_DIR       = Path("/opt/mobility_platform")
GENERATOR_DIR  = BASE_DIR / "data_generator"
OUTPUT_DIR     = GENERATOR_DIR / "output"
WAREHOUSE_DIR  = BASE_DIR / "warehouse"
SPARK_SCRIPT   = BASE_DIR / "spark" / "etl.py"

GOLD_TABLES = [
    "daily_ride_metrics",
    "driver_utilization",
    "city_demand_heatmap",
    "payment_summary",
    "cancellation_analysis",
]

CSV_FILES = [
    "ride_events.csv",
    "driver_location_events.csv",
    "payment_events.csv",
    "cancellation_events.csv",
]

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "mobility-platform",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def validate_inputs(**context) -> None:
    """Verify all 4 CSV files exist and contain at least one data row."""
    missing, empty = [], []

    for fname in CSV_FILES:
        path = OUTPUT_DIR / fname
        if not path.exists():
            missing.append(fname)
        else:
            lines = path.read_text().splitlines()
            if len(lines) < 2:   # header only = empty
                empty.append(fname)

    if missing:
        raise FileNotFoundError(f"Missing CSV files: {missing}")
    if empty:
        raise ValueError(f"Empty CSV files (header only): {empty}")

    print(f"[validate_inputs] All {len(CSV_FILES)} CSV files present and non-empty.")
    for fname in CSV_FILES:
        size_mb = (OUTPUT_DIR / fname).stat().st_size / 1_048_576
        print(f"  {fname:<35} {size_mb:.1f} MB")


def validate_outputs(**context) -> None:
    """Verify all gold-layer Parquet directories exist and contain files."""
    failures = []

    for table in GOLD_TABLES:
        table_dir = WAREHOUSE_DIR / "gold" / table
        if not table_dir.exists():
            failures.append(f"{table}: directory missing")
            continue
        parquet_files = list(table_dir.glob("*.parquet"))
        if not parquet_files:
            failures.append(f"{table}: no Parquet files found")

    if failures:
        raise RuntimeError(f"Gold layer validation failed:\n" + "\n".join(failures))

    print(f"[validate_outputs] All {len(GOLD_TABLES)} gold tables verified.")
    for table in GOLD_TABLES:
        table_dir = WAREHOUSE_DIR / "gold" / table
        files = list(table_dir.glob("*.parquet"))
        total_mb = sum(f.stat().st_size for f in files) / 1_048_576
        print(f"  {table:<30} {len(files)} files  {total_mb:.1f} MB")


def notify_success(**context) -> None:
    """Log a completion summary with row counts from gold layer."""
    run_date = context["ds"]
    print(f"\n{'='*60}")
    print(f"Mobility ETL Pipeline — SUCCESS")
    print(f"Run date : {run_date}")
    print(f"DAG run  : {context['run_id']}")
    print(f"{'='*60}")
    print(f"Gold layer tables written to: {WAREHOUSE_DIR}/gold/")
    for table in GOLD_TABLES:
        table_dir = WAREHOUSE_DIR / "gold" / table
        files = list(table_dir.glob("*.parquet"))
        print(f"  ✓ {table}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="mobility_platform_daily_etl",
    description="Daily ETL: generate → bronze → silver → gold",
    default_args=default_args,
    schedule_interval="0 6 * * *",   # 06:00 UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["mobility", "etl", "spark"],
) as dag:

    # 1. Generate synthetic data
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=(
            f"python3 {GENERATOR_DIR}/generator.py "
            f"--rides 100000 "
            f"--output {OUTPUT_DIR} "
            f"--seed $(date +%Y%m%d)"   # daily seed = reproducible per day
        ),
    )

    # 2. Validate input CSVs
    validate_inputs_task = PythonOperator(
        task_id="validate_inputs",
        python_callable=validate_inputs,
    )

    # 3. Bronze — raw ingest
    spark_bronze = BashOperator(
        task_id="spark_bronze",
        bash_command=(
            f"python3 {SPARK_SCRIPT} "
            f"--layer bronze "
            f"--input-dir {OUTPUT_DIR} "
            f"--warehouse-dir {WAREHOUSE_DIR}"
        ),
    )

    # 4. Silver — clean & validate
    spark_silver = BashOperator(
        task_id="spark_silver",
        bash_command=(
            f"python3 {SPARK_SCRIPT} "
            f"--layer silver "
            f"--input-dir {OUTPUT_DIR} "
            f"--warehouse-dir {WAREHOUSE_DIR}"
        ),
    )

    # 5. Gold — aggregate
    spark_gold = BashOperator(
        task_id="spark_gold",
        bash_command=(
            f"python3 {SPARK_SCRIPT} "
            f"--layer gold "
            f"--input-dir {OUTPUT_DIR} "
            f"--warehouse-dir {WAREHOUSE_DIR}"
        ),
    )

    # 6. Validate gold outputs
    validate_outputs_task = PythonOperator(
        task_id="validate_outputs",
        python_callable=validate_outputs,
    )

    # 7. Notify success
    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # ---------------------------------------------------------------------------
    # Pipeline dependencies
    # ---------------------------------------------------------------------------
    (
        generate_data
        >> validate_inputs_task
        >> spark_bronze
        >> spark_silver
        >> spark_gold
        >> validate_outputs_task
        >> notify
    )
