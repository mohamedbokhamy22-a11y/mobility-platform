"""
sql_assistant.py
================
GenAI SQL Assistant — Real-Time Mobility Data Platform

Ask questions in plain English. Claude translates them into SQL,
executes them against the Delta Lake tables via Spark, and explains
the results in plain language.

Architecture:
  User question → Claude (text-to-SQL) → SparkSQL on Delta tables → Claude (results explanation) → Answer

Tables available:
  rides              — 100k ride events (city, status, fare, surge, distance)
  driver_locations   — 250k GPS pings (driver, city, lat/lng, availability)
  payments           — 82k payment records (method, status, amount)
  cancellations      — 15k cancellation events (reason, cancelled_by)
  daily_ride_metrics — gold-layer daily aggregates
  city_demand        — hourly surge heatmap per city
  payment_summary    — daily payment method breakdown
  driver_utilization — per-driver daily earnings & trips

Usage:
    python sql_assistant.py                        # interactive REPL
    python sql_assistant.py --question "Which city had the most rides?"
    python sql_assistant.py --demo                 # run 6 preset demo questions
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import textwrap
from pathlib import Path

try:
    import anthropic
except ImportError:
    print("Run: pip3 install anthropic")
    sys.exit(1)

try:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except ImportError:
    print("Run: pip3 install pyspark delta-spark")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR  = Path(__file__).resolve().parent.parent
DELTA_DIR = BASE_DIR / "warehouse" / "delta"
GOLD_DIR  = BASE_DIR / "warehouse" / "gold"

# ---------------------------------------------------------------------------
# Demo questions that showcase different query patterns
# ---------------------------------------------------------------------------
DEMO_QUESTIONS = [
    "Which city had the highest total revenue from completed rides?",
    "What percentage of rides were cancelled, broken down by city?",
    "Which hour of the day has the highest average surge multiplier across all cities?",
    "What is the most common cancellation reason, and who cancels most — riders or drivers?",
    "Show me the top 5 payment methods by total transaction volume.",
    "Which city has the most available drivers on average?",
]

# ---------------------------------------------------------------------------
# Schema context fed to Claude
# ---------------------------------------------------------------------------
SCHEMA_CONTEXT = """
You have access to the following SparkSQL tables (registered as temp views):

TABLE: rides
  ride_id          STRING    -- unique trip ID
  rider_id         STRING
  driver_id        STRING
  city             STRING    -- New York, San Francisco, Chicago, Los Angeles, Miami, Seattle, Austin, Boston
  pickup_lat       DOUBLE
  pickup_lng       DOUBLE
  dropoff_lat      DOUBLE
  dropoff_lng      DOUBLE
  request_time     TIMESTAMP
  pickup_time      TIMESTAMP
  dropoff_time     TIMESTAMP  -- NULL if not completed
  ride_status      STRING    -- 'completed', 'cancelled', 'in_progress'
  fare_amount      DOUBLE    -- NULL if not completed
  surge_multiplier DOUBLE
  distance_miles   DOUBLE
  duration_minutes DOUBLE
  payment_type     STRING    -- 'card', 'cash', 'uber_cash', 'paypal'
  event_date       DATE
  trip_hour        INT       -- 0-23
  trip_day_of_week INT       -- 1=Sunday, 7=Saturday
  is_surge         BOOLEAN

TABLE: driver_locations
  event_id            STRING
  driver_id           STRING
  city                STRING
  lat                 DOUBLE
  lng                 DOUBLE
  availability_status STRING  -- 'available', 'on_trip', 'offline'
  event_date          DATE

TABLE: payments
  payment_id      STRING
  ride_id         STRING
  payment_status  STRING  -- 'success', 'failed', 'pending', 'refunded'
  amount          DOUBLE
  currency        STRING
  payment_method  STRING  -- 'card', 'cash', 'uber_cash', 'paypal'
  event_date      DATE

TABLE: cancellations
  cancellation_id STRING
  ride_id         STRING
  cancelled_by    STRING  -- 'rider', 'driver', 'system'
  reason          STRING  -- 'rider_cancelled', 'driver_cancelled', 'no_driver_available', 'rider_no_show'
  event_date      DATE

TABLE: daily_ride_metrics
  event_date           DATE
  city                 STRING
  ride_status          STRING
  total_rides          BIGINT
  total_revenue        DOUBLE
  avg_fare             DOUBLE
  avg_distance_miles   DOUBLE
  avg_duration_minutes DOUBLE
  avg_surge_multiplier DOUBLE
  surge_rides          BIGINT

TABLE: city_demand
  city             STRING
  trip_day_of_week INT
  trip_hour        INT
  total_requests   BIGINT
  avg_surge        DOUBLE
  surge_count      BIGINT
  avg_fare         DOUBLE
  surge_rate_pct   DOUBLE

TABLE: payment_summary
  event_date        DATE
  payment_method    STRING
  payment_status    STRING
  transaction_count BIGINT
  total_amount      DOUBLE
  avg_amount        DOUBLE

TABLE: driver_utilization
  event_date            DATE
  driver_id             STRING
  completed_trips       BIGINT
  total_earnings        DOUBLE
  avg_fare_per_trip     DOUBLE
  total_distance_miles  DOUBLE
  total_duration_minutes DOUBLE
  active_hours          DOUBLE

Rules:
- Use SparkSQL syntax (not PostgreSQL or MySQL)
- Do NOT use backtick-quoted identifiers unless necessary
- Prefer the gold-layer tables (daily_ride_metrics, city_demand, etc.) for aggregation questions — they are pre-computed and much faster
- Use the raw tables (rides, payments, etc.) only for record-level or ad-hoc questions
- Always add ORDER BY and LIMIT to keep results readable (LIMIT 20 max)
- Round DOUBLE columns to 2 decimal places with ROUND(col, 2)
- For percentage calculations use: ROUND(100.0 * numerator / denominator, 1)
"""

SYSTEM_PROMPT = f"""You are an expert data analyst and SQL engineer for a real-time mobility platform (similar to Uber).
Your job is to translate natural language questions into SparkSQL queries and explain results clearly.

{SCHEMA_CONTEXT}

When generating SQL:
1. Output ONLY a single SQL query — no markdown fences, no explanation, just the raw SQL.
2. The query must be valid SparkSQL that will execute without errors.
3. Use aliases to make column names self-explanatory in the output.

When explaining results:
- Be concise (3-5 sentences max)
- Highlight the single most interesting insight
- Use business language, not SQL jargon
- If the numbers reveal something unexpected, call it out
"""


# ---------------------------------------------------------------------------
# Spark setup
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("MobilityPlatform-SQLAssistant")
        .master("local[*]")
        .config("spark.sql.extensions",           "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions",   "4")
        .config("spark.driver.memory",            "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def register_tables(spark: SparkSession) -> None:
    """Load all Delta + Gold Parquet tables as Spark temp views."""
    delta_tables = {
        "rides":            DELTA_DIR / "delta_rides",
        "driver_locations": DELTA_DIR / "delta_driver_locations",
        "payments":         DELTA_DIR / "delta_payments",
        "cancellations":    DELTA_DIR / "delta_cancellations",
    }
    gold_tables = {
        "daily_ride_metrics": GOLD_DIR / "daily_ride_metrics",
        "city_demand":        GOLD_DIR / "city_demand_heatmap",
        "payment_summary":    GOLD_DIR / "payment_summary",
        "driver_utilization": GOLD_DIR / "driver_utilization",
    }

    for view_name, path in delta_tables.items():
        if path.exists():
            spark.read.format("delta").load(str(path)).createOrReplaceTempView(view_name)

    for view_name, path in gold_tables.items():
        if path.exists():
            spark.read.parquet(str(path)).createOrReplaceTempView(view_name)


# ---------------------------------------------------------------------------
# Claude API calls
# ---------------------------------------------------------------------------

def generate_sql(client: anthropic.Anthropic, question: str) -> str:
    """Ask Claude to translate a natural language question into SparkSQL."""
    response = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=1024,
        thinking={"type": "adaptive"},
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Generate a SparkSQL query to answer this question:\n\n{question}"
            }
        ],
    )
    # Extract text from response (may include thinking blocks)
    sql = ""
    for block in response.content:
        if block.type == "text":
            sql = block.text.strip()
            break

    # Strip any accidental markdown fences
    sql = re.sub(r"^```(?:sql)?\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\s*```$", "", sql)
    return sql.strip()


def explain_results(client: anthropic.Anthropic, question: str, sql: str, results_text: str) -> str:
    """Ask Claude to explain the query results in plain English."""
    response = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=512,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Question: {question}\n\n"
                    f"SQL executed:\n{sql}\n\n"
                    f"Results:\n{results_text}\n\n"
                    "Explain these results in 3-5 sentences. Highlight the most interesting insight."
                )
            }
        ],
    )
    return response.content[0].text.strip()


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def run_query(spark: SparkSession, sql: str, max_rows: int = 20) -> tuple[str, int]:
    """Execute SQL and return (formatted string, row count)."""
    df = spark.sql(sql)
    rows = df.limit(max_rows).collect()
    columns = df.columns

    if not rows:
        return "(no results)", 0

    # Format as a clean text table
    col_widths = [max(len(c), max((len(str(r[c])) for r in rows), default=0)) for c in columns]
    header = "  ".join(c.ljust(w) for c, w in zip(columns, col_widths))
    divider = "  ".join("-" * w for w in col_widths)
    lines = [header, divider]
    for row in rows:
        lines.append("  ".join(str(row[c]).ljust(w) for c, w in zip(columns, col_widths)))

    return "\n".join(lines), len(rows)


# ---------------------------------------------------------------------------
# Single question handler
# ---------------------------------------------------------------------------

def answer_question(
    client: anthropic.Anthropic,
    spark: SparkSession,
    question: str,
    verbose: bool = False,
) -> None:
    print(f"\n{'='*70}")
    print(f"Question: {question}")
    print("=" * 70)

    # Step 1: Generate SQL
    print("Generating SQL...", end="", flush=True)
    sql = generate_sql(client, question)
    print(" done")

    if verbose:
        print(f"\nSQL:\n{sql}\n")
    else:
        # Show a compact one-liner preview
        preview = " ".join(sql.split())[:120]
        print(f"SQL: {preview}{'...' if len(sql) > 120 else ''}")

    # Step 2: Execute
    print("Running query...", end="", flush=True)
    try:
        results_text, nrows = run_query(spark, sql)
        print(f" {nrows} row(s)\n")
    except Exception as e:
        print(f"\nQuery error: {e}")
        return

    print("Results:")
    print(results_text)

    # Step 3: Explain
    print("\nExplaining...", end="", flush=True)
    explanation = explain_results(client, question, sql, results_text)
    print(" done\n")

    print("Insight:")
    print(textwrap.fill(explanation, width=70, initial_indent="  ", subsequent_indent="  "))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Mobility Platform — GenAI SQL Assistant")
    parser.add_argument("--question", "-q", type=str, help="Single question to answer then exit")
    parser.add_argument("--demo",     action="store_true", help="Run all demo questions")
    parser.add_argument("--verbose",  action="store_true", help="Print full SQL query")
    args = parser.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: Set your API key first:\n  export ANTHROPIC_API_KEY=your-key-here")
        sys.exit(1)

    print("Starting SQL Assistant...")
    print("Loading Spark + Delta tables...", end="", flush=True)
    spark = get_spark()
    register_tables(spark)
    print(" ready")

    client = anthropic.Anthropic(api_key=api_key)

    if args.question:
        answer_question(client, spark, args.question, verbose=args.verbose)

    elif args.demo:
        print(f"\nRunning {len(DEMO_QUESTIONS)} demo questions...")
        for q in DEMO_QUESTIONS:
            answer_question(client, spark, q, verbose=args.verbose)

    else:
        # Interactive REPL
        print("\nMobility Platform SQL Assistant")
        print("Type a question in plain English. Type 'quit' to exit.\n")
        while True:
            try:
                question = input("You: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye!")
                break
            if not question:
                continue
            if question.lower() in ("quit", "exit", "q"):
                print("Goodbye!")
                break
            answer_question(client, spark, question, verbose=args.verbose)

    spark.stop()


if __name__ == "__main__":
    main()
