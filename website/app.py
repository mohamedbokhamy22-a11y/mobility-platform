"""
app.py — Mobility Platform Dashboard
Streamlit web app showcasing the full platform.
Run: streamlit run website/app.py
"""

import os
import re
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

BASE_DIR   = Path(__file__).resolve().parent.parent
GOLD_DIR   = BASE_DIR / "warehouse" / "gold"
DELTA_DIR  = BASE_DIR / "warehouse" / "delta"
SILVER_DIR = BASE_DIR / "warehouse" / "silver"
STREAM_DIR = BASE_DIR / "warehouse" / "streaming"

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Mobility Data Platform",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## 🚗 Mobility Platform")
    st.markdown("Real-time data engineering platform modeled after Uber's architecture.")
    st.markdown("---")
    page = st.radio("Navigate", [
        "🏠 Overview",
        "📊 Analytics",
        "⚡ Streaming",
        "🗄️ Delta Lake",
        "🤖 AI SQL Assistant",
    ])
    st.markdown("---")
    st.markdown("**GitHub**")
    st.markdown("[mobility-platform](https://github.com/mohamedbokhamy22-a11y/mobility-platform)")
    st.markdown("**Stack**")
    st.caption("Kafka · PySpark · Spark Streaming · Delta Lake · Airflow · Claude AI · Docker")


# ---------------------------------------------------------------------------
# Helper: load parquet safely
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def load_parquet(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    files = list(p.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


# ===========================================================================
# PAGE 1 — OVERVIEW
# ===========================================================================
if page == "🏠 Overview":
    st.title("🚗 Real-Time Mobility Data Platform")
    st.markdown("##### An end-to-end data engineering platform modeled after Uber's architecture")
    st.markdown("---")

    # KPI cards
    rides_df = load_parquet(str(SILVER_DIR / "ride_events"))
    pay_df   = load_parquet(str(SILVER_DIR / "payment_events"))
    loc_df   = load_parquet(str(SILVER_DIR / "driver_location_events"))
    can_df   = load_parquet(str(SILVER_DIR / "cancellation_events"))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Rides",          f"{len(rides_df):,}"  if not rides_df.empty else "—")
    c2.metric("Payment Events",       f"{len(pay_df):,}"    if not pay_df.empty  else "—")
    c3.metric("Driver Location Pings",f"{len(loc_df):,}"    if not loc_df.empty  else "—")
    c4.metric("Cancellations",        f"{len(can_df):,}"    if not can_df.empty  else "—")

    if not rides_df.empty and "fare_amount" in rides_df.columns:
        total_rev = rides_df["fare_amount"].dropna().sum()
        avg_fare  = rides_df["fare_amount"].dropna().mean()
        avg_surge = rides_df["surge_multiplier"].dropna().mean() if "surge_multiplier" in rides_df.columns else 0
        cancel_rate = len(can_df) / len(rides_df) * 100 if len(rides_df) > 0 else 0

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Total Revenue",    f"${total_rev:,.0f}")
        c6.metric("Avg Fare",         f"${avg_fare:.2f}")
        c7.metric("Avg Surge",        f"{avg_surge:.2f}x")
        c8.metric("Cancellation Rate",f"{cancel_rate:.1f}%")

    st.markdown("---")

    # Architecture diagram
    st.subheader("🏗️ Architecture")
    st.markdown("""
    ```
    ┌─────────────────────────────────────────────────────────────────┐
    │                    DATA SOURCES                                  │
    │   Synthetic Generator → 447,000+ events across 4 event types    │
    └─────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │                 KAFKA (Message Queue)                            │
    │   ride_events · driver_location_events · payment_events ·       │
    │   cancellation_events          37,000+ msg/sec                  │
    └──────────┬──────────────────────────────┬───────────────────────┘
               │                              │
               ▼                              ▼
    ┌──────────────────────┐    ┌─────────────────────────────────────┐
    │  SPARK BATCH ETL     │    │   SPARK STRUCTURED STREAMING        │
    │  Bronze → Silver →   │    │   5-min windowed KPIs per city      │
    │  Gold (Parquet)      │    │   Late-data watermarking            │
    └──────────┬───────────┘    └─────────────────────────────────────┘
               │
               ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │              DELTA LAKE (Open Table Format)                      │
    │   MERGE INTO upserts · Hard deletes · Time travel               │
    └──────────┬──────────────────────────────────────────────────────┘
               │
               ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │         AIRFLOW DAG  (Daily Orchestration)                       │
    │  generate → validate → bronze → silver → gold → validate →      │
    │  notify                                                          │
    └──────────┬──────────────────────────────────────────────────────┘
               │
               ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │              GENAI SQL ASSISTANT (Claude API)                    │
    │   Natural language → SparkSQL → Results → Plain English          │
    └─────────────────────────────────────────────────────────────────┘
    ```
    """)

    st.markdown("---")
    st.subheader("🛠️ Tech Stack")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**Ingestion & Streaming**")
        st.markdown("- Apache Kafka\n- kafka-python\n- Spark Structured Streaming")
    with col2:
        st.markdown("**Processing & Storage**")
        st.markdown("- PySpark (Bronze/Silver/Gold)\n- Delta Lake (ACID tables)\n- Apache Parquet")
    with col3:
        st.markdown("**Orchestration & AI**")
        st.markdown("- Apache Airflow\n- Claude API (Anthropic)\n- Docker Compose")


# ===========================================================================
# PAGE 2 — ANALYTICS
# ===========================================================================
elif page == "📊 Analytics":
    st.title("📊 Analytics Dashboard")
    st.markdown("Powered by the Gold layer — pre-aggregated PySpark outputs")
    st.markdown("---")

    metrics_df = load_parquet(str(GOLD_DIR / "daily_ride_metrics"))
    demand_df  = load_parquet(str(GOLD_DIR / "city_demand_heatmap"))
    pay_df     = load_parquet(str(GOLD_DIR / "payment_summary"))
    util_df    = load_parquet(str(GOLD_DIR / "driver_utilization"))

    if metrics_df.empty:
        st.warning("Gold layer not found. Run `python3 spark/etl.py` first.")
        st.stop()

    tab1, tab2, tab3, tab4 = st.tabs(["🏙️ City Revenue", "⚡ Surge Heatmap", "💳 Payments", "🚗 Driver Utilization"])

    # --- City Revenue ---
    with tab1:
        st.subheader("Total Revenue by City")
        city_rev = (
            metrics_df[metrics_df["ride_status"] == "completed"]
            .groupby("city")["total_revenue"]
            .sum()
            .reset_index()
            .sort_values("total_revenue", ascending=False)
        )
        city_rev["total_revenue"] = city_rev["total_revenue"].round(0)

        fig = px.bar(
            city_rev, x="city", y="total_revenue",
            color="total_revenue",
            color_continuous_scale="Blues",
            labels={"total_revenue": "Revenue ($)", "city": "City"},
            title="Total Revenue from Completed Rides by City",
        )
        fig.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Ride Volume by Status")
            status_counts = metrics_df.groupby("ride_status")["total_rides"].sum().reset_index()
            fig2 = px.pie(status_counts, names="ride_status", values="total_rides",
                          color_discrete_sequence=px.colors.qualitative.Set2)
            st.plotly_chart(fig2, use_container_width=True)

        with col2:
            st.subheader("Avg Fare by City")
            avg_fare = (
                metrics_df[metrics_df["ride_status"] == "completed"]
                .groupby("city")["avg_fare"]
                .mean()
                .reset_index()
                .sort_values("avg_fare", ascending=True)
            )
            fig3 = px.bar(avg_fare, x="avg_fare", y="city", orientation="h",
                          color="avg_fare", color_continuous_scale="Greens",
                          labels={"avg_fare": "Avg Fare ($)", "city": ""})
            fig3.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig3, use_container_width=True)

    # --- Surge Heatmap ---
    with tab2:
        st.subheader("Hourly Surge Multiplier Heatmap")
        if not demand_df.empty:
            pivot = demand_df.groupby(["city", "trip_hour"])["avg_surge"].mean().reset_index()
            pivot_wide = pivot.pivot(index="city", columns="trip_hour", values="avg_surge")

            fig = px.imshow(
                pivot_wide,
                color_continuous_scale="RdYlGn_r",
                labels={"x": "Hour of Day", "y": "City", "color": "Avg Surge"},
                title="Average Surge Multiplier by City and Hour",
                aspect="auto",
            )
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Peak Surge Hours")
            peak = demand_df.groupby("trip_hour")["avg_surge"].mean().reset_index()
            fig2 = px.line(peak, x="trip_hour", y="avg_surge",
                           labels={"trip_hour": "Hour of Day", "avg_surge": "Avg Surge Multiplier"},
                           markers=True)
            fig2.add_hline(y=1.5, line_dash="dash", line_color="red", annotation_text="High surge threshold")
            st.plotly_chart(fig2, use_container_width=True)

    # --- Payments ---
    with tab3:
        st.subheader("Payment Method Breakdown")
        if not pay_df.empty:
            method_totals = pay_df.groupby("payment_method")["transaction_count"].sum().reset_index()
            fig = px.pie(method_totals, names="payment_method", values="transaction_count",
                         title="Transactions by Payment Method",
                         color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Payment Success vs Failure Rate")
            status_totals = pay_df.groupby("payment_status")["transaction_count"].sum().reset_index()
            fig2 = px.bar(status_totals, x="payment_status", y="transaction_count",
                          color="payment_status",
                          color_discrete_map={"success": "#2ecc71", "failed": "#e74c3c",
                                              "pending": "#f39c12", "refunded": "#95a5a6"},
                          labels={"transaction_count": "Transactions", "payment_status": "Status"})
            st.plotly_chart(fig2, use_container_width=True)

    # --- Driver Utilization ---
    with tab4:
        st.subheader("Driver Utilization Distribution")
        if not util_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.histogram(util_df, x="completed_trips", nbins=40,
                                   title="Distribution of Completed Trips per Driver per Day",
                                   labels={"completed_trips": "Completed Trips"},
                                   color_discrete_sequence=["#3498db"])
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig2 = px.histogram(util_df, x="total_earnings", nbins=40,
                                    title="Distribution of Daily Earnings per Driver",
                                    labels={"total_earnings": "Daily Earnings ($)"},
                                    color_discrete_sequence=["#2ecc71"])
                st.plotly_chart(fig2, use_container_width=True)

            top_drivers = util_df.groupby("driver_id")["total_earnings"].sum().nlargest(10).reset_index()
            top_drivers.columns = ["Driver ID", "Total Earnings ($)"]
            top_drivers["Total Earnings ($)"] = top_drivers["Total Earnings ($)"].round(2)
            top_drivers["Driver ID"] = top_drivers["Driver ID"].str[:20] + "..."
            st.subheader("Top 10 Drivers by Earnings")
            st.dataframe(top_drivers, use_container_width=True, hide_index=True)


# ===========================================================================
# PAGE 3 — STREAMING
# ===========================================================================
elif page == "⚡ Streaming":
    st.title("⚡ Spark Structured Streaming")
    st.markdown("Real-time windowed aggregations from Kafka topics")
    st.markdown("---")

    stream_tables = {
        "🚗 Ride Windows (5-min KPIs)":      STREAM_DIR / "ride_windows",
        "🧑‍✈️ Driver Availability (1-min)":  STREAM_DIR / "driver_availability",
        "💳 Payment Rates (5-min)":           STREAM_DIR / "payment_rates",
        "❌ Cancellation Breakdown (5-min)":  STREAM_DIR / "cancellation_breakdown",
    }

    any_data = False
    for label, path in stream_tables.items():
        df = load_parquet(str(path))
        if df.empty:
            continue
        any_data = True
        st.subheader(label)

        if "window_start" in df.columns:
            df["window_start"] = pd.to_datetime(df["window_start"])
            df = df.sort_values("window_start", ascending=False)

        st.dataframe(df.head(20), use_container_width=True, hide_index=True)

        if label == "🚗 Ride Windows (5-min KPIs)" and "city" in df.columns and "ride_count" in df.columns:
            city_totals = df.groupby("city")["ride_count"].sum().reset_index().sort_values("ride_count", ascending=False)
            fig = px.bar(city_totals, x="city", y="ride_count",
                         title="Total Ride Requests per City (all streaming windows)",
                         color="ride_count", color_continuous_scale="Blues")
            fig.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

    if not any_data:
        st.info("No streaming data yet. Run the streaming job first:")
        st.code("python3 spark/streaming.py --runtime 120", language="bash")
        st.markdown("Make sure Kafka is running (`docker compose up -d`) and has data (`python3 kafka/producer.py`).")


# ===========================================================================
# PAGE 4 — DELTA LAKE
# ===========================================================================
elif page == "🗄️ Delta Lake":
    st.title("🗄️ Delta Lake Tables")
    st.markdown("ACID-compliant open table format with upserts, deletes, and time travel")
    st.markdown("---")

    tables = {
        "delta_rides":            (DELTA_DIR / "delta_rides",            "ride_id"),
        "delta_driver_locations": (DELTA_DIR / "delta_driver_locations",  "event_id"),
        "delta_payments":         (DELTA_DIR / "delta_payments",          "payment_id"),
        "delta_cancellations":    (DELTA_DIR / "delta_cancellations",     "cancellation_id"),
    }

    cols = st.columns(4)
    for i, (name, (path, key)) in enumerate(tables.items()):
        df = load_parquet(str(path))
        cols[i].metric(name.replace("delta_", "").replace("_", " ").title(), f"{len(df):,} rows")

    st.markdown("---")

    selected = st.selectbox("Preview a table", list(tables.keys()))
    path, key = tables[selected]
    df = load_parquet(str(path))

    if df.empty:
        st.warning(f"No data found. Run `python3 spark/hudi_tables.py` first.")
    else:
        st.markdown(f"**{len(df):,} records** · partitioned Delta Lake table")
        st.dataframe(df.head(50), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("✨ Delta Lake Capabilities")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("**MERGE INTO (Upsert)**")
            st.markdown("Update ride status from `in_progress` → `completed` without duplicates. 500 records merged in the demo.")
        with col2:
            st.markdown("**Hard Delete (GDPR)**")
            st.markdown("Permanently erase a driver's 50 location pings with a single DELETE operation — GDPR compliant.")
        with col3:
            st.markdown("**Time Travel**")
            st.markdown("Query the table at version 0 (before upsert) and compare — 2,973 vs 2,473 in-progress rides.")


# ===========================================================================
# PAGE 5 — AI SQL ASSISTANT
# ===========================================================================
elif page == "🤖 AI SQL Assistant":
    st.title("🤖 GenAI SQL Assistant")
    st.markdown("Ask anything about the mobility data in plain English. Claude translates it to SparkSQL and explains the results.")
    st.markdown("---")

    api_key = st.text_input("Anthropic API Key", type="password",
                            placeholder="sk-ant-...",
                            help="Get your key at console.anthropic.com")

    st.markdown("---")
    st.subheader("Ask a question")

    examples = [
        "Which city had the highest total revenue?",
        "What percentage of rides were cancelled by city?",
        "Which hour of the day has the highest average surge multiplier?",
        "What is the most common cancellation reason?",
        "Show the top 5 payment methods by transaction volume.",
        "Which city has the most available drivers?",
    ]
    example = st.selectbox("Or pick an example question:", ["— type your own —"] + examples)
    question = st.text_input("Your question:", value=example if example != "— type your own —" else "")

    if st.button("Ask Claude", type="primary") and question:
        if not api_key:
            st.error("Please enter your Anthropic API key above.")
        else:
            try:
                import anthropic
                from delta import configure_spark_with_delta_pip
                from pyspark.sql import SparkSession

                with st.spinner("Starting Spark..."):
                    builder = (
                        SparkSession.builder
                        .appName("MobilityDashboard")
                        .master("local[2]")
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                        .config("spark.sql.shuffle.partitions", "4")
                        .config("spark.driver.memory", "2g")
                    )
                    spark = configure_spark_with_delta_pip(builder).getOrCreate()
                    spark.sparkContext.setLogLevel("ERROR")

                    # Register tables
                    delta_views = {
                        "rides": DELTA_DIR / "delta_rides",
                        "driver_locations": DELTA_DIR / "delta_driver_locations",
                        "payments": DELTA_DIR / "delta_payments",
                        "cancellations": DELTA_DIR / "delta_cancellations",
                    }
                    gold_views = {
                        "daily_ride_metrics": GOLD_DIR / "daily_ride_metrics",
                        "city_demand": GOLD_DIR / "city_demand_heatmap",
                        "payment_summary": GOLD_DIR / "payment_summary",
                        "driver_utilization": GOLD_DIR / "driver_utilization",
                    }
                    for name, path in {**delta_views, **gold_views}.items():
                        if path.exists():
                            if name in delta_views:
                                spark.read.format("delta").load(str(path)).createOrReplaceTempView(name)
                            else:
                                spark.read.parquet(str(path)).createOrReplaceTempView(name)

                client = anthropic.Anthropic(api_key=api_key)

                SYSTEM = """You are a SparkSQL expert for a mobility platform. Available tables:
rides (ride_id, rider_id, driver_id, city, ride_status, fare_amount, surge_multiplier, distance_miles, duration_minutes, payment_type, event_date, trip_hour, is_surge),
driver_locations (event_id, driver_id, city, lat, lng, availability_status, event_date),
payments (payment_id, ride_id, payment_status, amount, currency, payment_method, event_date),
cancellations (cancellation_id, ride_id, cancelled_by, reason, event_date),
daily_ride_metrics (event_date, city, ride_status, total_rides, total_revenue, avg_fare, avg_surge_multiplier),
city_demand (city, trip_day_of_week, trip_hour, total_requests, avg_surge, surge_rate_pct),
payment_summary (event_date, payment_method, payment_status, transaction_count, total_amount),
driver_utilization (event_date, driver_id, completed_trips, total_earnings, active_hours).
Output ONLY raw SparkSQL — no markdown, no explanation."""

                with st.spinner("Claude is generating SQL..."):
                    resp = client.messages.create(
                        model="claude-opus-4-7",
                        max_tokens=512,
                        thinking={"type": "adaptive"},
                        system=SYSTEM,
                        messages=[{"role": "user", "content": f"Generate SparkSQL for: {question}"}],
                    )
                    sql = next((b.text.strip() for b in resp.content if b.type == "text"), "")
                    sql = re.sub(r"^```(?:sql)?\s*", "", sql, flags=re.IGNORECASE)
                    sql = re.sub(r"\s*```$", "", sql).strip()

                with st.expander("Generated SQL", expanded=True):
                    st.code(sql, language="sql")

                with st.spinner("Running query..."):
                    result_df = spark.sql(sql).limit(20).toPandas()

                st.subheader("Results")
                st.dataframe(result_df, use_container_width=True, hide_index=True)

                with st.spinner("Claude is explaining the results..."):
                    explain_resp = client.messages.create(
                        model="claude-opus-4-7",
                        max_tokens=300,
                        system="You are a data analyst. Explain query results in 3-4 sentences. Highlight the most interesting insight. Be concise and use business language.",
                        messages=[{"role": "user", "content": f"Question: {question}\nResults:\n{result_df.to_string()}\nExplain these results."}],
                    )
                    explanation = explain_resp.content[0].text.strip()

                st.subheader("💡 Insight")
                st.info(explanation)

                # Try to auto-chart if numeric column exists
                numeric_cols = result_df.select_dtypes("number").columns.tolist()
                str_cols = result_df.select_dtypes("object").columns.tolist()
                if numeric_cols and str_cols:
                    fig = px.bar(result_df, x=str_cols[0], y=numeric_cols[0],
                                 color=numeric_cols[0], color_continuous_scale="Blues")
                    fig.update_layout(coloraxis_showscale=False)
                    st.plotly_chart(fig, use_container_width=True)

            except Exception as e:
                st.error(f"Error: {e}")
