# Real-Time Mobility Data Platform

An end-to-end data engineering platform modeled after Uber's production architecture. Built with Apache Kafka, PySpark, Spark Structured Streaming, Delta Lake, Apache Airflow, and a Claude-powered GenAI SQL assistant.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA GENERATOR                                │
│   447,000+ synthetic events across 4 types · 8 cities           │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                 APACHE KAFKA                                     │
│   ride_events · driver_location_events · payment_events ·       │
│   cancellation_events          37,000+ msg/sec                  │
└──────────┬──────────────────────────────┬───────────────────────┘
           │                              │
           ▼                              ▼
┌──────────────────────┐    ┌─────────────────────────────────────┐
│  SPARK BATCH ETL     │    │   SPARK STRUCTURED STREAMING        │
│  Bronze → Silver →   │    │   5-min windowed city KPIs          │
│  Gold (Parquet)      │    │   Late-data watermarking            │
└──────────┬───────────┘    └─────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│              DELTA LAKE                                          │
│   MERGE INTO upserts · Hard deletes · Time travel               │
└──────────┬──────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│         APACHE AIRFLOW  (Daily Orchestration)                    │
│  generate → validate → bronze → silver → gold → validate →      │
│  notify                                                          │
└──────────┬──────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│              GENAI SQL ASSISTANT  (Claude API)                   │
│   Natural language → SparkSQL → Results → Plain English          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technologies |
|---|---|
| Ingestion | Apache Kafka, kafka-python |
| Batch Processing | PySpark, Bronze/Silver/Gold Lakehouse |
| Stream Processing | Spark Structured Streaming, windowed aggregations |
| Table Format | Delta Lake (ACID, upserts, time travel) |
| Orchestration | Apache Airflow, Docker Compose |
| GenAI | Claude API (Anthropic), adaptive thinking |
| Dashboard | Streamlit, Plotly |

---

## Project Structure

```
mobility-platform/
├── data_generator/
│   └── generator.py          # Synthetic event generator (100k+ rides)
├── kafka/
│   ├── producer.py           # Streams 4 CSV topics at 37k+ msg/sec
│   └── consumer.py           # Debug consumer / message verifier
├── spark/
│   ├── etl.py                # Bronze → Silver → Gold batch ETL
│   ├── streaming.py          # Spark Structured Streaming from Kafka
│   └── hudi_tables.py        # Delta Lake tables with upsert/delete/time travel
├── airflow/
│   └── dags/
│       └── mobility_etl_dag.py  # 7-task daily Airflow DAG
├── genai/
│   └── sql_assistant.py      # Claude-powered NL → SparkSQL assistant
├── website/
│   └── app.py                # Streamlit dashboard (5 tabs)
├── docker-compose.yml        # Kafka + Zookeeper + Kafka UI + Airflow
└── requirements.txt
```

---

## Quickstart

### Prerequisites
- Python 3.11+
- Java 17 (for PySpark)
- Docker Desktop

### 1. Install dependencies
```bash
pip install -r requirements.txt
pip install pyspark delta-spark streamlit plotly pyarrow anthropic
```

### 2. Start infrastructure
```bash
docker compose up -d
```
Starts Kafka (port 9092), Kafka UI (port 8080), and Airflow (port 8081).

### 3. Generate data
```bash
python3 data_generator/generator.py --rides 100000 --output data_generator/output
```

### 4. Stream into Kafka
```bash
python3 kafka/producer.py --data-dir data_generator/output
```

### 5. Run Spark batch ETL
```bash
python3 spark/etl.py
```
Builds `warehouse/bronze/`, `warehouse/silver/`, `warehouse/gold/`.

### 6. Run Spark Structured Streaming
```bash
python3 spark/streaming.py --runtime 120
```

### 7. Create Delta Lake tables
```bash
python3 spark/hudi_tables.py
```
Demos upsert, GDPR delete, and time travel automatically.

### 8. Launch the web dashboard
```bash
streamlit run website/app.py
```
Open **http://localhost:8501**

### 9. GenAI SQL Assistant (CLI)
```bash
export ANTHROPIC_API_KEY=sk-ant-your-key-here
python3 genai/sql_assistant.py --demo
```
Or interactive:
```bash
python3 genai/sql_assistant.py
```

---

## Screenshot

![Dashboard Screenshot](assets/screenshot.png)

---

## Dashboard

The Streamlit dashboard has 5 tabs:

| Tab | Description |
|---|---|
| 🏠 Overview | KPI cards, architecture diagram, tech stack |
| 📊 Analytics | Revenue by city, surge heatmap, payment breakdown, driver utilization |
| ⚡ Streaming | Live Spark Structured Streaming output tables and charts |
| 🗄️ Delta Lake | Table previews, upsert/delete/time-travel explainer |
| 🤖 AI SQL Assistant | Ask questions in plain English — Claude generates SQL, runs it, charts it, and explains results |

---

## Key Features

**Bronze / Silver / Gold Lakehouse**
Raw CSVs are ingested into typed Parquet (Bronze), cleaned and deduplicated (Silver), then aggregated into 5 analytics tables (Gold): daily ride metrics, city demand heatmap, driver utilization, payment summary, and cancellation analysis.

**Spark Structured Streaming**
5 concurrent streaming queries read from Kafka topics, compute windowed KPIs (ride volume, revenue, surge, driver availability, cancellation rates) with watermark-based late-data handling, and write micro-batch Parquet outputs every 30 seconds.

**Delta Lake**
All 4 event types are written as Delta tables supporting ACID transactions. The demo shows: MERGE INTO upsert (500 in-progress rides → completed), hard delete (GDPR erasure of a driver's 50 location pings), and time travel (version 0 snapshot vs. current state).

**GenAI SQL Assistant**
Powered by Claude (claude-opus-4-7 with adaptive thinking). Translates natural language questions into SparkSQL, executes them against Delta Lake tables, and explains results in plain English with auto-generated charts.

**Airflow Orchestration**
A 7-task DAG runs daily at 06:00 UTC: generate data → validate inputs → Spark bronze → Spark silver → Spark gold → validate outputs → notify success. Containerized via Docker Compose alongside Kafka.

---

## UI Links (when running)

| Service | URL | Credentials |
|---|---|---|
| Streamlit Dashboard | http://localhost:8501 | — |
| Kafka UI | http://localhost:8080 | — |
| Airflow | http://localhost:8081 | admin / admin |

---

## Author

**Mohamed Bokhamy**
[linkedin.com/in/mohamedbokhamy](https://linkedin.com/in/mohamedbokhamy) · [github.com/mohamedbokhamy22-a11y](https://github.com/mohamedbokhamy22-a11y)
