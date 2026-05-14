# Batch Pipeline using PySpark

A production-style PySpark batch pipeline implementing the **Medallion Architecture** (**Bronze → Silver → Gold**) for scalable, analytics-ready event processing.

The project demonstrates how to design a robust data engineering workflow that handles:

* Raw event ingestion
* Schema enforcement
* Data quality validation
* Quarantine handling
* Incremental processing
* Dimension enrichment
* Analytical aggregations
* Partition-aware batch optimization

Built with simplicity and operational clarity in mind, this repository focuses on practical engineering patterns commonly used in modern data platforms.

---

# Architecture Overview

```text
                Raw JSONL Events
                        │
                        ▼
              ┌─────────────────┐
              │ Bronze Layer    │
              │ Cleaning & DQ   │
              └─────────────────┘
                        │
                        ▼
              ┌─────────────────┐
              │ Silver Layer    │
              │ Enrichment      │
              └─────────────────┘
                        │
                        ▼
              ┌─────────────────┐
              │ Gold Layer      │
              │ Aggregations    │
              └─────────────────┘
```

---

# Project Structure

```text
.
├── config/
│   └── pipeline.yaml
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── quarantine/
│   └── reference/
│       └── users.csv
│
├── job/
│   └── pipeline.py
│
├── src/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   ├── schema.py
│   └── utils.py
│
├── Makefile
├── pyproject.toml
└── README.md
```

---

# Core Features

## Bronze Layer — Raw Ingestion & Cleansing

The Bronze layer is responsible for transforming raw JSONL event logs into clean, structured datasets.

### Capabilities

* Explicit Spark schema enforcement
* Corrupt record detection using `_corrupt_record`
* Invalid row quarantine
* Deduplication using business keys
* Field normalization
* Partitioned Parquet outputs

### Data Quality Logic

#### Schema Enforcement

Raw events are read using a predefined schema from `src/schema.py`.

This prevents:

* Silent schema drift
* Implicit type coercion
* Corrupted downstream datasets

#### Corrupt Record Handling

Spark operates in `PERMISSIVE` mode:

* Malformed rows are captured
* Invalid records are redirected to:

```text
data/quarantine/
```

instead of silently failing or being discarded.

#### Deduplication Strategy

Duplicate events are resolved using:

* `event_id`
* latest `event_ts`
* highest `value`

This guarantees deterministic outputs during reruns.

#### Standardization

The pipeline normalizes incoming data:

| Field        | Transformation          |
| ------------ | ----------------------- |
| `event_type` | Uppercased              |
| `value`      | Nulls replaced with `0` |

---

## Silver Layer — Business Enrichment

The Silver layer enriches clean event data with dimensional reference data and business logic.

### Capabilities

* User dimension joins
* Derived business metrics
* Safe date parsing
* Null-safe enrichment
* Partition integrity enforcement

### Enrichment Logic

#### User Reference Join

Bronze events are joined with:

```text
data/reference/users.csv
```

#### Derived Columns

| Column              | Description                               |
| ------------------- | ----------------------------------------- |
| `is_purchase`       | Boolean purchase indicator                |
| `days_since_signup` | Difference between signup and event dates |

#### Defensive Defaults

Missing dimensions are safely handled:

| Scenario            | Default                           |
| ------------------- | --------------------------------- |
| Missing country     | `"UNKNOWN"`                       |
| Invalid signup date | Safely parsed using `try_to_date` |

#### Partition Safety

Records with null `event_date` are filtered before write operations to avoid invalid partition structures.

---

## Gold Layer — Analytics Tables

The Gold layer generates analytics-ready datasets optimized for BI and reporting workloads.

---

## 1. Country Daily Active Metrics

Daily country-level aggregates including:

| Metric            | Description           |
| ----------------- | --------------------- |
| `total_events`    | Total event count     |
| `total_value`     | Sum of event values   |
| `total_purchases` | Purchase count        |
| `unique_users`    | Distinct active users |

Partitioned by:

```text
event_date
```

---

## 2. Average Days Before Purchase

Calculates:

```text
avg_days_to_purchase
```

Grouped by:

* `country`
* `event_date`

Used for:

* conversion analysis
* onboarding effectiveness
* purchase latency tracking

---

# Incremental Processing Strategy

The pipeline is designed for safe reruns and late-arriving event handling.

---

## Dynamic Partition Overwrite

Spark configuration:

```python
spark.sql.sources.partitionOverwriteMode = "dynamic"
```

Benefits:

* Overwrites only affected partitions
* Prevents full-table rewrites
* Enables efficient backfills

---

## Late Data Handling

Since datasets are partitioned by `event_date`:

* Late-arriving records automatically land in the correct historical partition
* Reprocessing a specific day safely rewrites only that partition

This avoids:

* duplicate events
* cross-partition corruption
* unnecessary recomputation

---

# Why No Delta Lake or MERGE?

This pipeline intentionally avoids row-level merge operations.

## Reasoning

Event logs are:

* immutable
* append-oriented
* time-series based

Using `MERGE INTO` for high-volume event data introduces:

* expensive shuffles
* heavy key matching
* unnecessary compute overhead

Instead, bulk partition overwrites provide:

* simpler architecture
* faster execution
* lower infrastructure cost
* lightweight Parquet-based storage

This pattern is commonly preferred for scalable event-processing systems.

---

# Data Quality Principles

| Principle              | Implementation                 |
| ---------------------- | ------------------------------ |
| Explicit schema        | `src/schema.py`                |
| Corrupt record capture | `_corrupt_record`              |
| Quarantine strategy    | `data/quarantine/`             |
| Null-safe defaults     | `fillna()` + defensive parsing |
| Idempotent reruns      | Dynamic partition overwrite    |
| Partition integrity    | Null `event_date` filtering    |

---

# Technology Stack

| Component | Purpose                       |
| --------- | ----------------------------- |
| PySpark   | Distributed data processing   |
| Parquet   | Columnar storage format       |
| uv        | Dependency management         |
| Makefile  | Developer workflow automation |
| PyYAML    | Configuration management      |

---

# Setup

## Prerequisites

* Python `3.13+`
* Java installed
* Spark-compatible environment
* [uv package manager](https://github.com/astral-sh/uv?utm_source=chatgpt.com)

---

# Installation

## 1. Initialize Repository

```bash
cd [repository-name]
make init
```

---

## 2. Install Dependencies

```bash
make install
```

This installs:

* PySpark
* PyYAML
* py4j
* formatting tools

---

# Running the Pipeline

Execute the complete workflow:

```bash
make run
```

This automatically:

* sets `PYTHONPATH`
* loads `config/pipeline.yaml`
* executes:

```text
Bronze → Silver → Gold
```

---

# Development Commands

## Format Code

```bash
make format
```

Runs:

* `ruff`
* fallback `black`

---

## Clean Environment

```bash
make clean
```

Removes:

* generated outputs
* quarantine files
* Spark metadata
* `__pycache__`

---

# Example Workflow

```text
Raw JSONL
   │
   ▼
Bronze Cleansing
   │
   ├── Valid Records  ──► Bronze Parquet
   └── Invalid Rows   ──► Quarantine
                               │
                               ▼
                        Manual Inspection

Bronze Parquet
   │
   ▼
Silver Enrichment
   │
   ▼
Gold Aggregates
   │
   ▼
BI / Analytics / Reporting
```

---

# Engineering Design Goals

This project prioritizes:

* readability
* operational simplicity
* deterministic reruns
* scalable partition management
* defensive data quality practices
* production-oriented ETL patterns

It is intentionally designed as a lightweight batch pipeline that demonstrates strong data engineering fundamentals without introducing unnecessary platform complexity.

---

# Future Improvements

Potential next steps include:

* Airflow orchestration
* Delta Lake support
* Data quality observability
* Great Expectations integration
* Unit and integration testing
* Dockerized local environment
* CI/CD automation
* Incremental watermarking
* Cloud object storage support (S3/GCS/ADLS)

---

# License

MIT License.
