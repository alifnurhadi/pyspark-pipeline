

# Data Engineer Batch Pipeline (PySpark)

## Overview
This repository contains a PySpark batch data pipeline implementing a Medallion Architecture (Bronze, Silver, Gold). It is designed to ingest raw, messy event data, clean and validate it, enrich it with user reference data, and produce analytics-ready aggregate tables.

## Pipeline Design
The pipeline orchestrator (`job/pipeline.py`) drives the workflow sequentially through three layers:

1. **Bronze Layer (Raw to Cleaned):** * Ingests raw JSONL events using an explicitly defined schema (`src/schema.py`).
   * Schema mismatches and invalid records are caught via a `_corrupt_record` column and quarantined into a separate directory for later inspection.
   * Deduplicates records based on `event_id`, keeping the latest event by `event_ts` and `value`.
   * Normalizes fields (e.g., uppercasing `event_type` and filling null `value` fields with `0`).
   * Writes the cleaned output to Parquet format, partitioned by `event_date`.

2. **Silver Layer (Enrichment):**
   * Joins the cleaned Bronze events with user reference data (`data/reference/users.csv`).
   * Handles missing dimensions gracefully by defaulting missing countries to `"UNKNOWN"`.
   * Derives business logic columns: `is_purchase` (boolean) and `days_since_signup` (integer).
   * Filters out any records with a null `event_date` to ensure partition integrity before writing out to Parquet.

3. **Gold Layer (Aggregations):**
   * Generates two primary analytics tables:
     * **Country DAU:** Aggregates daily metrics per country, including `total_events`, `total_value`, `total_purchases`, and `unique_users`.
     * **Days Before Purchase:** Calculates the average days between a user signing up and making a purchase, aggregated by country (`avg_days_to_purchase`).
   * Writes final outputs partitioned by `event_date`.

## Data Quality Rules
* **Explicit Schema Enforcement:** Reading data strictly uses Spark's `PERMISSIVE` mode paired with a defined schema. Corrupted data is intercepted immediately.
* **Defensive Cleansing:** Missing `value` amounts are filled with zero, missing countries are labeled, and non-existent `signup_date` strings are safely parsed using `try_to_date`.
* **Quarantine:** Rather than failing the job or quietly dropping data, unparseable lines are segregated into `data/quarentine` for auditing.

## Incremental Processing & Late Data Strategy
* **Idempotency:** The pipeline uses Spark's dynamic partition overwrite mode (`spark.sql.sources.partitionOverwriteMode = "dynamic"`). 
* **Late Data Handling:** Because the pipeline partitions outputs by `event_date`, late-arriving events are automatically grouped into their correct historical partition. If the pipeline is re-run for a specific day, it safely overwrites only that day's partition without duplicating records or corrupting adjacent day Because we are dealing with immutable, time-series event data partitioned by event_date, we do not need row-level updates. Overwriting the daily partition is significantly faster and less computationally expensive than performing row-by-row merges. It allows us to use standard, lightweight Parquet files without requiring the overhead of a Delta Lake

## Setup & Usage

### Prerequisites
* Python 3.13 or higher
* [uv](https://github.com/astral-sh/uv) installed for fast dependency management.

### Makefile Commands
The project is bundled with a `Makefile` to simplify local execution.

**1. Install Dependencies**
Install PySpark, PyYAML, and py4j dependencies cleanly using `uv`:
```bash
make install
```

**2. Run the Pipeline**
Execute the full data pipeline (Bronze -> Silver -> Gold). This command automatically injects the `PYTHONPATH` and points the orchestrator to `config/pipeline.yaml`:
```bash
make run
```

**3. Format Code**
Ensure code is PEP-8 compliant. This will run `ruff` (or fallback to `black`) to format the codebase:
```bash
make format
```

**4. Clean Up**
Wipe out the generated metadata, quarantine data, output folders, and any `__pycache__` artifacts to reset the environment:
```bash
make clean
```
