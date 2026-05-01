# Snowflake ETL Pipeline - Case Study

## Overview

This project demonstrates a complete end-to-end ELT pipeline built entirely on Snowflake, following the **Medallion Architecture** (Bronze → Silver → Gold). It ingests raw employee data in multiple formats (CSV, JSON, Parquet), transforms it through structured layers, and produces analytics-ready aggregated tables — all orchestrated with native Snowflake features.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌─────────────┐
│  Raw Files  │────▶│    BRONZE    │────▶│    SILVER    │────▶│    GOLD     │
│  (Stage)    │     │  (Raw Load)  │     │ (Cleansed)   │     │(Aggregated) │
└─────────────┘     └──────────────┘     └──────────────┘     └─────────────┘
     CSV/JSON/           COPY INTO /         Parsed JSON          GROUP BY
     Parquet             Snowpipe            + Metadata           Analytics
```

## Database Structure

| Schema | Purpose |
|--------|---------|
| `ETL_PIPELINE_BRONZE_SCH` | Raw data landing zone — tables loaded via COPY/Snowpipe |
| `ETL_PIPELINE_SILVER_SCH` | Cleansed/parsed data with metadata (ETL run ID, timestamps) |
| `ETL_PIPELINE_GOLD_SCH` | Business-ready aggregated tables and views |
| `ETL_PIPELINE_TASKS_SCH` | Task orchestration objects (Streams + Tasks pipeline) |
| `ETL_PIPELINE_OPS_SCH` | Operational monitoring — event table, data quality results |

## Key Objects

### Bronze Layer
- `BRONZE_STG` — Internal stage for file uploads
- `BRONZE_EMPLOYEE_ABSENSE` — CSV-sourced employee absence data
- `BRONZE_EMPLOYEE_DATA_JSON` — JSON-sourced employee data
- `BRONZE_EMPLOYEE_DATA_PARQUET` — Parquet-sourced employee data (primary pipeline table)
- `BRONZE_EMPLOYEE_PARQUET_PIPE` — Snowpipe for automated/manual Parquet ingestion

### Silver Layer
- `SILVER_EMPLOYEE_DATA_PARQUET` — Parsed employee data with flattened survey scores and ETL metadata
- `SILVER_EMP_PARQUET_DT` — Dynamic Table equivalent (auto-refreshing)
- `BRONZE_EMP_PARQUET_STREAM` — Change Data Capture stream on the bronze table

### Gold Layer
- `EMPLOYEE_DEMOGRAPHICS_BY_DEPARTMENT` — Department-level demographic aggregations
- `SURVEY_RESULTS_BY_DEPARTMENT` — Department-level engagement survey averages
- `V_SURVEY_RESULTS_BY_DEPARTMENT` — Live view over survey aggregation
- `*_DT` — Dynamic Table equivalents with 5-minute target lag

### Orchestration
- `T_SILVER_FROM_BRONZE_STREAM` — Root task triggered by stream data (CRON: 6AM UTC)
- `T_GOLD_REFRESH_AFTER_SILVER` — Successor task that materializes gold tables after silver loads

### Operations
- `PIPELINE_EVENTS` — Event table for procedure-level logging
- `DATA_QUALITY_RESULTS` — Stored procedure-driven DQ check results

## Project Files

| File | Topic |
|------|-------|
| `01.BatchLoading.sql` | Schema/stage/table creation, COPY INTO for CSV/JSON/Parquet |
| `02.SnowpipeLoading.sql` | Snowpipe creation, refresh, and monitoring |
| `03.SilverStagingLayer.sql` | Silver table creation, JSON parsing, stored procedure for staging |
| `04.GoldTransformationLayer.sql` | CTE, temp table, and view approaches; modular stored procedures with master orchestrator |
| `05.StreamsAndTasks.sql` | Stream + Task-based incremental pipeline with logging wrappers |
| `06.DynamicTables.sql` | Declarative pipeline using Dynamic Tables (auto-refresh alternative) |
| `07.Monitoring.sql` | Pipe refresh, task execution, event table queries, dynamic table monitoring |
| `08.TrackDataQuality.sql` | Data quality stored procedures (null checks, range validation) |

## Pipeline Patterns Demonstrated

### Pattern 1: Manual Batch (Files 01–04)
Load files → Parse/enrich in Silver → Aggregate in Gold via stored procedures.

### Pattern 2: Stream + Task (File 05)
CDC stream detects new bronze rows → Root task loads Silver → Successor task builds Gold. Fully event-driven with CRON scheduling.

### Pattern 3: Dynamic Tables (File 06)
Declarative SQL definitions with `TARGET_LAG = '5 minutes'`. Snowflake manages refresh scheduling automatically — no tasks or streams needed.

## Data Model

The source dataset is **employee HR data** containing:
- Demographics: name, age, gender, city, department, division, job title
- Employment: business unit, store location, length of service, hours absent
- Engagement Survey: nested JSON with 5 scores (satisfaction, work-life balance, career growth, communication, teamwork) on a 1–5 scale

## How to Run

1. **Create the database** (run as SYSADMIN or ACCOUNTADMIN):
   ```sql
   CREATE DATABASE IF NOT EXISTS ETL_PIPELINE_DB;
   ```

2. **Execute files in order** (01 → 08). Each file is self-contained with environment setup at the top.

3. **Upload data files** to `@ETL_PIPELINE_DB.ETL_PIPELINE_BRONZE_SCH.BRONZE_STG`:
   - `employee_*_absence.csv`
   - `employee_data_*.json`
   - `employee_data_part_*.parquet`

4. **For the Streams+Tasks pipeline**: Resume tasks, then load new data via the pipe to trigger execution.

5. **For Dynamic Tables**: Resume the dynamic tables and load data — they refresh automatically.

## Concepts Covered

- Medallion Architecture (Bronze/Silver/Gold)
- Multi-format ingestion (CSV, JSON, Parquet)
- COPY INTO with pattern matching and MATCH_BY_COLUMN_NAME
- Snowpipe (manual refresh mode)
- VARIANT parsing with PARSE_JSON / TRY_TO_NUMBER
- Stored Procedures with error handling and ETL metadata
- Streams (CDC / Change Data Capture)
- Tasks with predecessors and CRON scheduling
- Dynamic Tables with target lag
- Event Tables for observability
- Data Quality checks via stored procedures
- Views for real-time aggregation

## Prerequisites

- Snowflake account with ACCOUNTADMIN access
- Warehouse: `COMPUTE_WH`
- Sample employee data files (Parquet partitioned as `employee_data_part_*.parquet`)
