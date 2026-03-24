# Thalassa

Bruin-based data pipeline for Greek sailing traffic analytics using the `data.gov.gr` `sailing_traffic` API.

## What This Repo Contains

- `pipeline/`: **canonical pipeline** (BigQuery SQL assets + Python ingestion)
- `pipeline_duckdb/`: DuckDB mirror for local testing/parity only
- `dashboard/`: local Streamlit analytics UI over `duckdb.db`

The long-term target is `pipeline/`. DuckDB is for fast local validation.

## Pipeline Overview

Source:
- `https://data.gov.gr/api/v1/query/sailing_traffic`

Logical DAG:

```text
thalassa.raw_sailing_traffic (python)
  -> thalassa.stg_sailing_traffic
    -> thalassa.int_traffic_record
      -> thalassa.int_route_day -> thalassa.fct_route_traffic_daily -> weekly/monthly
      -> thalassa.int_port_movement -> thalassa.int_port_day -> thalassa.fct_port_activity_daily -> weekly/monthly
      -> thalassa.dim_route
      -> thalassa.dim_route_pair
      -> thalassa.row_counts_daily/weekly/monthly/yearly
```

## Project Structure

```text
pipeline/
  pipeline.yml
  assets/
    ingestion/raw_sailing_traffic.py
    staging/stg_sailing_traffic.sql
    intermediate/
    marts/
    reports/

pipeline_duckdb/
  pipeline.yml
  assets/   # same logical assets in duckdb.sql dialect

dashboard/app.py
scripts/check_pipeline_sql_parity.py
```

## Prerequisites

- `bruin`
- Python `>=3.11`
- `uv` (for dashboard/local Python tooling)

## Configuration

Connections are configured in `.bruin.yml`.

- Canonical pipeline (`pipeline/`) expects `google_cloud_platform` connection `gcp-default`.
- Local mirror (`pipeline_duckdb/`) uses `duckdb-default` (`duckdb.db`).

## Runtime Variables

Defined in each `pipeline.yml`:

- `request_window_unit`: `day | month | year`
- `request_window_size`: integer >= 1
- `request_max_retries`
- `request_retry_base_delay_seconds`
- `request_retry_max_delay_seconds`

## Quickstart

### 1) Validate canonical pipeline

```powershell
bruin validate .\pipeline
```

### 2) Run ingestion + downstream (canonical BigQuery)

```powershell
bruin run .\pipeline\assets\ingestion\raw_sailing_traffic.py --downstream --start-date 2025-01-01 --end-date 2025-01-31
```

### 3) Optional: run local DuckDB mirror

```powershell
bruin validate .\pipeline_duckdb
bruin run .\pipeline_duckdb\assets\ingestion\raw_sailing_traffic.py --downstream --start-date 2025-01-01 --end-date 2025-01-31
```

## Dashboard (Local)

```powershell
uv sync
uv run streamlit run .\dashboard\app.py
```

Reads from `duckdb.db` and uses `thalassa.*` tables.

## SQL Parity Check

Compares BigQuery and DuckDB SQL assets after dialect normalization:

```powershell
uv run python .\scripts\check_pipeline_sql_parity.py
```

## Notes

- Ingestion is append-based and stores `raw_payload` for traceability.
- Staging/intermediate models handle canonicalization, deduplication, and grain-ready derivations.
- Modeling decisions should be made in `pipeline/` first, then mirrored to `pipeline_duckdb/`.

## BigQuery Optimization (Implemented)

The canonical BigQuery pipeline now defines physical layout in Bruin model metadata using:

- `materialization.partition_by`
- `materialization.cluster_by`

This is applied to date-grain staging/intermediate/fact/report assets so dashboard date filters and route/port filters can scan less data as volume grows.

Implemented strategy summary:

- Daily tables partitioned by `service_date`
- Weekly tables partitioned by `week_start`
- Monthly tables partitioned by `year_month`
- Route traffic tables clustered by `departure_port`, `arrival_port`, `route_pair_key`
- Port activity tables clustered by `port_name`
- Upstream intermediate/staging tables clustered by route/port access columns

Details are documented in [docs/BIGQUERY_PARTITIONING_CLUSTERING.md](docs/BIGQUERY_PARTITIONING_CLUSTERING.md).

Important rollout note: if target BigQuery tables already exist without partitioning, run an initial `--full-refresh` (or drop/recreate) because BigQuery cannot change partition spec via `CREATE OR REPLACE`.
