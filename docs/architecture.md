# Thalassa Architecture

## Medallion overview

```mermaid
flowchart LR
    subgraph src["Source"]
        API["🌐 data.gov.gr API"]
    end

    subgraph bronze["🥉 Bronze — Raw"]
        RAW["BigQuery: raw layer"]
    end

    subgraph silver["🥈 Silver — Cleaned & Modelled"]
        STG["BigQuery: staging"]
        INT["BigQuery: intermediate"]
    end

    subgraph gold["🥇 Gold — Serving"]
        MARTS["BigQuery: marts"]
        REPORTS["BigQuery: reports"]
        INTEL["BigQuery: intelligence snapshots"]
    end

    subgraph serve["Serving"]
        DASH["Streamlit: dashboard/app.py"]
        LLM["LLM (optional): OpenRouter · Anthropic: Gemini"]
    end

    subgraph infra["Infrastructure"]
        TF["Terraform: GCP · BigQuery · IAM: Secret Manager"]
        BRUIN["Bruin: orchestration · checks: schedule · variables"]
    end

    API -->|"Bruin Python asset: batch ingest · daily"| RAW
    RAW --> STG --> INT --> MARTS & REPORTS & INTEL
    MARTS & REPORTS & INTEL --> DASH
    INTEL --> LLM --> DASH
    TF -.->|"provisions"| bronze & silver & gold
    BRUIN -.->|"orchestrates"| bronze & silver & gold
```

## Detailed data flow

```mermaid
flowchart TD
    subgraph Source["External Source"]
        API["data.gov.gr: sailing_traffic API"]
    end

    subgraph IaC["Infrastructure (Terraform)"]
        TF["infra/: BigQuery dataset · IAM: Secret Manager · Artifact Registry"]
    end

    subgraph Ingestion["Ingestion — Bruin Python asset"]
        ING["raw_sailing_traffic.py: windowed fetch · retries · backoff: stores raw_payload"]
    end

    subgraph BQ["BigQuery Warehouse (GCP)"]
        RAW["raw_sailing_traffic: raw landing · append-only"]

        subgraph Transform["Bruin SQL transformations"]
            STG["stg_sailing_traffic: type cleanup · normalization"]
            INT_REC["int_traffic_record: dedup · canonical keys"]

            subgraph Route["Route branch"]
                INT_RD["int_route_day"]
                FRT_D["fct_route_traffic_daily"]
                FRT_W["fct_route_traffic_weekly"]
                FRT_M["fct_route_traffic_monthly"]
            end

            subgraph Port["Port branch"]
                INT_PM["int_port_movement"]
                INT_PD["int_port_day"]
                FPA_D["fct_port_activity_daily"]
                FPA_W["fct_port_activity_weekly"]
                FPA_M["fct_port_activity_monthly"]
            end

            subgraph Dims["Dimensions"]
                DIM_R["dim_route"]
                DIM_RP["dim_route_pair"]
                DIM_P["dim_port"]
            end

            subgraph Reports["Reports / Quality"]
                RC["row_counts_daily/weekly: /monthly/yearly"]
                INTEL["intelligence_snapshots: auto_panel_snapshot_writer"]
            end
        end
    end

    subgraph Dashboard["Serving — Streamlit"]
        APP["dashboard/app.py: KPI cards · Traffic Pulse: Top Corridors · Port Balance: Intelligence panel"]
        LLM["LLM layer (optional): OpenRouter · Anthropic · Gemini: cached narrative generation"]
    end

    API -->|"batch pull: daily schedule"| ING
    TF -.->|"provisions"| BQ
    ING --> RAW
    RAW --> STG
    STG --> INT_REC
    INT_REC --> INT_RD --> FRT_D --> FRT_W & FRT_M
    INT_REC --> INT_PM --> INT_PD --> FPA_D --> FPA_W & FPA_M
    INT_REC --> DIM_R & DIM_RP
    INT_PD --> DIM_P
    INT_REC --> RC
    FRT_D & FPA_D --> INTEL
    INTEL --> LLM
    FRT_D & FRT_W & FRT_M --> APP
    FPA_D & FPA_W & FPA_M --> APP
    DIM_R & DIM_RP & DIM_P --> APP
    RC --> APP
    INTEL --> APP
    LLM --> APP
```

## Warehouse layers and key models

| Layer | Purpose | Main assets |
| --- | --- | --- |
| Raw landing | Append-only source landing with `raw_payload` preserved | `<THALASSA_BQ_DATASET>.raw_sailing_traffic` |
| Staging | Type cleanup, text normalization, safe casting | `<THALASSA_BQ_DATASET>.stg_sailing_traffic` |
| Intermediate | Canonical record grain and reusable aggregates | `<THALASSA_BQ_DATASET>.int_traffic_record`, `int_route_day`, `int_port_movement`, `int_port_day` |
| Marts | Dashboard-facing dimensions and fact tables | `dim_route`, `dim_route_pair`, `dim_port`, `fct_route_traffic_*`, `fct_port_activity_*` |
| Reports | Quality and profiling outputs | `row_counts_daily`, `row_counts_weekly`, `row_counts_monthly`, `row_counts_yearly` |
| Intelligence | Cached narrative layer for the UI | `intelligence_snapshots`, `auto_panel_snapshot_writer` |

## Batch pipeline details

The pipeline is defined in `pipeline/pipeline.yml` and is scheduled as `daily` starting from `2024-01-01`.

Runtime behavior is controlled with Bruin variables such as:

- `request_window_unit`
- `request_window_size`
- `request_max_retries`
- `request_retry_base_delay_seconds`
- `request_retry_max_delay_seconds`
- `request_failed_window_replay_passes`
- `request_failed_window_replay_delay_seconds`

The ingestion asset calls the public API in windows, retries transient failures, replays failed windows, and stores the raw JSON payload for traceability.

## BigQuery optimization and data quality

### Partitioning and clustering

The canonical warehouse models are optimized for analytic reads.

- Daily tables are partitioned by `service_date`
- Weekly tables are partitioned by `week_start`
- Monthly tables are partitioned by `year_month`
- Route-heavy tables are clustered by `departure_port`, `arrival_port`, and `route_pair_key`
- Port-heavy tables are clustered by `port_name`

This matters because the dashboard almost always filters by date and then slices by route or port.

### Data quality checks

Bruin checks are embedded directly in the assets.

Examples already implemented:

- `not_null`, `unique`, `positive`, and `non_negative` checks on key columns
- safe-cast checks for passenger and vehicle counts
- collision checks for `route_key` and `route_pair_key`
- accepted values checks for movement types

## Tech stack

| Layer | Tooling |
| --- | --- |
| Ingestion and orchestration | Bruin |
| Transformation | SQL + Bruin model metadata |
| Language runtime | Python 3.11+ |
| Warehouse | BigQuery |
| Cloud | GCP |
| IaC | Terraform |
| Dashboard | Streamlit + Altair/Vega-Lite |
| Secrets and auth | GCP ADC, optional service account file, Secret Manager |
| Optional intelligence layer | OpenRouter, Anthropic, or Gemini with BigQuery snapshot caching |
