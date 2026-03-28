# Thalassa

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Bruin](https://img.shields.io/badge/Bruin-Orchestrated-111827?style=flat-square)](https://getbruin.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Warehouse-4285F4?style=flat-square&logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![GCP](https://img.shields.io/badge/GCP-Cloud-4285F4?style=flat-square&logo=googlecloud&logoColor=white)](https://cloud.google.com/)
[![Terraform](https://img.shields.io/badge/Terraform-IaC-844FBA?style=flat-square&logo=terraform&logoColor=white)](https://www.terraform.io/)

Thalassa is a production-style batch data engineering project for Greek maritime traffic analytics. It ingests public sailing traffic data from the `data.gov.gr` `sailing_traffic` API, lands the raw records in BigQuery, transforms them into curated analytics tables with Bruin, and serves a Streamlit dashboard with both operational KPIs and deeper route/port analysis.

This repository is written to satisfy the spirit of the DE Zoomcamp course project: pick a real dataset, build an end-to-end pipeline, transform the data in a cloud warehouse, and expose it through a dashboard that is easy for reviewers to reproduce.

If you want the shortest path from clone to a running dashboard, jump to [Quick start](#quick-start).

## Table of contents

- [Problem statement](#problem-statement)
- [Dataset](#dataset)
- [Key features](#key-features)
- [Course project requirements mapping](#course-project-requirements-mapping)
- [Architecture](#architecture)
- [Batch pipeline details](#batch-pipeline-details)
- [Tech stack](#tech-stack)
- [Repository layout](#repository-layout)
- [Warehouse layers and key models](#warehouse-layers-and-key-models)
- [Dashboard](#dashboard)
- [BigQuery optimization and data quality](#bigquery-optimization-and-data-quality)
- [Step-by-step reproduction](#step-by-step-reproduction)
- [Quick start](#quick-start)
- [Future improvements](#future-improvements)
- [Contributing](#contributing)
- [Acknowledgements](#acknowledgements)

## Problem statement

Greek coastal traffic data is publicly available, but it is not immediately usable for analytics. Raw API responses are noisy, schema-light, and difficult to compare across time, ports, and routes.

This project answers questions such as:

- Which corridors carry the highest passenger volume?
- Which ports are the busiest over time?
- How do passenger and vehicle volumes change daily, weekly, and monthly?
- Is traffic concentrating in a few corridors?
- Which ports are arrival-heavy or departure-heavy in a given window?

To answer those questions reliably, the project builds a repeatable batch pipeline with explicit quality checks, curated warehouse models, and a dashboard backed by stable fact tables instead of ad hoc raw queries.

## Dataset

- Source: `data.gov.gr` `sailing_traffic` API
- Domain: Greek maritime passenger and vehicle traffic
- Pipeline interpretation: the source is modeled as reported traffic observations by service date, route code, departure port, and arrival port
- Analytics outcome: curated route- and port-level warehouse tables for dashboarding

## Key features

- A batch pipeline orchestrated with Bruin
- Cloud infrastructure on GCP provisioned with Terraform
- A BigQuery warehouse with raw, staging, intermediate, marts, and report layers
- A Streamlit dashboard with more than two tiles
- Optional cached AI-generated insight snapshots stored in BigQuery

## Course project requirements mapping

| Course area | How this repo addresses it |
| --- | --- |
| Problem description | Clear business problem around Greek sailing traffic demand, corridor concentration, and port activity |
| Cloud | GCP is used for the warehouse and supporting cloud resources |
| IaC | Terraform in `infra/` provisions the BigQuery dataset, IAM, Secret Manager, and optional Artifact Registry |
| Data ingestion | Batch ingestion from the public API using a Bruin Python asset |
| Workflow orchestration | Bruin manages dependencies, scheduling, variables, and downstream execution |
| Data warehouse | BigQuery is the canonical warehouse |
| Warehouse optimization | Partitioning and clustering are defined in the Bruin model metadata |
| Transformations | SQL models are organized into staging, intermediate, marts, and reports |
| Dashboard | Streamlit dashboard includes temporal and categorical analysis tiles |
| Reproducibility | Full step-by-step setup and run instructions are included below |

## Architecture

```text
data.gov.gr sailing_traffic API
        |
        v
Bruin Python ingestion asset
`pipeline/assets/ingestion/raw_sailing_traffic.py`
        |
        v
BigQuery raw landing table
`<THALASSA_BQ_DATASET>.raw_sailing_traffic`
        |
        v
Bruin SQL transformations
staging -> intermediate -> marts -> reports
        |
        +--> `<THALASSA_BQ_DATASET>.row_counts_*`
        +--> `<THALASSA_BQ_DATASET>.fct_route_traffic_*`
        +--> `<THALASSA_BQ_DATASET>.fct_port_activity_*`
        +--> `<THALASSA_BQ_DATASET>.dim_*`
        +--> `<THALASSA_BQ_DATASET>.intelligence_snapshots`
        |
        v
Streamlit dashboard
`dashboard/app.py`
```

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

## Repository layout

These are the folders that matter for reproducing the project:

- `pipeline/`: canonical production pipeline
- `dashboard/`: Streamlit dashboard and intelligence helpers
- `infra/`: Terraform for GCP foundation; see `infra/README.md` for IAM and cleanup details
- `scripts/`: local operational helpers, including snapshot generation
- `docs/`: supporting architecture and optimization notes
- `.env.example`: local environment template
- `.streamlit/secrets.toml.example`: optional Streamlit service account template

## Warehouse layers and key models

| Layer | Purpose | Main assets |
| --- | --- | --- |
| Raw landing | Append-only source landing with `raw_payload` preserved | `<THALASSA_BQ_DATASET>.raw_sailing_traffic` |
| Staging | Type cleanup, text normalization, safe casting | `<THALASSA_BQ_DATASET>.stg_sailing_traffic` |
| Intermediate | Canonical record grain and reusable aggregates | `<THALASSA_BQ_DATASET>.int_traffic_record`, `<THALASSA_BQ_DATASET>.int_route_day`, `<THALASSA_BQ_DATASET>.int_port_movement`, `<THALASSA_BQ_DATASET>.int_port_day` |
| Marts | Dashboard-facing dimensions and fact tables | `<THALASSA_BQ_DATASET>.dim_route`, `<THALASSA_BQ_DATASET>.dim_route_pair`, `<THALASSA_BQ_DATASET>.dim_port`, `<THALASSA_BQ_DATASET>.fct_route_traffic_daily`, `<THALASSA_BQ_DATASET>.fct_port_activity_daily`, weekly/monthly rollups |
| Reports | Quality and profiling outputs | `<THALASSA_BQ_DATASET>.row_counts_daily`, `<THALASSA_BQ_DATASET>.row_counts_weekly`, `<THALASSA_BQ_DATASET>.row_counts_monthly`, `<THALASSA_BQ_DATASET>.row_counts_yearly` |
| Intelligence | Cached narrative layer for the UI | `<THALASSA_BQ_DATASET>.intelligence_snapshots`, `<THALASSA_BQ_DATASET>.auto_panel_snapshot_writer` |

## Dashboard

The dashboard reads curated BigQuery tables, not raw API payloads.

Main outputs include:

- KPI cards for passengers, vehicles, active corridors, concentration, and leading port
- `Traffic Pulse`: a temporal trend chart for daily, weekly, or monthly movement
- `Top Corridors`: a categorical ranking of the busiest departure-arrival pairs
- `Port Balance`: a categorical ranking of the busiest ports by passenger flow
- Analytics views for weekday profile, corridor concentration, corridor efficiency, and port net flow
- A cached intelligence panel backed by `<THALASSA_BQ_DATASET>.intelligence_snapshots`

This means the course dashboard requirement is covered by at least:

- one temporal tile: `Traffic Pulse`
- one categorical tile: `Top Corridors` or `Port Balance`

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

## Step-by-step reproduction

If you are using PowerShell, replace `./` with `.\` in the commands below.

### 1. Clone the repository

```bash
git clone https://github.com/dimzachar/thalassa-analytics.git
cd thalassa-analytics
```

### 2. Install the required tools

You need these tools available in your shell:

- Python 3.11+
- `uv`
- `bruin`
- `terraform`
- `gcloud`

Terraform install guide:

- https://developer.hashicorp.com/terraform/install

Verify them:

```bash
python --version
uv --version
bruin --version
terraform version
gcloud version
```

### 3. Create or choose a GCP project

You need a GCP project with billing enabled.

At minimum, the project must support:

- BigQuery
- IAM
- Secret Manager
- Service Usage
- optional Artifact Registry

### 4. Authenticate to Google Cloud

The simplest local path is Application Default Credentials.

```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project YOUR_GCP_PROJECT
```

Replace `YOUR_GCP_PROJECT` with your real GCP project ID.

Alternative: use a service account JSON file and export one of these variables:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/key.json
# or
export THALASSA_GCP_SERVICE_ACCOUNT_FILE=/absolute/path/to/key.json
```

For Streamlit specifically, you can also copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml` and fill in the service account values.

### 5. Configure Bruin

Update `.bruin.yml` so the `gcp-default` connection points to your project.

Minimal example:

```yaml
default_environment: default

environments:
  default:
    connections:
      google_cloud_platform:
        - name: gcp-default
          project_id: YOUR_GCP_PROJECT
          location: EU
          use_application_default_credentials: true
```

Replace `YOUR_GCP_PROJECT` with your real GCP project ID.

### 6. Choose a setup path

Pick one of these and follow only that one.

#### One-click setup (recommended)

Use this when you want one command to set the dataset, sync Bruin, validate the pipeline, and apply Terraform.
Run the command from the repo root after Steps 4 and 5 are done.

PowerShell:

```powershell
.\scripts\set_dataset.ps1 my_dataset -ProjectId YOUR_GCP_PROJECT -Region europe-west1 -BqLocation EU -Environment prod
```

Bash:

```bash
./scripts/set_dataset.sh my_dataset --project-id YOUR_GCP_PROJECT --region europe-west1 --bq-location EU --environment prod
```

Replace `my_dataset` with the dataset name you want and `YOUR_GCP_PROJECT` with your real GCP project ID. Change `europe-west1`, `EU`, and `prod` too if your region, BigQuery location, or environment are different.

What this does:

- creates `.env` from `.env.example` if needed
- sets `THALASSA_BQ_DATASET`
- sets `THALASSA_BQ_PROJECT` and `THALASSA_BQ_LOCATION` when you pass them
- creates `infra/terraform.tfvars` from the example if needed
- writes `project_id`, `region`, `bq_location`, and `environment`
- syncs Bruin asset dataset prefixes
- runs `bruin validate ./pipeline --fast`
- runs `terraform init`, `plan`, and `apply`

The dataset name lives in `.env`. Terraform reads it from `THALASSA_BQ_DATASET`, so you do not need to repeat it in `infra/terraform.tfvars`.
Project-wide Terraform resource names are derived from that dataset too. For example, `my_dataset` becomes a service account like `my-dataset-pipeline` and an Artifact Registry repo like `my-dataset`.

Add `-AutoApprove` in PowerShell or `--auto-approve` in Bash if you want a non-interactive Terraform apply.

#### Manual setup

Use this if you want to control `.env` and Terraform inputs yourself.
Run these commands from the repo root.

PowerShell:

```powershell
Copy-Item .env.example .env
Copy-Item infra/terraform.tfvars.example infra/terraform.tfvars
```

Bash:

```bash
cp .env.example .env
cp infra/terraform.tfvars.example infra/terraform.tfvars
```

Edit `.env` and set at least:

- `THALASSA_BQ_PROJECT`
- `THALASSA_BQ_DATASET`
- `THALASSA_BQ_LOCATION`

Then edit `infra/terraform.tfvars` and set at least:

- `project_id`
- `region`
- `bq_location`
- `environment`

Then sync Bruin and apply the infrastructure:

```bash
uv run --no-project python ./scripts/sync_bruin_dataset.py
bruin validate ./pipeline --fast
terraform -chdir=infra init
terraform -chdir=infra plan
terraform -chdir=infra apply
```

Terraform creates:

- the BigQuery dataset
- a pipeline service account
- IAM bindings for BigQuery
- optional Secret Manager secret slots for AI providers
- optional Artifact Registry

Those AI secret slots are empty placeholders. You do not need to add OpenRouter, Anthropic, or Gemini keys unless you want LLM-generated intelligence text. Without them, the project still runs and falls back to deterministic reporting.

### 7. Install Python dependencies

```bash
uv sync
```

### 8. Run the initial backfill

For a first run, use `--full-refresh`. This is especially important if tables already exist and you want BigQuery partitioning and clustering to match the checked-in model definitions.

```bash
uv run --no-project python ./scripts/sync_bruin_dataset.py
bruin run --full-refresh ./pipeline/assets/ingestion/raw_sailing_traffic.py --downstream --start-date 2025-01-01 --end-date 2025-01-31
```

What this does:

- fetches API data for the selected window
- lands raw rows in `<THALASSA_BQ_DATASET>.raw_sailing_traffic`
- builds staging, intermediate, marts, and report tables
- refreshes `<THALASSA_BQ_DATASET>.intelligence_snapshots`
- triggers the snapshot writer downstream

### 9. Launch the dashboard

```bash
uv run streamlit run ./dashboard/app.py
```

Open the local Streamlit URL shown in the terminal.

### 10. Change the dataset later

Once the project is already set up, use the same script to switch datasets.

For a local-only switch without Terraform:

PowerShell:

```powershell
.\scripts\set_dataset.ps1 my_dataset -SkipTerraform
```

Bash:

```bash
./scripts/set_dataset.sh my_dataset --skip-terraform
```

Replace `my_dataset` with the dataset name you want.

If the new dataset also needs Terraform changes applied, rerun the command from [One-click setup (recommended)](#one-click-setup-recommended) without the skip flag.

### 11. Validate manually

```bash
uv run --no-project python ./scripts/sync_bruin_dataset.py
bruin validate ./pipeline --fast
bruin validate ./pipeline
```

### 12. Optional: regenerate intelligence snapshots directly

These commands are useful when you want to refresh only the narrative layer.

```bash
bruin run ./pipeline/assets/reports/intelligence_snapshots.sql --downstream
```

or

```bash
uv run python ./scripts/generate_auto_panel_snapshot.py
```

### 13. Verify that the run worked

At this point you should have:

- BigQuery tables under your configured `THALASSA_BQ_DATASET`
- a dashboard that loads KPI cards and charts successfully
- visible categorical and temporal tiles for course review

A quick warehouse sanity query is:

```sql
SELECT
  MAX(service_date) AS latest_service_date,
  SUM(total_passengers) AS passengers,
  SUM(total_vehicles) AS vehicles
FROM `YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.row_counts_daily`;
```

Replace `YOUR_GCP_PROJECT` and `YOUR_THALASSA_BQ_DATASET` with your real values.

### 14. Tear Down The Infrastructure

If you are done with the project and want to delete the GCP resources, run:

```bash
terraform -chdir=infra destroy
```

Run that from the repo root with the same `infra/terraform.tfvars` and `.env` values you used when you applied the infrastructure.
If you changed the dataset later, switch back to the deployed dataset first so Terraform points at the right resources.

Important:

- the BigQuery dataset is protected by default because `dataset_delete_contents_on_destroy = false`
- if the dataset still has tables in it, `terraform destroy` will fail instead of deleting your data

If you want Terraform to delete the dataset and everything inside it too:

1. edit `infra/terraform.tfvars`
2. set `dataset_delete_contents_on_destroy = true`
3. run `terraform -chdir=infra destroy`

If you want to keep the safer default, leave it as `false` and manually empty or delete the dataset tables first.

## Quick start

If you want the shortest reviewer path, use the full guide above but only touch these sections:

1. Complete [4. Authenticate to Google Cloud](#4-authenticate-to-google-cloud) and [5. Configure Bruin](#5-configure-bruin).
2. Run the setup flow in [6. Choose a setup path](#6-choose-a-setup-path). If the dataset and infrastructure already exist, use the local-only switch in [10. Change the dataset later](#10-change-the-dataset-later) instead.
3. Run `uv sync`.
4. Run the backfill in [8. Run the initial backfill](#8-run-the-initial-backfill).
5. Launch the dashboard in [9. Launch the dashboard](#9-launch-the-dashboard).

## Future improvements

- Add CI checks for `bruin validate`, dependency sync, and Terraform planning
- Add targeted automated tests for the intelligence and repository layers
- Deploy the Streamlit dashboard to a managed runtime for easier peer access
- Add a dedicated cloud storage landing layer in a future iteration if you want a stricter lake-to-warehouse architecture
- Expand operational monitoring around data freshness and failed pipeline runs

## Contributing

Contributions are welcome, especially around data quality, dashboard UX, testing, and deployment hardening.

Before opening a PR:

1. Keep the change focused on one concern
2. Run `uv sync` if dependencies changed
3. Run `uv run --no-project python ./scripts/sync_bruin_dataset.py`
4. Run `bruin validate ./pipeline`
5. If you changed SQL models or ingestion logic, run the relevant `bruin run` command for a representative date window
6. If you changed the dashboard, include a screenshot or short note describing the UI impact
7. In the PR description, include the purpose of the change, touched paths, and validation commands you ran

For larger changes, opening an issue first is the best way to align on scope before implementation.

## Acknowledgements

- Public dataset provided by `data.gov.gr`
- Pipeline orchestration and asset framework powered by Bruin
- Warehouse and cloud services provided by BigQuery and GCP
- Dashboard built with Streamlit
- Project structure inspired by common open-source README patterns used in mature GitHub repos
