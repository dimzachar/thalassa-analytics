# Course Project Requirements Mapping

This document maps the DE Zoomcamp course evaluation criteria to how this project addresses each one.

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
| Reproducibility | Full step-by-step setup and run instructions are included in the [README](../README.md) |
