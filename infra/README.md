# Terraform Infra

This folder is the Terraform-only reference for Thalassa.

For the full project setup flow, use the main guide in [../README.md](../README.md).
That is the source of truth for:

- Google auth
- Bruin config
- one-click setup
- manual setup
- pipeline runs
- dashboard launch

Use this file when you want just the infra-specific details.

## What Terraform Creates

- required GCP APIs
- one BigQuery dataset
- one pipeline service account
- IAM for BigQuery jobs and dataset writes
- Secret Manager secret slots
- optional Artifact Registry repository

Terraform does not create:

- local Python environments
- Streamlit deployment
- Docker image builds
- service account JSON keys
- secret values

## Inputs

Terraform reads the dataset from `../.env`:

- `THALASSA_BQ_DATASET`

It reads the rest from `infra/terraform.tfvars`:

- `project_id`
- `region`
- `bq_location`
- `environment`

Useful defaults and examples live in:

- `infra/terraform.tfvars.example`

Project-wide resource names are derived from the dataset by default.
Example:

- dataset `my_dataset`
- service account `my-dataset-pipeline`
- Artifact Registry repo `my-dataset`

The helper scripts use a separate Terraform workspace per dataset:

- `dataset-my_dataset`

## Direct Terraform Commands

If you are working on infra directly, run from the repo root:

```bash
terraform -chdir=infra init
terraform -chdir=infra workspace select dataset-my_dataset || terraform -chdir=infra workspace new dataset-my_dataset
terraform -chdir=infra plan
terraform -chdir=infra apply
```

If you want the end-to-end helper instead, use the commands in the main README.

## IAM Granted

The pipeline service account gets:

- `roles/bigquery.jobUser` on the project
- `roles/serviceusage.serviceUsageConsumer` on the project (required for BigQuery usage in Streamlit/Cloud)
- `roles/bigquery.dataEditor` on the configured dataset
- `roles/secretmanager.secretAccessor` on the configured runtime secrets

Optional:

- `roles/bigquery.readSessionUser` only if you explicitly enable BigQuery Storage API support

## Secrets

Terraform creates empty Secret Manager slots only.
It does not upload secret values.

Default secret slots:

- `openrouter-api-key`
- `anthropic-api-key`
- `google-api-key`

These are optional. The main pipeline and dashboard still work without them.
They are only needed if you want LLM-generated intelligence text.

## Destroy / Cleanup

When you are done and want to delete the GCP resources, run:

```bash
terraform -chdir=infra workspace select dataset-my_dataset
terraform -chdir=infra destroy
```

Use the same `infra/terraform.tfvars` values and dataset workspace that were used for `terraform apply`.
Dataset workspaces delete only dataset-scoped resources.
Do not run `terraform destroy` from the default workspace unless you intentionally want to manage shared project-wide resources such as enabled APIs and optional shared secret slots.

Important:

- the BigQuery dataset is protected by default because `dataset_delete_contents_on_destroy = false`
- if the dataset still contains tables, `terraform destroy` will fail instead of deleting your data

If you want Terraform to delete the dataset contents too:

1. set `dataset_delete_contents_on_destroy = true` in `infra/terraform.tfvars`
2. run `terraform -chdir=infra workspace select dataset-my_dataset`
3. run `terraform -chdir=infra destroy`

If you prefer the safer default, keep it `false` and manually empty the dataset first.
