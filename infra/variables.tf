variable "project_id" {
  description = "GCP project ID where Thalassa infrastructure will be created."
  type        = string
}

variable "region" {
  description = "Primary GCP region for regional resources such as Artifact Registry."
  type        = string
  default     = "europe-west1"
}

variable "bq_location" {
  description = "BigQuery location for the analytics dataset."
  type        = string
  default     = "EU"
}

variable "dataset_id" {
  description = "Internal fallback BigQuery dataset ID when ../.env does not define THALASSA_BQ_DATASET."
  type        = string
  default     = "thalassa"
}

variable "environment" {
  description = "Short environment name used in resource naming."
  type        = string
  default     = "prod"
}

variable "app_name" {
  description = "Optional override for the short application name used in labels and resource names. Defaults to a dataset-derived value."
  type        = string
  default     = ""
}

variable "labels" {
  description = "Extra labels to apply where supported."
  type        = map(string)
  default     = {}
}

variable "secret_ids" {
  description = "Secret Manager secret IDs to create as empty slots for runtime configuration."
  type        = list(string)
  default = [
    "openrouter-api-key",
    "anthropic-api-key",
    "google-api-key",
  ]
}

variable "create_artifact_registry" {
  description = "Whether to create an Artifact Registry repository for future container images."
  type        = bool
  default     = true
}

variable "artifact_registry_repository_id" {
  description = "Optional override for the Artifact Registry repository ID. Defaults to a dataset-derived value."
  type        = string
  default     = ""
}

variable "enable_bigquery_storage_roles" {
  description = "Grant BigQuery Storage Read API roles for faster dataframe downloads when explicitly enabled in the app."
  type        = bool
  default     = false
}

variable "pipeline_service_account_id" {
  description = "Optional override for the pipeline runtime service-account ID. Defaults to a dataset-derived value."
  type        = string
  default     = ""
}

variable "dataset_delete_contents_on_destroy" {
  description = "Allow Terraform destroy to delete tables inside the BigQuery dataset. Keep false for safety."
  type        = bool
  default     = false
}
