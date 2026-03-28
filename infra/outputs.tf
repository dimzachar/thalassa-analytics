output "bigquery_dataset_fqn" {
  description = "Fully qualified BigQuery dataset name."
  value       = "${var.project_id}.${local.effective_dataset_id}"
}

output "pipeline_service_account_email" {
  description = "Email of the pipeline service account."
  value       = google_service_account.pipeline.email
}

output "secret_ids" {
  description = "Secret Manager secret IDs created for runtime configuration."
  value       = sort([for secret in google_secret_manager_secret.runtime : secret.secret_id])
}

output "artifact_registry_repository" {
  description = "Artifact Registry repository resource name, if created."
  value       = try(google_artifact_registry_repository.containers[0].name, null)
}

output "artifact_registry_docker_repo" {
  description = "Docker push/pull hostname path for the repository, if created."
  value       = var.create_artifact_registry ? "${var.region}-docker.pkg.dev/${var.project_id}/${local.effective_artifact_registry_repository_id}" : null
}
