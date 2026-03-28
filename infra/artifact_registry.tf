resource "google_artifact_registry_repository" "containers" {
  count = var.create_artifact_registry ? 1 : 0

  depends_on = [google_project_service.required]

  project       = var.project_id
  location      = var.region
  repository_id = local.effective_artifact_registry_repository_id
  description   = "Container images for Thalassa services."
  format        = "DOCKER"
  labels        = local.common_labels
}
