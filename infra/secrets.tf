resource "google_secret_manager_secret" "runtime" {
  depends_on = [google_project_service.required]
  for_each   = toset(var.secret_ids)

  project   = var.project_id
  secret_id = each.value
  labels    = local.common_labels

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "pipeline_secret_accessor" {
  for_each = google_secret_manager_secret.runtime

  project   = var.project_id
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline.email}"
}
