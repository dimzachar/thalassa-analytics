resource "google_project_iam_member" "pipeline_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_serviceusage_consumer" {
  project = var.project_id
  role    = "roles/serviceusage.serviceUsageConsumer"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_data_editor" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "bq_storage_read_session_user" {
  for_each = local.bq_storage_project_members

  project = var.project_id
  role    = "roles/bigquery.readSessionUser"
  member  = each.value
}
