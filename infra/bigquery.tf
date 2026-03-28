resource "google_bigquery_dataset" "warehouse" {
  depends_on = [google_project_service.required]

  project                    = var.project_id
  dataset_id                 = local.effective_dataset_id
  location                   = var.bq_location
  description                = "Analytics warehouse for the Thalassa maritime traffic pipeline."
  delete_contents_on_destroy = var.dataset_delete_contents_on_destroy
  labels                     = local.common_labels
}
