resource "google_service_account" "pipeline" {
  depends_on = [google_project_service.required]

  project      = var.project_id
  account_id   = local.effective_pipeline_service_account_id
  display_name = "Thalassa Pipeline"
  description  = "Runtime identity for Bruin pipeline runs and snapshot generation."
}
