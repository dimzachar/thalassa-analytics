locals {
  manages_shared_project_resources = terraform.workspace == "default"
  dotenv_path  = "${path.module}/../.env"
  dotenv_lines = fileexists(local.dotenv_path) ? split("\n", file(local.dotenv_path)) : []
  dotenv_map = {
    for line in local.dotenv_lines :
    trimspace(split(line, "=")[0]) => trimspace(join("=", slice(split(line, "="), 1, length(split(line, "=")))))
    if trimspace(line) != "" && !startswith(trimspace(line), "#") && length(split(line, "=")) > 1
  }
  env_dataset_id                            = trimspace(lookup(local.dotenv_map, "THALASSA_BQ_DATASET", ""))
  effective_dataset_id                      = local.env_dataset_id != "" ? local.env_dataset_id : var.dataset_id
  dataset_slug_raw                          = replace(lower(local.effective_dataset_id), "/[^a-z0-9]+/", "-")
  dataset_slug                              = trim(replace(local.dataset_slug_raw, "/-+/", "-"), "-")
  resource_prefix_base                      = local.dataset_slug != "" ? local.dataset_slug : "thalassa"
  resource_prefix                           = can(regex("^[a-z]", local.resource_prefix_base)) ? local.resource_prefix_base : "d-${local.resource_prefix_base}"
  effective_app_name                        = trimspace(var.app_name) != "" ? trimspace(var.app_name) : local.resource_prefix
  effective_artifact_registry_repository_id = trimspace(var.artifact_registry_repository_id) != "" ? trimspace(var.artifact_registry_repository_id) : trim(substr(local.resource_prefix, 0, min(length(local.resource_prefix), 63)), "-")
  effective_pipeline_service_account_id     = trimspace(var.pipeline_service_account_id) != "" ? trimspace(var.pipeline_service_account_id) : trim(substr("${local.resource_prefix}-pipeline", 0, min(length("${local.resource_prefix}-pipeline"), 30)), "-")

  common_labels = merge(
    {
      app         = local.effective_app_name
      environment = var.environment
      managed_by  = "terraform"
    },
    var.labels
  )

  required_apis = setunion(
    toset([
      "bigquery.googleapis.com",
      "iam.googleapis.com",
      "secretmanager.googleapis.com",
      "serviceusage.googleapis.com",
    ]),
    var.create_artifact_registry ? toset(["artifactregistry.googleapis.com"]) : toset([]),
    var.enable_bigquery_storage_roles ? toset(["bigquerystorage.googleapis.com"]) : toset([])
  )
  required_apis_in_workspace = local.manages_shared_project_resources ? local.required_apis : toset([])
  secret_ids_in_workspace    = local.manages_shared_project_resources ? toset(var.secret_ids) : toset([])

  bq_storage_project_members = var.enable_bigquery_storage_roles ? toset([
    "serviceAccount:${google_service_account.pipeline.email}",
  ]) : toset([])
}
