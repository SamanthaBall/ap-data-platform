
# Orchestrating pipelines via scheduled and event-based triggers

module "cloud-functions-gcs-0" {
  source         = "../../../modules/gcs"
  project_id     = module.orch-project.project_id
  prefix         = var.prefix
  name           = "cloud-functions-gcs-0"
  location       = var.region
  storage_class  = "STANDARD"
  encryption_key = var.service_encryption_keys.storage
  force_destroy  = !var.deletion_protection
}


module "cf-trigger-dataproc" {
  source      = "../../../modules/cloud-function-v2"
  project_id  = module.orch-project.project_id
  region      = var.region
  name        = "cf-trigger-dataproc"
  bucket_name = module.cloud-functions-gcs-0.name
  bundle_config = {
    path = "cloud-functions/twitter/bronze_to_silver.py"
  }
  environment_variables = {
    PROJECT_ID = module.orch-project.project_id
    REGION     = var.region
  }
}

# module "cf-http-two" {
#   source      = "../../../modules/cloud-function-v2"
#   project_id  = module.orch-project.project_id
#   region      = var.region
#   name        = "test-cf-http-two"
#   bucket_name = "cloud-functions-gcs-0"
#   bundle_config = {
#     path = "assets/sample-function/"
#   }
#   depends_on = [
#     google_project_iam_member.bucket_default_compute_account_grant,
#   ]
# }
# tftest fixtures=fixtures/functions-default-sa-iam-grants.tf inventory=multiple_functions.yaml e2e



# resource "google_cloud_functions_function_iam_member" "invoker" {
#   project        = google_cloud_functions_function.my_function.project
#   region         = google_cloud_functions_function.my_function.region
#   cloud_function = google_cloud_functions_function.my_function.name

#   role   = "roles/cloudfunctions.invoker"
#   member = "serviceAccount:${google_cloud_scheduler_job.my_job.service_account_email}"
# }



# # Scheduled pipeline

# resource "google_cloud_scheduler_job" "my_job" {
#   name             = "my-job"
#   description      = "A description of my job"
#   schedule         = "*/5 * * * *" # Example cron schedule
#   time_zone        = "UTC"

#   http_target {
#     http_method = "POST"
#     uri         = "<YOUR_CLOUD_FUNCTION_URL>"

#     oauth_token {
#       service_account_email = "<YOUR_SERVICE_ACCOUNT_EMAIL>"
#     }
#   }
# }


# # Event-based pipeline

# resource "google_cloud_functions_function" "my_function" {
#   name        = "my-function"
#   runtime     = "python39" # Choose your runtime
#   entry_point = "my_function_entry_point"

#   source_archive_bucket = "<YOUR_BUCKET>"
#   source_archive_object = "<YOUR_ARCHIVE_OBJECT>"
  
#   trigger_http = true
  
#   environment_variables = {
#     ENV_VAR_1 = "value1"
#     ENV_VAR_2 = "value2"
#   }

#   service_account_email = "<YOUR_SERVICE_ACCOUNT_EMAIL>"
# }
