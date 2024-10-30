
# Orchestrating pipelines via scheduled and event-based triggers



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
