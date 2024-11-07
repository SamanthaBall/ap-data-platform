# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# tfdoc:file:description common project.

locals {
  cmn_iam = {
    data_analysts = [
      # uncomment if access to all tagged columns is needed
      # "roles/datacatalog.categoryFineGrainedReader",
      "roles/datacatalog.viewer"
    ]
    data_engineers = [
      "roles/dlp.estimatesAdmin",
      "roles/dlp.reader",
      "roles/dlp.user"
    ]
    data_security = [
      "roles/datacatalog.admin",
      "roles/dlp.admin"
    ]
    sa_load = [
      "roles/datacatalog.viewer",
      "roles/dlp.user"
    ]
    sa_transf_bq = [
      "roles/datacatalog.categoryFineGrainedReader",
      "roles/datacatalog.viewer"
    ]
    sa_transf_df = [
      "roles/datacatalog.categoryFineGrainedReader",
      "roles/datacatalog.viewer",
      "roles/dlp.user"
    ]
  }
}

module "common-project" {
  source          = "../../../modules/project"
  parent          = var.project_config.parent
  billing_account = var.project_config.billing_account_id
  project_create  = var.project_config.project_create
  prefix          = local.use_projects ? null : var.prefix
  name = (
    local.use_projects
    ? var.project_config.project_ids.common
    : "${var.project_config.project_ids.common}${local.project_suffix}"
  )
  iam                   = local.use_projects ? {} : local.cmn_iam_auth
  iam_bindings_additive = !local.use_projects ? {} : local.cmn_iam_additive
  services = concat(var.project_services, [
    "datacatalog.googleapis.com",
    "dlp.googleapis.com",
  ])
}

module "common-datacatalog" {
  source     = "../../../modules/data-catalog-policy-tag"
  project_id = module.common-project.project_id
  name       = "${var.prefix}-datacatalog-policy-tags"
  location   = var.location
  tags       = var.data_catalog_tags
}


# Example: Defining data assets

resource "google_data_catalog_entry_group" "data_lakehouse_group" {
  project = module.common-project.project_id
  region = var.region
  entry_group_id = "data_lakehouse"
  display_name = "Data Lakehouse Assets"
}

resource "google_data_catalog_entry" "data_lakehouse_entry" {
  entry_group = google_data_catalog_entry_group.data_lakehouse_group.id
  entry_id = "bronze_layer"
  type = "DATA_STREAM"
  display_name = "Bronze Layer - Raw Data"
  gcs_fileset_spec {
    file_patterns = ["gs://bronze_layer_bucket/*"]
  }
}


# Example DLP scan for bronze

resource "google_data_loss_prevention_job_trigger" "dlp_bronze_layer" {
  parent = "projects/my-project-name"
  description = "Description"
  #job_id = "dlp_inspect_bronze_layer"
  display_name = "DLP Scan - Bronze Layer"
  
  inspect_job {
    storage_config {
      cloud_storage_options {
        file_set {
          url = "gs://bronze_layer_bucket/*"
        }
      }
    }

    inspect_config {
      info_types { name = ["EMAIL_ADDRESS", "PHONE_NUMBER", "CREDIT_CARD_NUMBER", "SOCIAL_SECURITY_NUMBER"]}
      min_likelihood = "POSSIBLE"
      limits {
        max_findings_per_request = 100
      }
    }

    actions {
      save_findings {
        output_config {
          table {
            project_id = module.common-project.project_id
            dataset_id = "dlp_results"
            table_id   = "bronze_layer_findings"
          }
        }
      }
    }
  }

  triggers{
     schedule {
    recurrence_period_duration = "604800s"  # Weekly
  }
  }

}



# To create KMS keys in the common project: uncomment this section
# and assign key links accondingly in local.service_encryption_keys variable

# module "cmn-kms-0" {
#   source     = "../../../modules/kms"
#   project_id = module.common-project.project_id
#   keyring = {
#     name     = "${var.prefix}-kr-global",
#     location = "global"
#   }
#   keys = {
#     pubsub = null
#   }
# }

# module "cmn-kms-1" {
#   source     = "../../../modules/kms"
#   project_id = module.common-project.project_id
#   keyring = {
#     name     = "${var.prefix}-kr-mregional",
#     location = var.location
#   }
#   keys = {
#     bq      = null
#     storage = null
#   }
# }

# module "cmn-kms-2" {
#   source     = "../../../modules/kms"
#   project_id = module.cmn-prj.project_id
#   keyring = {
#     name     = "${var.prefix}-kr-regional",
#     location = var.region
#   }
#   keys = {
#     composer = null
#     dataflow = null
#   }
# }
