# Copyright 2024 Google LLC
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

# tfdoc:file:description Data Warehouse projects.

locals {
  dwh_iam = {
    data_analysts = [
      "roles/bigquery.dataViewer",
      "roles/bigquery.jobUser",
      "roles/datacatalog.tagTemplateViewer",
      "roles/datacatalog.viewer",
      "roles/storage.objectViewer"
    ]
    data_engineers = [
      "roles/bigquery.dataViewer",
      "roles/bigquery.jobUser",
      "roles/datacatalog.tagTemplateViewer",
      "roles/datacatalog.viewer",
      "roles/storage.objectViewer"
    ]
    sa_transf_bq = [
      "roles/bigquery.dataOwner",
      "roles/bigquery.jobUser"
    ]
    sa_transf_df = [
      "roles/bigquery.dataOwner",
      "roles/storage.objectAdmin"
    ]
  }
  lnd_iam = {
    data_engineers = [
      "roles/bigquery.dataViewer",
      "roles/bigquery.jobUser",
      "roles/datacatalog.tagTemplateViewer",
      "roles/datacatalog.viewer",
      "roles/storage.objectViewer"
    ]
    sa_load = [
      "roles/bigquery.dataOwner",
      "roles/bigquery.jobUser",
      "roles/storage.objectCreator"
    ]
    sa_transf_bq = [
      "roles/bigquery.dataViewer",
      "roles/datacatalog.categoryAdmin"
    ]
    sa_transf_df = [
      "roles/bigquery.dataViewer"
    ]
  }
}

# Project

module "dwh-bronze-project" {
  source          = "../../../modules/project"
  parent          = var.project_config.parent
  billing_account = var.project_config.billing_account_id
  project_create  = var.project_config.project_create
  prefix          = local.use_projects ? null : var.prefix
  name = (
    local.use_projects
    ? var.project_config.project_ids.dwh-bronze
    : "${var.project_config.project_ids.dwh-bronze}${local.project_suffix}"
  )
  iam                   = local.use_projects ? {} : local.lnd_iam_auth
  iam_bindings_additive = !local.use_projects ? {} : local.lnd_iam_additive
  services              = local.dwh_services
  service_encryption_key_ids = {
    "bigquery.googleapis.com" = compact([var.service_encryption_keys.bq])
    "storage.googleapis.com"  = compact([var.service_encryption_keys.storage])
  }
}

module "dwh-silver-project" {
  source          = "../../../modules/project"
  parent          = var.project_config.parent
  billing_account = var.project_config.billing_account_id
  project_create  = var.project_config.project_create
  prefix          = local.use_projects ? null : var.prefix
  name = (
    local.use_projects
    ? var.project_config.project_ids.dwh-silver
    : "${var.project_config.project_ids.dwh-silver}${local.project_suffix}"
  )
  iam                   = local.use_projects ? {} : local.dwh_iam_auth
  iam_bindings_additive = !local.use_projects ? {} : local.dwh_iam_additive
  services              = local.dwh_services
  service_encryption_key_ids = {
    "bigquery.googleapis.com" = compact([var.service_encryption_keys.bq])
    "storage.googleapis.com"  = compact([var.service_encryption_keys.storage])
  }
}

module "dwh-gold-project" {
  source          = "../../../modules/project"
  parent          = var.project_config.parent
  billing_account = var.project_config.billing_account_id
  project_create  = var.project_config.project_create
  prefix          = local.use_projects ? null : var.prefix
  name = (
    local.use_projects
    ? var.project_config.project_ids.dwh-gold
    : "${var.project_config.project_ids.dwh-gold}${local.project_suffix}"
  )
  iam                   = local.use_projects ? {} : local.dwh_iam_auth
  iam_bindings_additive = !local.use_projects ? {} : local.dwh_iam_additive
  services              = local.dwh_services
  service_encryption_key_ids = {
    "bigquery.googleapis.com" = compact([var.service_encryption_keys.bq])
    "storage.googleapis.com"  = compact([var.service_encryption_keys.storage])
  }
}

module "dwh-conf-project" {
  source          = "../../../modules/project"
  parent          = var.project_config.parent
  billing_account = var.project_config.billing_account_id
  project_create  = var.project_config.project_create
  prefix          = local.use_projects ? null : var.prefix
  name = (
    local.use_projects
    ? var.project_config.project_ids.dwh-conf
    : "${var.project_config.project_ids.dwh-conf}${local.project_suffix}"
  )
  iam                   = local.use_projects ? {} : local.dwh_iam_auth
  iam_bindings_additive = !local.use_projects ? {} : local.dwh_iam_additive
  services              = local.dwh_services
  service_encryption_key_ids = {
    "bigquery.googleapis.com" = compact([var.service_encryption_keys.bq])
    "storage.googleapis.com"  = compact([var.service_encryption_keys.storage])
  }
}


# bigquery not needed for the bronze layer? (raw)
# module "dwh-bronze-bq-0" {
#   source         = "../../../modules/bigquery-dataset"
#   project_id     = module.dwh-lnd-project.project_id
#   id             = "${replace(var.prefix, "-", "_")}_dwh_lnd_bq_0"
#   location       = var.location
#   encryption_key = var.service_encryption_keys.bq
# }

module "dwh-silver-bq-0" {
  source         = "../../../modules/bigquery-dataset"
  project_id     = module.dwh-cur-project.project_id
  id             = "${replace(var.prefix, "-", "_")}_dwh_silver_bq_0"
  location       = var.location
  encryption_key = var.service_encryption_keys.bq
}

module "dwh-gold-bq-0" {
  source         = "../../../modules/bigquery-dataset"
  project_id     = module.dwh-cur-project.project_id
  id             = "${replace(var.prefix, "-", "_")}_dwh_gold_bq_0"
  location       = var.location
  encryption_key = var.service_encryption_keys.bq
}

module "dwh-conf-bq-0" {
  source         = "../../../modules/bigquery-dataset"
  project_id     = module.dwh-conf-project.project_id
  id             = "${replace(var.prefix, "-", "_")}_dwh_conf_bq_0"
  location       = var.location
  encryption_key = var.service_encryption_keys.bq
}

module "dwh-bronze-gcs-0" {
  source         = "../../../modules/gcs"
  project_id     = module.dwh-bronze-project.project_id
  prefix         = var.prefix
  name           = "dwh-bronze"
  location       = var.location
  storage_class  = "STANDARD"
  encryption_key = var.service_encryption_keys.storage
  force_destroy  = !var.deletion_protection
}

module "dwh-silver-cs-0" {
  source         = "../../../modules/gcs"
  project_id     = module.dwh-silver-project.project_id
  prefix         = var.prefix
  name           = "dwh-silver"
  location       = var.location
  storage_class  = "STANDARD"
  encryption_key = var.service_encryption_keys.storage
  force_destroy  = !var.deletion_protection
}


module "dwh-gold-cs-0" {
  source         = "../../../modules/gcs"
  project_id     = module.dwh-gold-project.project_id
  prefix         = var.prefix
  name           = "dwh-gold"
  location       = var.location
  storage_class  = "STANDARD"
  encryption_key = var.service_encryption_keys.storage
  force_destroy  = !var.deletion_protection
}



module "dwh-conf-cs-0" {
  source         = "../../../modules/gcs"
  project_id     = module.dwh-conf-project.project_id
  prefix         = var.prefix
  name           = "dwh-conf-cs-0"
  location       = var.location
  storage_class  = "STANDARD"
  encryption_key = var.service_encryption_keys.storage
  force_destroy  = !var.deletion_protection
}
