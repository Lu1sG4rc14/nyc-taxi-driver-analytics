locals {
  deploy_job = var.ingest_image != ""

  datasets = toset([
    "L00_staging",
    "L10_bronze",
    "L20_silver",
    "L30_gold",
    "L100_ops",
  ])

  required_services = toset([
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "cloudbilling.googleapis.com",
    "billingbudgets.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "cloudscheduler.googleapis.com",
    "iam.googleapis.com",
    "run.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
  ])
}

resource "google_project" "this" {
  name            = var.project_name
  project_id      = var.project_id
  billing_account = var.billing_account
  org_id          = var.org_id
  folder_id       = var.folder_id
  labels          = var.labels
}

resource "google_project_service" "apis" {
  for_each = local.required_services

  project            = google_project.this.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "raw" {
  project                     = google_project.this.project_id
  name                        = "${var.project_id}-taxi-raw"
  location                    = var.storage_location
  uniform_bucket_level_access = true
  force_destroy               = var.delete_contents_on_destroy
  labels                      = var.labels

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age            = 90
      matches_prefix = ["generated/"]
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "staging" {
  project                     = google_project.this.project_id
  name                        = "${var.project_id}-taxi-staging"
  location                    = var.storage_location
  uniform_bucket_level_access = true
  force_destroy               = var.delete_contents_on_destroy
  labels                      = var.labels

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset" "datasets" {
  for_each = local.datasets

  project                    = google_project.this.project_id
  dataset_id                 = each.value
  location                   = var.bigquery_location
  delete_contents_on_destroy = var.delete_contents_on_destroy
  labels                     = var.labels

  depends_on = [google_project_service.apis]
}

resource "google_artifact_registry_repository" "containers" {
  project       = google_project.this.project_id
  location      = var.region
  repository_id = "taxi-driver-analytics"
  description   = "Container images for the NYC taxi driver analytics pipeline."
  format        = "DOCKER"
  labels        = var.labels

  depends_on = [google_project_service.apis]
}

resource "google_service_account" "ingest" {
  project      = google_project.this.project_id
  account_id   = "taxi-ingest-job"
  display_name = "Taxi ingest Cloud Run Job"

  depends_on = [google_project_service.apis]
}

resource "google_project_iam_member" "ingest_bigquery_job_user" {
  project = google_project.this.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_project_iam_member" "ingest_logging_writer" {
  project = google_project.this.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_bigquery_dataset_iam_member" "ingest_dataset_editor" {
  for_each = google_bigquery_dataset.datasets

  project    = google_project.this.project_id
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_storage_bucket_iam_member" "ingest_raw_object_admin" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_storage_bucket_iam_member" "ingest_staging_object_admin" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_cloud_run_v2_job" "ingest" {
  count = local.deploy_job ? 1 : 0

  project  = google_project.this.project_id
  name     = "taxi-driver-ingest"
  location = var.region
  labels   = var.labels

  deletion_protection = false

  template {
    template {
      service_account = google_service_account.ingest.email
      timeout         = "3600s"
      max_retries     = 1

      containers {
        image = var.ingest_image

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }

        env {
          name  = "PROJECT_ID"
          value = google_project.this.project_id
        }
        env {
          name  = "RAW_BUCKET"
          value = google_storage_bucket.raw.name
        }
        env {
          name  = "STAGING_BUCKET"
          value = google_storage_bucket.staging.name
        }
        env {
          name  = "BIGQUERY_LOCATION"
          value = var.bigquery_location
        }
        env {
          name  = "SOURCE_MONTHS"
          value = ""
        }
        env {
          name  = "SOURCE_DATE"
          value = ""
        }
        env {
          name  = "SOURCE_START_DATE"
          value = ""
        }
        env {
          name  = "SOURCE_END_DATE"
          value = ""
        }
        env {
          name  = "FORCE_RELOAD"
          value = "false"
        }
      }
    }
  }

  depends_on = [
    google_artifact_registry_repository.containers,
    google_bigquery_dataset_iam_member.ingest_dataset_editor,
    google_storage_bucket_iam_member.ingest_raw_object_admin,
    google_storage_bucket_iam_member.ingest_staging_object_admin,
  ]
}

resource "google_service_account" "scheduler" {
  count = local.deploy_job && var.enable_scheduler ? 1 : 0

  project      = google_project.this.project_id
  account_id   = "taxi-scheduler"
  display_name = "Taxi daily scheduler"
}

resource "google_project_iam_custom_role" "cloud_run_job_runner" {
  count = local.deploy_job && var.enable_scheduler ? 1 : 0

  project     = google_project.this.project_id
  role_id     = "cloudRunJobRunner"
  title       = "Cloud Run Job Runner"
  description = "Allows triggering Cloud Run Jobs without broader administration rights."
  permissions = ["run.jobs.run"]
}

resource "google_project_iam_member" "scheduler_run_job" {
  count = local.deploy_job && var.enable_scheduler ? 1 : 0

  project = google_project.this.project_id
  role    = google_project_iam_custom_role.cloud_run_job_runner[0].name
  member  = "serviceAccount:${google_service_account.scheduler[0].email}"
}

resource "google_service_account_iam_member" "scheduler_act_as_ingest" {
  count = local.deploy_job && var.enable_scheduler ? 1 : 0

  service_account_id = google_service_account.ingest.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.scheduler[0].email}"
}

resource "google_cloud_scheduler_job" "daily_ingest" {
  count = local.deploy_job && var.enable_scheduler ? 1 : 0

  project     = google_project.this.project_id
  region      = var.region
  name        = "taxi-driver-daily-ingest"
  description = "Runs the NYC taxi incremental ingest job daily."
  schedule    = var.schedule_cron
  time_zone   = var.schedule_time_zone
  paused      = var.scheduler_paused

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${google_project.this.project_id}/jobs/${google_cloud_run_v2_job.ingest[0].name}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler[0].email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [
    google_project_iam_member.scheduler_run_job,
    google_service_account_iam_member.scheduler_act_as_ingest,
  ]
}

resource "google_billing_budget" "monthly_guardrail" {
  count = var.create_budget ? 1 : 0

  billing_account = var.billing_account
  display_name    = "${var.project_id} monthly guardrail"

  budget_filter {
    projects = ["projects/${google_project.this.number}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.monthly_budget_usd)
    }
  }

  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 0.9
  }
  threshold_rules {
    threshold_percent = 1.0
  }

  depends_on = [google_project_service.apis]
}
