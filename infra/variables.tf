variable "project_id" {
  description = "New GCP project id."
  type        = string
  default     = "nyc-taxi-driver-analytics"
}

variable "project_name" {
  description = "Human-readable GCP project name."
  type        = string
  default     = "NYC Taxi Driver Analytics"
}

variable "billing_account" {
  description = "Billing account id, for example 000000-000000-000000."
  type        = string
}

variable "org_id" {
  description = "Optional organization id. Use either org_id or folder_id."
  type        = string
  default     = null
}

variable "folder_id" {
  description = "Optional folder id. Use either org_id or folder_id."
  type        = string
  default     = null
}

variable "region" {
  description = "Default region for Cloud Run, Scheduler and Artifact Registry."
  type        = string
  default     = "us-central1"
}

variable "storage_location" {
  description = "Cloud Storage bucket location."
  type        = string
  default     = "US"
}

variable "bigquery_location" {
  description = "BigQuery dataset location."
  type        = string
  default     = "US"
}

variable "ingest_image" {
  description = "Container image URI for the Cloud Run Job. Leave empty for first bootstrap apply."
  type        = string
  default     = ""
}

variable "enable_scheduler" {
  description = "Whether to create the daily Cloud Scheduler trigger after ingest_image is set."
  type        = bool
  default     = true
}

variable "scheduler_paused" {
  description = "Whether the Cloud Scheduler trigger should be created in a paused state."
  type        = bool
  default     = true
}

variable "schedule_cron" {
  description = "Cloud Scheduler cron expression."
  type        = string
  default     = "0 7 * * *"
}

variable "schedule_time_zone" {
  description = "Cloud Scheduler time zone."
  type        = string
  default     = "Europe/Madrid"
}

variable "delete_contents_on_destroy" {
  description = "Allows Terraform destroy to delete BigQuery datasets and buckets containing objects. Keep false for real environments."
  type        = bool
  default     = false
}

variable "create_budget" {
  description = "Create a simple monthly billing budget guardrail."
  type        = bool
  default     = false
}

variable "monthly_budget_usd" {
  description = "Monthly budget amount in USD."
  type        = number
  default     = 5
}

variable "labels" {
  description = "Common resource labels."
  type        = map(string)
  default = {
    app         = "taxi-driver-analytics"
    environment = "portfolio"
  }
}
