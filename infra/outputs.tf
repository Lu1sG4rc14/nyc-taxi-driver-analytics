output "project_id" {
  value = google_project.this.project_id
}

output "raw_bucket" {
  value = google_storage_bucket.raw.name
}

output "staging_bucket" {
  value = google_storage_bucket.staging.name
}

output "artifact_registry_repository" {
  value = "${var.region}-docker.pkg.dev/${google_project.this.project_id}/${google_artifact_registry_repository.containers.repository_id}"
}

output "ingest_job_name" {
  value = length(google_cloud_run_v2_job.ingest) == 0 ? null : google_cloud_run_v2_job.ingest[0].name
}

output "scheduler_job_name" {
  value = length(google_cloud_scheduler_job.daily_ingest) == 0 ? null : google_cloud_scheduler_job.daily_ingest[0].name
}
