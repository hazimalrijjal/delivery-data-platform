output "dataset_ids" {
  description = "The BigQuery datasets that now exist."
  value       = [for d in google_bigquery_dataset.this : d.dataset_id]
}

output "redash_service_account" {
  description = "Email of the read-only service account (empty when create_redash_user = false)."
  value       = var.create_redash_user ? google_service_account.redash[0].email : ""
}
