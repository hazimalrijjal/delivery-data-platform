# The three BigQuery datasets the pipeline uses. dbt and ingest/load_bigquery.py
# would create them on demand; Terraform makes them part of written-down,
# reviewable infrastructure instead.
locals {
  datasets = {
    raw     = "Raw CSV batches exactly as loaded (text columns plus _batch_id, _source_file, _loaded_at)."
    staging = "dbt staging views: de-duplicated, typed, flagged."
    marts   = "dbt marts: facts, dimensions, KPIs, data-quality report."
  }
}

resource "google_bigquery_dataset" "this" {
  for_each = local.datasets

  dataset_id    = each.key
  friendly_name = each.key
  description   = each.value
  location      = var.location

  labels = {
    project = "delivery-data-platform"
    layer   = each.key
  }
  # No default table expiry is set: the free sandbox forces 60 days by itself,
  # and a billed project should keep its tables.
}

# Read-only account for Redash: it may read the marts and run queries, nothing else.
resource "google_service_account" "redash" {
  count        = var.create_redash_user ? 1 : 0
  account_id   = "redash-reader"
  display_name = "Redash read-only access to the marts"
}

resource "google_bigquery_dataset_iam_member" "redash_reads_marts" {
  count      = var.create_redash_user ? 1 : 0
  dataset_id = google_bigquery_dataset.this["marts"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.redash[0].email}"
}

resource "google_project_iam_member" "redash_runs_queries" {
  count   = var.create_redash_user ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.redash[0].email}"
}
