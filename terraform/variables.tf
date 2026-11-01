variable "project_id" {
  description = "Your Google Cloud project id (the one BigQuery uses), for example my-project-123456."
  type        = string
}

variable "location" {
  description = "Where BigQuery stores the data. Must match BQ_LOCATION used by dbt and the loader."
  type        = string
  default     = "US"
}

variable "create_redash_user" {
  description = "Create a read-only service account for Redash? Needs the IAM API. Set false to create only the datasets."
  type        = bool
  default     = true
}
