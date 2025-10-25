variable "credentials" {
  description = "My Credentials"
  default     = "/Users/anirudh/repos/de-zoomcamp/01-docker-terraform/terraform/keys/ny-taxi-rides-creds.json"
}

variable "project" {
  description = "Project"
  default     = "ny-rides-476115"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "ny-rides-476115-demo-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

