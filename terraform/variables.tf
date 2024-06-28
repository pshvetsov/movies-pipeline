variable "project" {
  type        = string
  description = "GCP project ID"
  default     = "movies-pipeline"
}

variable "region" {
  type        = string
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "europe-west3"
}

variable "storage_class" {
  type        = string
  description = "The Storage Class of the new bucket. Ref: https://cloud.google.com/storage/docs/storage-classes"
  default     = "STANDARD"
}

variable "movies_raw_dataset" {
  type        = string
  description = "Dataset in BigQuery where raw data (external tables) will be loaded."
  default     = "movies_raw"
}

variable "movies_analytics_datasets" {
  type        = string
  description = "Dataset in BigQuery where raw data (from Google Cloud Storage and DBT) will be loaded."
  default     = "movies_analytics"
}
