terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.21.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "movies_datalake" {
  name     = "movies-datalake"
  location = var.region

  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 5 //days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "movies_raw_dataset" {
  project                    = var.project
  location                   = var.region
  dataset_id                 = var.movies_raw_dataset
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "movies_analytics_datasets" {
  project                    = var.project
  location                   = var.region
  dataset_id                 = var.movies_analytics_datasets
  delete_contents_on_destroy = true
}
