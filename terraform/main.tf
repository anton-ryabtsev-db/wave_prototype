provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

resource "google_pubsub_topic" "topic" {
  name = "job-topic"
}

resource "google_cloud_scheduler_job" "job" {
  name        = "test-job"
  description = "test job"
  schedule    = "* * * * *"

  pubsub_target {
    topic_name = google_pubsub_topic.topic.id
    data       = base64encode("test")
  }
}

resource "google_storage_bucket" "pub_sub_example" {
  name          = "pub_sub_example"
  location      = var.region
  force_destroy = true
}
