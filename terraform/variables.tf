variable "project_id" {
  type = string
  default = "sandbox-307310"
}

variable "region" {
  type = string
  default = "europe-west3"
}

variable "zone" {
  type = string
  default = "europe-west3-a"
}

variable "template_gcs_path" {
  type = string
  description = "The GCS path to the Dataflow job template. Expect 'gs://<bucket>/<path>'. Ex: 'gs://my-bucket/templates/template_file'"
  default = "gs://pub_sub_example/samples/dataflow/templates/word-count-streaming-beam.json"
}
