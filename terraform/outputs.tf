output "data_lake_bucket" {
  value = aws_s3_bucket.data_lake.id
}

output "gcp_project_id" {
  description = "The Google Cloud Project ID used"
  value       = var.gcp_project_id
}
