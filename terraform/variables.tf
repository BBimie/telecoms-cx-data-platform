variable "project_name" {
  description = "Project name for tagging resources"
  default     = "CoreTelecoms-Project"
}

variable "data_lake_bucket_name" {
  description = "Unique name the Data Lake S3 Bucket"
  default     = "coretelecoms-datalake-2025" 
}

variable "environment" {
  description = "Deployment Environment"
  default     = "dev"
}

variable "gcp_project_id" {
  description = "The ID of the Google Cloud Project"
  type        = string
  default     = "root-logic-479318-d0"
}