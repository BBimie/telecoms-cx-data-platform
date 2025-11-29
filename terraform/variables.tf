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

variable "gcp_project_id" {}

variable "snowflake_organization_name" {}
variable "snowflake_account_name" {}
variable "snowflake_user" {}
variable "snowflake_password" { sensitive = true }
variable "snowflake_role" { default = "ACCOUNTADMIN" }

variable "snowflake_db_name" {
  description = "Name of the Snowflake Database"
  default     = "CORE_TELECOMS"
}