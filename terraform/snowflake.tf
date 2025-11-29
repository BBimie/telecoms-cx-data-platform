# 1. Virtual Warehouse (The Compute Engine)
resource "snowflake_warehouse" "compute_wh" {
  name           = "CORE_TELECOMS_WH"
  warehouse_size = "x-small" # Cheapest option
  auto_suspend   = 60        # Shut down after 300 seconds of inactivity (Save money!)
  auto_resume    = true
}

# 2. The Database
resource "snowflake_database" "core_telecoms" {
  name = var.snowflake_db_name
}

# 3. The Schemas (Medallion Architecture)
# BRONZE: Raw data exactly as it comes from S3
resource "snowflake_schema" "raw" {
  database = snowflake_database.core_telecoms.name
  name     = "RAW"
}

# SILVER: Cleaned, typed data (dbt models)
resource "snowflake_schema" "staging" {
  database = snowflake_database.core_telecoms.name
  name     = "STAGING"
}

# GOLD: Aggregated business logic (dbt models
resource "snowflake_schema" "marts" {
  database = snowflake_database.core_telecoms.name
  name     = "MARTS"
}

# 4. Service User for dbt/Airflow
resource "snowflake_user" "service_user" {
  name         = "SVC_AIRFLOW_DBT"
  login_name   = "SVC_AIRFLOW_DBT"
  password     = "ChangeMe123!"
  default_role = "SYSADMIN"
}

# Permissions for the dbt/Airflow User
resource "snowflake_grant_privileges_to_account_role" "grant_sysadmin" {
  privileges        = ["USAGE", "OPERATE", "MONITOR"]
  account_role_name = "SYSADMIN"
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.compute_wh.name
  }
}