
# 1. S3 Data Lake Resources
resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket_name
  
  tags = {
    Name        = "CoreTelecoms Data Lake"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Enable Versioning
resource "aws_s3_bucket_versioning" "lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Create data folders
resource "aws_s3_object" "folders" {
  for_each = toset(["raw/", "processed/", "presentation/"])
  bucket   = aws_s3_bucket.data_lake.id
  key      = each.value
}

# 2. SSM Parameter Store

resource "aws_ssm_parameter" "source_access_key" {
  name        = "/coretelecoms/source/access_key"
  description = "Access Key for the Source Data Account"
  type        = "SecureString"
  value       = "CHANGE_ME_IN_CONSOLE"
  tags        = { Environment = var.environment }
  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "source_secret_key" {
  name        = "/coretelecoms/source/secret_key"
  description = "Secret Key for the Source Data Account"
  type        = "SecureString"
  value       = "CHANGE_ME_IN_CONSOLE"
  tags        = { Environment = var.environment }

  lifecycle {
    ignore_changes = [value]
  }
}



resource "aws_ssm_parameter" "db_password" {
  name        = "/coretelecoms/db/password"
  description = "Password for the Postgres DB"
  type        = "SecureString"
  value       = "CHANGE_ME_IN_CONSOLE"
  tags        = { Environment = var.environment }

  lifecycle {
    ignore_changes = [value]
  }
}



resource "aws_ssm_parameter" "db_host" {
  name        = "/coretelecoms/db/host"
  description = "Database Host Endpoint"
  type        = "String" 
  value       = "CHANGE_ME_IN_CONSOLE"
  tags        = { Environment = var.environment }
  
  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "db_name" {
  name        = "/coretelecoms/db/name"
  description = "Database Name"
  type        = "String"
  value       = "CHANGE_ME_IN_CONSOLE"
  tags        = { Environment = var.environment }

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "db_port" {
  name        = "/coretelecoms/db/port"
  description = "Database Port"
  type        = "String"
  value       = "5432" # Default Postgres port, but you can change in console
  tags        = { Environment = var.environment }

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "db_schema" {
  name        = "/coretelecoms/db/schema"
  description = "Target Schema Name"
  type        = "String"
  value       = "customer_complaints" # From your requirements
  tags        = { Environment = var.environment }

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "db_user" {
  name        = "/coretelecoms/db/user"
  description = "Database Username"
  type        = "SecureString" # Username is often considered sensitive
  value       = "CHANGE_ME_IN_CONSOLE"
  tags        = { Environment = var.environment }

  lifecycle {
    ignore_changes = [value]
  }
}

# 3. IAM User for Airflow / Python Scripts
resource "aws_iam_user" "airflow_service_user" {
  name = "coretelecoms-airflow-worker"
  tags = { Project = var.project_name }
}

# Create Access Keys for this user
resource "aws_iam_access_key" "airflow_key" {
  user = aws_iam_user.airflow_service_user.name
}

# Define Permissions
resource "aws_iam_policy" "pipeline_policy" {
  name        = "CoreTelecoms_Airflow_Policy"
  description = "Allows access to Data Lake and reading Secrets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "AllowS3Access"
        Effect   = "Allow"
        Action   = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}",
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Sid      = "AllowSSMRead"
        Effect   = "Allow"
        Action   = [
          "ssm:GetParameter",
          "ssm:GetParameters"
        ]
        Resource = "arn:aws:ssm:eu-north-1:*:parameter/coretelecoms/*"
      }
    ]
  })
}

# Attach Policy to User
resource "aws_iam_user_policy_attachment" "attach_pipeline_policy" {
  user       = aws_iam_user.airflow_service_user.name
  policy_arn = aws_iam_policy.pipeline_policy.arn
}
