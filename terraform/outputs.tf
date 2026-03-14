# ──────────────────────────────────────────────────
# Author: Naveen Vishlavath
# File: terraform/outputs.tf
#
# Output values exposed after terraform apply.
# These are used by:
#   - Other terraform modules
#   - CI/CD pipelines
#   - Application configs (Spark, Airflow, dbt)
#
# To see outputs after apply:
#   terraform output
#   terraform output delta_lake_bucket_name
# ──────────────────────────────────────────────────

# ── s3 outputs ───────────────────────────────────────
output "delta_lake_bucket_name" {
  description = "Name of the Delta Lake S3 bucket"
  value       = aws_s3_bucket.delta_lake.bucket
}

output "delta_lake_bucket_arn" {
  description = "ARN of the Delta Lake S3 bucket"
  value       = aws_s3_bucket.delta_lake.arn
}

output "delta_lake_bucket_region" {
  description = "Region of the Delta Lake S3 bucket"
  value       = aws_s3_bucket.delta_lake.region
}

output "airflow_logs_bucket_name" {
  description = "Name of the Airflow logs S3 bucket"
  value       = aws_s3_bucket.airflow_logs.bucket
}

output "airflow_logs_bucket_arn" {
  description = "ARN of the Airflow logs S3 bucket"
  value       = aws_s3_bucket.airflow_logs.arn
}

# ── medallion layer paths ─────────────────────────────
# these paths are used by Spark and Airflow
# to know exactly where to read/write data
output "bronze_path" {
  description = "S3 path for Bronze layer — raw events"
  value       = "s3://${aws_s3_bucket.delta_lake.bucket}/bronze"
}

output "silver_path" {
  description = "S3 path for Silver layer — cleaned events"
  value       = "s3://${aws_s3_bucket.delta_lake.bucket}/silver"
}

output "gold_path" {
  description = "S3 path for Gold layer — business aggregations"
  value       = "s3://${aws_s3_bucket.delta_lake.bucket}/gold"
}

output "checkpoint_path" {
  description = "S3 path for Spark streaming checkpoints"
  value       = "s3://${aws_s3_bucket.delta_lake.bucket}/checkpoints"
}

# ── iam outputs ───────────────────────────────────────
output "spark_role_arn" {
  description = "ARN of the Spark IAM role"
  value       = aws_iam_role.spark_role.arn
}

output "spark_role_name" {
  description = "Name of the Spark IAM role"
  value       = aws_iam_role.spark_role.name
}

output "airflow_role_arn" {
  description = "ARN of the Airflow IAM role"
  value       = aws_iam_role.airflow_role.arn
}

output "airflow_role_name" {
  description = "Name of the Airflow IAM role"
  value       = aws_iam_role.airflow_role.name
}

# ── snowflake outputs ─────────────────────────────────
# non-sensitive snowflake config
# password and account are sensitive — not exposed here
output "snowflake_database" {
  description = "Snowflake database name"
  value       = var.snowflake_database
}

output "snowflake_warehouse" {
  description = "Snowflake warehouse name"
  value       = var.snowflake_warehouse
}

output "snowflake_role" {
  description = "Snowflake role used for dbt and data loading"
  value       = var.snowflake_role
}

# ── environment outputs ───────────────────────────────
output "environment" {
  description = "Current deployment environment"
  value       = var.environment
}

output "aws_region" {
  description = "AWS region resources are deployed in"
  value       = var.aws_region
}

output "project_name" {
  description = "Project name used in resource naming"
  value       = var.project_name
}