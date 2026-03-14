# ──────────────────────────────────────────────────
# Author: Naveen Vishlavath
# File: terraform/variables.tf
#
# All input variables for the e-commerce platform
# infrastructure. Change these to deploy to different
# environments (dev, staging, prod).
#
# How to pass sensitive values safely:
#   export TF_VAR_snowflake_account=yzjbrlr-ro49347
#   export TF_VAR_snowflake_user=NAVEENNAYAK657
#   export TF_VAR_snowflake_password=yourpassword
#   export TF_VAR_aws_account_id=123456789012
#
# Never hardcode sensitive values in this file.
# ──────────────────────────────────────────────────

# ── general ──────────────────────────────────────────
variable "project_name" {
  description = "Name of the project — used in all resource names"
  type        = string
  default     = "ecommerce-data-platform"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

# common tags applied to all resources
# makes cost tracking and resource management easier
variable "tags" {
  description = "Common tags applied to all AWS resources"
  type        = map(string)
  default     = {
    Project   = "ecommerce-data-platform"
    Owner     = "naveen"
    ManagedBy = "terraform"
  }
}

# ── aws ───────────────────────────────────────────────
variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "aws_account_id" {
  description = "AWS account ID — used for IAM policy ARNs"
  type        = string
  sensitive   = true
  # no default — must be provided explicitly
  # export TF_VAR_aws_account_id=123456789012
}

# ── s3 ────────────────────────────────────────────────
variable "delta_lake_bucket_name" {
  description = "Name of the S3 bucket for Delta Lake storage"
  type        = string
  default     = "ecommerce-delta-lake"
}

variable "airflow_logs_bucket_name" {
  description = "Name of the S3 bucket for Airflow logs"
  type        = string
  default     = "ecommerce-airflow-logs"
}

variable "bronze_retention_days" {
  description = "Days before Bronze data moves to Glacier"
  type        = number
  default     = 90

  validation {
    condition     = var.bronze_retention_days >= 30
    error_message = "Bronze retention must be at least 30 days."
  }
}

variable "silver_retention_days" {
  description = "Days before Silver data moves to Glacier"
  type        = number
  default     = 180

  validation {
    condition     = var.silver_retention_days >= 90
    error_message = "Silver retention must be at least 90 days."
  }
}

# ── snowflake ─────────────────────────────────────────
variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
  sensitive   = true
  # no default — pass via environment variable
  # export TF_VAR_snowflake_account=yzjbrlr-ro49347
}

variable "snowflake_user" {
  description = "Snowflake username"
  type        = string
  sensitive   = true
  # export TF_VAR_snowflake_user=NAVEENNAYAK657
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
  # export TF_VAR_snowflake_password=yourpassword
  # marked sensitive — never appears in terraform plan output
}

variable "snowflake_role" {
  description = "Snowflake role to use"
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_warehouse" {
  description = "Snowflake warehouse name"
  type        = string
  default     = "COMPUTE_WH"
}

variable "snowflake_database" {
  description = "Snowflake database name"
  type        = string
  default     = "ECOMMERCE_DB"
}