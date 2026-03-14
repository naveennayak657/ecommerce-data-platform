# ──────────────────────────────────────────────────
# Author: Naveen Vishlavath
# File: terraform/main.tf
#
# Main Terraform configuration for the e-commerce
# data platform infrastructure.
#
# What this provisions:
#   - S3 bucket for Delta Lake storage
#   - S3 bucket for Airflow logs
#   - IAM role for Spark to read/write S3
#   - IAM role for Airflow to trigger jobs
#
# NOTE: We use us-east-1 because it has the lowest
# latency to Snowflake's default region.
#
# To apply:
#   terraform init
#   terraform plan
#   terraform apply
# ──────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # in production this would be an S3 backend
  # so terraform state is shared across the team
  # backend "s3" {
  #   bucket = "ecommerce-terraform-state"
  #   key    = "prod/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

# ── provider ─────────────────────────────────────────
provider "aws" {
  region = var.aws_region

  # default tags apply to every resource automatically
  # no need to repeat tags on each resource
  default_tags {
    tags = {
      Project     = "ecommerce-data-platform"
      Owner       = "naveen"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ── s3 bucket — delta lake ───────────────────────────
# stores Bronze/Silver/Gold layers
# in local dev we use filesystem — this is for production
resource "aws_s3_bucket" "delta_lake" {
  bucket = "${var.project_name}-delta-lake-${var.environment}"

  tags = {
    Name    = "Delta Lake Storage"
    Purpose = "Bronze Silver Gold layers"
  }
}

# versioning protects against accidental deletes
resource "aws_s3_bucket_versioning" "delta_lake" {
  bucket = aws_s3_bucket.delta_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# data lake should never be public
resource "aws_s3_bucket_public_access_block" "delta_lake" {
  bucket                  = aws_s3_bucket.delta_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# lifecycle — move old bronze data to Glacier after 90 days
# significant cost saving in production
resource "aws_s3_bucket_lifecycle_configuration" "delta_lake" {
  bucket = aws_s3_bucket.delta_lake.id

  rule {
    id     = "bronze_to_glacier"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "silver_to_glacier"
    status = "Enabled"
    filter { prefix = "silver/" }
    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
}

# ── s3 bucket — airflow logs ─────────────────────────
resource "aws_s3_bucket" "airflow_logs" {
  bucket = "${var.project_name}-airflow-logs-${var.environment}"

  tags = {
    Name    = "Airflow Logs"
    Purpose = "Pipeline execution logs"
  }
}

resource "aws_s3_bucket_public_access_block" "airflow_logs" {
  bucket                  = aws_s3_bucket.airflow_logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── iam role — spark ─────────────────────────────────
# least privilege — spark can only access delta lake bucket
resource "aws_iam_role" "spark_role" {
  name = "${var.project_name}-spark-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "spark_s3_policy" {
  name        = "${var.project_name}-spark-s3-policy"
  description = "Allows Spark to read/write Delta Lake S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ]
      Resource = [
        aws_s3_bucket.delta_lake.arn,
        "${aws_s3_bucket.delta_lake.arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "spark_s3" {
  role       = aws_iam_role.spark_role.name
  policy_arn = aws_iam_policy.spark_s3_policy.arn
}

# ── iam role — airflow ───────────────────────────────
resource "aws_iam_role" "airflow_role" {
  name = "${var.project_name}-airflow-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "airflow_s3_policy" {
  name        = "${var.project_name}-airflow-s3-policy"
  description = "Allows Airflow to read/write logs to S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.airflow_logs.arn,
        "${aws_s3_bucket.airflow_logs.arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_s3" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_s3_policy.arn
}