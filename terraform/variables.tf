variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Base name for project resources"
  type        = string
  default     = "unstructured-data-pipeline"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "raw_bucket_name" {
  description = "Name for the raw S3 bucket"
  type        = string
  default     = "raw-images-pipeline-bucket"
}

variable "dead_letter_bucket_name" {
  description = "Name for the dead letter S3 bucket"
  type        = string
  default     = "dead-letter-pipeline-bucket"
}

variable "emr_master_instance_type" {
  description = "Instance type for EMR master node"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_type" {
  description = "Instance type for EMR core/worker nodes (Spot)"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_count" {
  description = "Number of worker nodes in EMR cluster"
  type        = number
  default     = 2
}

variable "spark_script_s3_path" {
  description = "S3 path to the PySpark embedding extraction script"
  type        = string
  # Example: s3://my-scripts-bucket/spark_jobs/extract_embeddings.py
  default     = ""
}
