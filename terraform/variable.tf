variable "aws_region" {
  default = "us-east-1"
}

variable "raw_bucket" {
  default = "raw-bucket-s3-source"
}

variable "processed_bucket" {
  default = "processed-bucket-s3-target"
}

variable "code_bucket" {
  default = "code-bucket-s3-code"
}

variable "gold_bucket" {
  default = "gold-bucket-curated"  
}

