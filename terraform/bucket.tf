resource "aws_s3_bucket" "raw_bucket" {
  bucket        = var.raw_bucket
  force_destroy = true
}


resource "aws_s3_bucket" "processed_bucket" {
  bucket        = var.processed_bucket
  force_destroy = true
}

resource "aws_s3_bucket" "code_bucket" {
  bucket        = var.code_bucket
  force_destroy = true
}

resource "aws_s3_object" "code_bucket_object" {
  bucket     = var.code_bucket
  key        = "glue_script.py"
  source     = "glue_script.py"
  depends_on = [aws_s3_bucket.code_bucket]
}

resource "aws_s3_bucket" "gold_bucket" {
  bucket        = var.gold_bucket
  force_destroy = true
}

