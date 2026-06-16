resource "aws_iam_role" "redshift_serverless_role" {
  name = "redshift_serverless_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = ["redshift.amazonaws.com",
          "redshift-serverless.amazonaws.com"]
        }
      },
    ]
  })
}

resource "aws_iam_policy" "redshift_serverless_policy" {
  name = "redshift_serverless_policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:GetObjectVersion",
          "s3:ListMultipartUploadParts",
          "s3:ListBucketMultipartUploads"
        ]
        Effect   = "Allow"
        Resource = ["arn:aws:s3:::gold-bucket-curated", "arn:aws:s3:::gold-bucket-curated/*"]
      }
    ]
  }
)
}

resource "aws_iam_role_policy_attachment" "attach_redshift_policy" {
  role       = aws_iam_role.redshift_serverless_role.name
  policy_arn = aws_iam_policy.redshift_serverless_policy.arn
}
