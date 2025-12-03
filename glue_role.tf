resource "aws_iam_role" "glue_role" {
  name = "glue_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_role_policy" "glue_role_policy" {
  name = "glue_role_policy"
  role = aws_iam_role.glue_role.id
  policy = jsonencode({
	"Version": "2012-10-17",
	"Statement": [
		{
			"Action": [
				"s3:ListBucket",
				"s3:GetObject",
				"s3:PutObject",
				"s3:DeleteObject",
				"s3:GetObjectVersion"
			],
			"Effect": "Allow",
			"Resource": [
				"arn:aws:s3:::*",
				"arn:aws:s3:::*/*"
			]
		},
		{
			"Action": [
				"logs:CreateLogGroup",
				"logs:CreateLogStream",
				"logs:PutLogEvents"
			],
			"Effect": "Allow",
			"Resource": "arn:aws:logs:*:*:*"
		},
		{
			"Action": [
				"glue:DeleteTable",
				"glue:CreateDatabase",
				"glue:GetDatabase",
				"glue:GetDatabases",
				"glue:GetTable",
				"glue:GetTables",
				"glue:GetConnection",
				"glue:GetJob",
				"glue:StartJobRun",
				"glue:GetJobRun",
				"glue:CreateTable",
				"glue:UpdateTable",
				"glue:GetPartitions",
				"glue:GetPartitionIndexes",
				"glue:BatchCreatePartition",
				"glue:CreateJob",
				"glue:UpdateJob",
				"glue:DeleteJob"
			],
			"Effect": "Allow",
			"Resource": "*"
		},
		{
			"Action": [
				"redshift-data:ExecuteStatement",
				"redshift-data:BatchExecuteStatement",
				"redshift-data:GetStatementResult",
				"redshift-data:DescribeStatement",
				"redshift-data:ListStatements",
				"redshift-data:CancelStatement",
				"redshift-serverless:GetCredentials",
				"redshift-serverless:ListWorkgroups",
				"redshift-serverless:GetWorkgroup"
			],
			"Effect": "Allow",
			"Resource": "*"
		}
	]
})
}

