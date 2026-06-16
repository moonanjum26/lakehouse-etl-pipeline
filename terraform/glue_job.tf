resource "aws_glue_job" "glue_job" {
  name              = "glue-etl-job"
  description       = "An example Glue ETL job"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "5.0"
  max_retries       = 0
  timeout           = 2880
  number_of_workers = 2
  worker_type       = "G.1X"
  command {
    script_location = "s3://${aws_s3_bucket.code_bucket.id}/glue_script.py"
    name            = "glueetl"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--continuous-log-logGroup"          = "/aws-glue/jobs"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--enable-auto-scaling"              = "true"
    "--source-path"                      = "s3://${aws_s3_bucket.raw_bucket.bucket}/"
    "--destination-path"                 = "s3://${aws_s3_bucket.processed_bucket.bucket}/"
    "--database-name"                    = aws_glue_catalog_database.processed_database.name
    "--table-name"                       = "processed_table"
    "--job-name"                         = "glue-etl-job"
    "--enable-glue-datacatalog"          = "true"
    
}

}