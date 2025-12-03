resource "aws_glue_catalog_database" "processed_database" {
  name = "processed_database"
}

# resource "aws_glue_catalog_table" "aws_glue_catalog_table" {
#   name          = "processed_table"
#   database_name = aws_glue_catalog_database.processed_database.name

#   table_type = "EXTERNAL_TABLE"

#   parameters = {
#     EXTERNAL              = "TRUE"
#     "parquet.compression" = "SNAPPY"
#   }

#   storage_descriptor {
#     location      = "s3://${aws_s3_bucket.processed_bucket.bucket}/sales-data/"
#     input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
#     output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

#     ser_de_info {
#       name                  = "my-stream"
#       serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

#       parameters = {
#         "serialization.format" = 1
#       }
#     }
#   }
  
#   depends_on = [ aws_glue_catalog_database.processed_database ]
# }

resource "aws_glue_trigger" "glue_trigger" {
  name     = "glue_trigger"
  schedule = "cron(33 8 * * ? *)"
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.glue_job.name
  }
  start_on_creation = true
}