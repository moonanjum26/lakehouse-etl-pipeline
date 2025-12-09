resource "aws_redshiftserverless_namespace" "redshift_namespace" {
  namespace_name = "analytics"
  admin_username = "*******"
  admin_user_password = "******"
  db_name = "analyticsdb"
  default_iam_role_arn = aws_iam_role.redshift_serverless_role.arn
  iam_roles = [aws_iam_role.redshift_serverless_role.arn, aws_iam_role.glue_role.arn]

}
