resource "aws_redshiftserverless_workgroup" "redshift_workgroup" {
  workgroup_name = "analytics-wg"
  namespace_name = aws_redshiftserverless_namespace.redshift_namespace.namespace_name
 
  base_capacity = 8
  subnet_ids = data.aws_subnets.default.ids

  security_group_ids = [aws_security_group.redshift_serverless_sg.id]
  publicly_accessible = true
}