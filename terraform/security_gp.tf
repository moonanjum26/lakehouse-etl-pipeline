resource "aws_security_group" "redshift_serverless_sg" {
    name = "redshift-serverless-sg"
    vpc_id = data.aws_vpc.default.id

    ingress {
        from_port = 5439
        to_port = 5439
        protocol = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }
    egress{
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
}