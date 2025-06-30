provider "aws" {
  region = "us-west-2"
}

data "aws_vpc" "default" {
  default = true
}

resource "aws_iam_role" "redshift_s3_access" {
  name = "RedshiftS3AccessRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "redshift.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "attach_s3_policy" {
  role       = aws_iam_role.redshift_s3_access.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_security_group" "redshift_sg" {
  name        = "redshift_sg"
  description = "Allow Redshift access from my IP"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Redshift access"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # replace this
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_redshift_cluster" "sparkify_cluster" {
  cluster_identifier = "sparkify-redshift-cluster"
  node_type          = "ra3.large"
  number_of_nodes    = 1
  database_name      = "dev"
  master_username    = "awsuser"
  master_password    = "Passw1234"
  iam_roles          = [aws_iam_role.redshift_s3_access.arn]
  publicly_accessible = true
  skip_final_snapshot = true

  vpc_security_group_ids = [aws_security_group.redshift_sg.id]
}

output "redshift_endpoint" {
  value = aws_redshift_cluster.sparkify_cluster.endpoint
}

output "redshift_role_arn" {
  value = aws_iam_role.redshift_s3_access.arn
}
