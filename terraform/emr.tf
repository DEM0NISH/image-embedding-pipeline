resource "aws_security_group" "emr_master" {
  name        = "emr-master-sg"
  description = "Security group for EMR master node"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-emr-master-sg"
  }
}

resource "aws_security_group" "emr_core" {
  name        = "emr-core-sg"
  description = "Security group for EMR core nodes"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    security_groups = [aws_security_group.emr_master.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-emr-core-sg"
  }
}

resource "aws_emr_cluster" "cluster" {
  name          = "${var.project_name}-emr-cluster"
  release_label = "emr-6.10.0"
  applications  = ["Spark", "Hadoop"]
  service_role  = aws_iam_role.emr_service_role.arn

  ec2_attributes {
    subnet_id                         = aws_subnet.private.id
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_core.id
    instance_profile                  = aws_iam_instance_profile.emr_ec2_profile.arn
  }

  master_instance_group {
    instance_type  = var.emr_master_instance_type
    instance_count = 1
  }

  core_instance_group {
    instance_type  = var.emr_core_instance_type
    instance_count = var.emr_core_instance_count
    bid_price      = "0.20" # Explicit bid price for spot instances to reduce cost
  }

  tags = {
    Name = "${var.project_name}-cluster"
  }

  # Necessary to wait for the IAM roles to be fully created
  depends_on = [
    aws_iam_role_policy_attachment.emr_service_role_attach,
    aws_iam_role_policy_attachment.emr_ec2_role_attach,
    aws_iam_instance_profile.emr_ec2_profile
  ]
}
