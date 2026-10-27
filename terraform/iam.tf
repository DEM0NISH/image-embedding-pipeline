# EMR Service Role
data "aws_iam_policy_document" "emr_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "emr_service_role" {
  name               = "EMR_DefaultRole_${var.project_name}"
  assume_role_policy = data.aws_iam_policy_document.emr_assume_role.json
}

resource "aws_iam_role_policy_attachment" "emr_service_role_attach" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# EMR EC2 Instance Profile
data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "emr_ec2_role" {
  name               = "EMR_EC2_DefaultRole_${var.project_name}"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

resource "aws_iam_role_policy_attachment" "emr_ec2_role_attach" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# Custom policy for S3 access from EMR
data "aws_iam_policy_document" "emr_s3_custom" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject"
    ]
    resources = [
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/*",
      aws_s3_bucket.dead_letter.arn,
      "${aws_s3_bucket.dead_letter.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "emr_s3_policy" {
  name   = "EMR_S3_Policy_${var.project_name}"
  policy = data.aws_iam_policy_document.emr_s3_custom.json
}

resource "aws_iam_role_policy_attachment" "emr_s3_attach" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = aws_iam_policy.emr_s3_policy.arn
}

resource "aws_iam_instance_profile" "emr_ec2_profile" {
  name = "EMR_EC2_Profile_${var.project_name}"
  role = aws_iam_role.emr_ec2_role.name
}

# Lambda Role
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "lambda_role" {
  name               = "Lambda_Pipeline_Trigger_${var.project_name}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_custom_policy" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/*"
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "elasticmapreduce:AddJobFlowSteps",
      "elasticmapreduce:DescribeStep",
      "elasticmapreduce:DescribeCluster"
    ]
    resources = ["*"]
  }
  statement {
    effect = "Allow"
    actions = [
      "states:StartExecution"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "lambda_policy" {
  name   = "Lambda_Pipeline_Policy_${var.project_name}"
  policy = data.aws_iam_policy_document.lambda_custom_policy.json
}

resource "aws_iam_role_policy_attachment" "lambda_custom_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# Step Functions Role
data "aws_iam_policy_document" "sfn_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "sfn_role" {
  name               = "StepFunctions_Role_${var.project_name}"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_role.json
}

data "aws_iam_policy_document" "sfn_custom_policy" {
  statement {
    effect = "Allow"
    actions = [
      "elasticmapreduce:AddJobFlowSteps",
      "elasticmapreduce:DescribeStep",
      "elasticmapreduce:CancelSteps"
    ]
    resources = ["*"]
  }
  statement {
    effect = "Allow"
    actions = [
      "events:PutTargets",
      "events:PutRule",
      "events:DescribeRule"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "sfn_policy" {
  name   = "StepFunctions_Policy_${var.project_name}"
  policy = data.aws_iam_policy_document.sfn_custom_policy.json
}

resource "aws_iam_role_policy_attachment" "sfn_custom_attach" {
  role       = aws_iam_role.sfn_role.name
  policy_arn = aws_iam_policy.sfn_policy.arn
}
