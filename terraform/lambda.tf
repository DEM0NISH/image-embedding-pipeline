data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda"
  output_path = "${path.module}/lambda_function.zip"
}

resource "aws_lambda_function" "trigger_pipeline" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.project_name}-trigger"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  runtime          = "python3.9"
  timeout          = 60

  environment {
    variables = {
      EMR_CLUSTER_ID       = aws_emr_cluster.cluster.id
      STEP_FUNCTIONS_ARN   = aws_sfn_state_machine.pipeline_orchestrator.arn
      SPARK_SCRIPT_S3_PATH = var.spark_script_s3_path
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_custom_attach
  ]
}
