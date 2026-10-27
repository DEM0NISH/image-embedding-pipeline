resource "aws_sfn_state_machine" "pipeline_orchestrator" {
  name     = "${var.project_name}-orchestrator"
  role_arn = aws_iam_role.sfn_role.arn

  definition = <<EOF
{
  "Comment": "Orchestrates the unstructured data pipeline",
  "StartAt": "SubmitSparkJob",
  "States": {
    "SubmitSparkJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:addStep.sync",
      "Parameters": {
        "ClusterId.$": "$.ClusterId",
        "Step": {
          "Name": "ExtractEmbeddings",
          "ActionOnFailure": "CONTINUE",
          "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args.$": "$.Args"
          }
        }
      },
      "End": true
    }
  }
}
EOF
}
