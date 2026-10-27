output "vpc_id" {
  value = aws_vpc.main.id
}

output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "dead_letter_bucket_name" {
  value = aws_s3_bucket.dead_letter.bucket
}

output "emr_cluster_id" {
  value = aws_emr_cluster.cluster.id
}

output "lambda_function_name" {
  value = aws_lambda_function.trigger_pipeline.function_name
}

output "step_functions_arn" {
  value = aws_sfn_state_machine.pipeline_orchestrator.arn
}
