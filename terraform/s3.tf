resource "aws_s3_bucket" "raw" {
  bucket        = "${var.raw_bucket_name}-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket" "dead_letter" {
  bucket        = "${var.dead_letter_bucket_name}-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

# Allow S3 to invoke Lambda function
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_pipeline.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw.arn
}

# The S3 Event Notification
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger_pipeline.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}
