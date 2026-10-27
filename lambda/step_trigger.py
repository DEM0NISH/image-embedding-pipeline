"""
step_trigger.py — Submits the pipeline execution via AWS Step Functions.

Design decision: Rather than calling EMR directly from Lambda, we invoke
Step Functions (which then calls EMR via addStep.sync). This gives us:
  1. Built-in retry/failure state tracking in the console.
  2. A visual audit trail for every pipeline run.
  3. Decoupling of Lambda from the long-running EMR step timing.

All configuration is read from environment variables — no hardcoded values.
"""

import json
import logging
import os
import uuid
from typing import List

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# Environment variables injected by Terraform at deploy time
EMR_CLUSTER_ID = os.environ["EMR_CLUSTER_ID"]            # EMR cluster to submit the step to
STEP_FUNCTIONS_ARN = os.environ["STEP_FUNCTIONS_ARN"]    # The pipeline orchestrator ARN
SPARK_SCRIPT_S3_PATH = os.environ.get(                   # S3 path to the PySpark script
    "SPARK_SCRIPT_S3_PATH",
    "s3://your-scripts-bucket/spark_jobs/extract_embeddings.py",
)

_sfn_client = boto3.client("stepfunctions")


def start_pipeline_execution(bucket_name: str, image_keys: List[str]) -> str:
    """
    Start a Step Functions execution to process a batch of images.

    The execution payload includes the EMR Cluster ID and the full list of
    Spark CLI arguments, so the state machine can pass them directly to EMR.

    Args:
        bucket_name: S3 bucket containing the uploaded images.
        image_keys:  List of S3 object keys for the images to process.

    Returns:
        The ARN of the started Step Functions execution.

    Raises:
        RuntimeError: If Step Functions fails to start the execution.
    """
    # Build the S3 URI list for each image — Spark will iterate these
    image_s3_uris = [f"s3://{bucket_name}/{key}" for key in image_keys]

    # These args are passed directly into the EMR HadoopJarStep Args field
    spark_args = _build_spark_args(image_s3_uris)

    # Unique execution name (Step Functions requires it per-run)
    execution_name = f"pipeline-run-{uuid.uuid4().hex[:8]}"

    payload = {
        "ClusterId": EMR_CLUSTER_ID,
        "Args": spark_args,
    }

    logger.info(
        "Starting Step Functions execution '%s' with %d image(s).",
        execution_name,
        len(image_keys),
    )

    try:
        response = _sfn_client.start_execution(
            stateMachineArn=STEP_FUNCTIONS_ARN,
            name=execution_name,
            input=json.dumps(payload),
        )
        return response["executionArn"]

    except ClientError as ce:
        error_code = ce.response["Error"]["Code"]
        logger.error("AWS ClientError starting Step Functions execution: %s", ce)
        raise RuntimeError(f"Step Functions start failed ({error_code}): {ce}") from ce

    except BotoCoreError as be:
        logger.error("BotoCoreError starting Step Functions execution: %s", be)
        raise RuntimeError(f"Step Functions connectivity error: {be}") from be


def _build_spark_args(image_s3_uris: List[str]) -> List[str]:
    """
    Construct the argument list for the spark-submit EMR step.

    The PySpark script receives '--image_paths' followed by the URIs.
    We use spark-submit to ensure proper cluster-mode setup on EMR.

    Args:
        image_s3_uris: Full s3:// paths for every image in this batch.

    Returns:
        List of string arguments compatible with EMR HadoopJarStep.
    """
    args = [
        "spark-submit",
        "--deploy-mode", "cluster",
        "--master", "yarn",
        SPARK_SCRIPT_S3_PATH,
        "--image_paths",
    ]
    # Append all image URIs as positional items after the flag
    args.extend(image_s3_uris)
    return args
