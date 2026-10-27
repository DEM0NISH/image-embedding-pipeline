"""
lambda_function.py — Entry point for the pipeline trigger Lambda.

Triggered by S3 ObjectCreated events. Parses the event to extract
the bucket name and uploaded image keys, then starts a Step Functions
execution to orchestrate the EMR Spark job.
"""

import json
import logging
import os

from event_parser import parse_s3_event
from step_trigger import start_pipeline_execution

# Configure structured logging — critical for CloudWatch observability
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context) -> dict:
    """
    Main Lambda entry point invoked by S3 Event Notifications.

    Args:
        event:   The raw S3 event payload passed by AWS.
        context: Lambda runtime context (unused but required by signature).

    Returns:
        A dict with statusCode and a summary body.
    """
    logger.info("Lambda triggered. Raw event: %s", json.dumps(event))

    try:
        # --- Step 1: Parse the S3 event ---
        bucket_name, image_keys = parse_s3_event(event)

        if not image_keys:
            logger.warning("No valid image keys found in event. Exiting early.")
            return _response(200, "No images to process.")

        logger.info(
            "Parsed %d image(s) from bucket '%s'.", len(image_keys), bucket_name
        )

        # --- Step 2: Kick off the Step Functions execution ---
        execution_arn = start_pipeline_execution(bucket_name, image_keys)

        logger.info("Step Functions execution started: %s", execution_arn)
        return _response(
            200,
            f"Pipeline triggered for {len(image_keys)} image(s). Execution: {execution_arn}",
        )

    except ValueError as ve:
        # Raised by parse_s3_event when the event structure is unexpected
        logger.error("Event parsing error: %s", ve, exc_info=True)
        return _response(400, f"Bad event format: {ve}")

    except Exception as exc:
        # Catch-all — we log and surface the error without crashing silently
        logger.error("Unexpected error in Lambda handler: %s", exc, exc_info=True)
        return _response(500, f"Internal error: {exc}")


def _response(status_code: int, message: str) -> dict:
    """Utility to build a standard Lambda response."""
    return {"statusCode": status_code, "body": json.dumps({"message": message})}
