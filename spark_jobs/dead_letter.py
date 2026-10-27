"""
dead_letter.py — Routes invalid/failed images to the S3 dead letter bucket.

We write a JSON manifest file per failed image rather than copying the image
itself. This keeps the DLQ lightweight and gives ops teams a structured
log of what failed, when, and why — queryable with Athena if needed.
"""

import json
import logging
import time

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

_s3_client = boto3.client("s3")


def route_to_dead_letter(image_uri: str, reason: str, dead_letter_bucket: str) -> None:
    """
    Write a JSON failure record to the dead letter S3 bucket.

    The DLQ key preserves the original image's S3 path as a subdirectory
    so records can be correlated back to the source image easily.

    Args:
        image_uri:          The original s3:// URI of the failed image.
        reason:             Short status string (e.g. "zero_byte", "corrupt").
        dead_letter_bucket: Name of the target DLQ S3 bucket.
    """
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Build a structured JSON payload for the DLQ record
    payload = {
        "original_uri": image_uri,
        "failure_reason": reason,
        "timestamp": timestamp,
    }

    # Derive the DLQ key from the original image path
    # e.g. s3://raw-bucket/folder/img.jpg → dlq/folder/img.jpg.json
    original_key = image_uri.split("//", 1)[-1].partition("/")[2]
    dlq_key = f"dlq/{original_key}.json"

    try:
        _s3_client.put_object(
            Bucket=dead_letter_bucket,
            Key=dlq_key,
            Body=json.dumps(payload, indent=2),
            ContentType="application/json",
        )
        logger.info("DLQ record written: s3://%s/%s", dead_letter_bucket, dlq_key)
    except ClientError as ce:
        # Log DLQ write failures but don't re-raise — we cannot lose the partition
        logger.error(
            "Failed to write DLQ record for %s: %s", image_uri, ce, exc_info=True
        )
