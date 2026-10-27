"""
event_parser.py — Utilities for parsing S3 event notification payloads.

S3 can batch multiple records into a single event (e.g., when notifications
accumulate). This module handles the full list and filters out non-image keys
to avoid wasting EMR resources on irrelevant uploads.
"""

import logging
import urllib.parse
from typing import List, Tuple

logger = logging.getLogger(__name__)

# Only process files with these extensions — keeps the pipeline image-focused
VALID_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}


def parse_s3_event(event: dict) -> Tuple[str, List[str]]:
    """
    Extract the bucket name and a list of valid image keys from an S3 event.

    S3 URL-encodes object keys (e.g., spaces become '+'), so we decode them
    before returning to ensure the Spark job receives clean S3 paths.

    Args:
        event: Raw S3 notification event from AWS.

    Returns:
        Tuple of (bucket_name, list_of_image_s3_keys).

    Raises:
        ValueError: If the event structure is malformed or missing required fields.
    """
    records = event.get("Records")
    if not records:
        raise ValueError("Event contains no 'Records' field.")

    bucket_name = None
    image_keys: List[str] = []

    for record in records:
        try:
            s3_info = record["s3"]
            bucket = s3_info["bucket"]["name"]
            # S3 encodes '+' for spaces; urllib decoding handles all edge cases
            key = urllib.parse.unquote_plus(s3_info["object"]["key"])
        except KeyError as ke:
            # Log and skip malformed individual records rather than failing the whole batch
            logger.warning("Skipping malformed record (missing key: %s): %s", ke, record)
            continue

        # All records in an event should share the same bucket
        if bucket_name is None:
            bucket_name = bucket
        elif bucket_name != bucket:
            logger.warning(
                "Multiple buckets in one event (%s vs %s). Using first bucket.",
                bucket_name,
                bucket,
            )

        # Filter to image extensions only
        if _is_valid_image_key(key):
            image_keys.append(key)
            logger.debug("Accepted key: %s", key)
        else:
            logger.info("Skipping non-image key: %s", key)

    if bucket_name is None:
        raise ValueError("Could not extract a bucket name from the event records.")

    return bucket_name, image_keys


def _is_valid_image_key(key: str) -> bool:
    """
    Check if an S3 object key points to a supported image file.

    Args:
        key: The S3 object key string.

    Returns:
        True if extension matches a supported image type, False otherwise.
    """
    lower_key = key.lower()
    return any(lower_key.endswith(ext) for ext in VALID_IMAGE_EXTENSIONS)
