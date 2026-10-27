"""
image_validator.py — Validates image files fetched from S3 before processing.

Validation strategy:
  1. Zero-byte check: skip files with Content-Length == 0 in S3 metadata.
     Saves downloading the file body at all.
  2. Header check: download enough bytes to let Pillow verify the header
     without reading the full potentially 10MB+ file.
  3. All invalid files return a string status tag used for DLQ routing.

We avoid raising exceptions here — the caller (process_partition) decides
what to do with the status string, keeping validation logic isolated.
"""

import io
import logging

import boto3
from botocore.exceptions import ClientError
from PIL import Image, UnidentifiedImageError

logger = logging.getLogger(__name__)

# We only need the first 64KB to validate image headers for all common formats
HEADER_READ_BYTES = 65_536

_s3_client = boto3.client("s3")


def validate_image(s3_uri: str) -> str:
    """
    Validate an S3 image object and return a status string.

    Args:
        s3_uri: Full s3://bucket/key URI of the image.

    Returns:
        "ok"        — Image is valid and ready for processing.
        "zero_byte" — Object exists but has zero content length.
        "corrupt"   — Header check failed (not a recognizable image).
        "s3_error"  — Could not access the object (permissions, not found).
    """
    bucket, key = _parse_s3_uri(s3_uri)

    try:
        # --- Step 1: Zero-byte check via HEAD (no data transfer) ---
        head = _s3_client.head_object(Bucket=bucket, Key=key)
        content_length = head.get("ContentLength", 0)

        if content_length == 0:
            logger.warning("Zero-byte file detected: %s", s3_uri)
            return "zero_byte"

        # --- Step 2: Header validation via partial GET ---
        # Using Range header to download only the first 64KB — avoids full transfer
        response = _s3_client.get_object(
            Bucket=bucket,
            Key=key,
            Range=f"bytes=0-{HEADER_READ_BYTES - 1}",
        )
        header_bytes = response["Body"].read()

        # Pillow's verify() reads the header and raises if it cannot decode the format
        Image.open(io.BytesIO(header_bytes)).verify()

        return "ok"

    except UnidentifiedImageError:
        logger.warning("Corrupt image header: %s", s3_uri)
        return "corrupt"

    except ClientError as ce:
        error_code = ce.response["Error"]["Code"]
        logger.error("S3 error for %s [%s]: %s", s3_uri, error_code, ce)
        return "s3_error"

    except Exception as exc:
        # Defensive catch for unexpected errors (e.g. Pillow truncation on partial read)
        logger.error("Unexpected validation error for %s: %s", s3_uri, exc)
        return "corrupt"


def _parse_s3_uri(s3_uri: str):
    """
    Split an s3://bucket/key URI into (bucket, key) components.

    Args:
        s3_uri: Full S3 URI.

    Returns:
        Tuple of (bucket_name, object_key).

    Raises:
        ValueError: If the URI is not a valid s3:// path.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {s3_uri}")
    without_scheme = s3_uri[5:]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Malformed S3 URI: {s3_uri}")
    return bucket, key
