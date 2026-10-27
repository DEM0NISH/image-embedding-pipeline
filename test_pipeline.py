"""
test_pipeline.py — End-to-end integration test for the full pipeline.

Test flow:
    1. Generate 10 synthetic test images (9 valid, 1 corrupt) locally.
    2. Upload them to the raw S3 bucket.
    3. Wait for the Lambda S3 trigger to fire and start a Step Functions execution.
    4. Poll the Step Functions execution until it completes.
    5. Verify vectors were indexed in Pinecone.
    6. Run a semantic similarity query and print the top 5 results.
    7. Check the dead letter bucket to confirm the corrupt file was routed correctly.

Prerequisites:
    - Terraform infrastructure is deployed (`terraform apply` completed).
    - Environment variables set (see below).
    - Pinecone index initialized (`python mlflow/pinecone_setup.py --init`).

Environment variables required:
    AWS_REGION             — e.g. "us-east-1"
    RAW_BUCKET_NAME        — From terraform output raw_bucket_name
    DEAD_LETTER_BUCKET     — From terraform output dead_letter_bucket_name
    STEP_FUNCTIONS_ARN     — From terraform output step_functions_arn
    PINECONE_API_KEY       — Your Pinecone API key
    PINECONE_INDEX_NAME    — The Pinecone index name
"""

import io
import logging
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from PIL import Image
from pinecone import Pinecone

# Import the query function from the setup module
sys.path.insert(0, str(Path(__file__).parent / "mlflow"))
from pinecone_setup import query_similar_images

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("e2e_test")

# ---------------------------------------------------------------------------
# Configuration — from environment only
# ---------------------------------------------------------------------------
AWS_REGION         = os.environ.get("AWS_REGION", "us-east-1")
RAW_BUCKET         = os.environ["RAW_BUCKET_NAME"]
DEAD_LETTER_BUCKET = os.environ["DEAD_LETTER_BUCKET"]
SFN_ARN            = os.environ["STEP_FUNCTIONS_ARN"]
PINECONE_API_KEY   = os.environ["PINECONE_API_KEY"]
PINECONE_INDEX     = os.environ.get("PINECONE_INDEX_NAME", "image-embeddings")

TEST_PREFIX        = "e2e-test/"
NUM_VALID_IMAGES   = 9
POLL_INTERVAL_SECS = 15
MAX_POLL_ATTEMPTS  = 40   # 40 × 15s = 10 minutes max wait


# ---------------------------------------------------------------------------
# Step 1: Generate test images
# ---------------------------------------------------------------------------

def generate_test_images(tmp_dir: Path) -> list:
    """
    Create 9 valid JPEG images and 1 corrupt (zero-byte) image on disk.

    Returns:
        List of (local_path, s3_key) tuples for all 10 test files.
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)
    files = []

    for i in range(NUM_VALID_IMAGES):
        filename = f"valid_image_{i:02d}.jpg"
        local_path = tmp_dir / filename
        s3_key = f"{TEST_PREFIX}{filename}"

        # Create a unique 64×64 noise image so embeddings are meaningfully different
        import random
        pixels = bytes([random.randint(0, 255) for _ in range(64 * 64 * 3)])
        img = Image.frombytes("RGB", (64, 64), pixels)
        img.save(str(local_path), format="JPEG")
        files.append((local_path, s3_key))
        logger.debug("Generated valid image: %s", filename)

    # Create one corrupt file (zero bytes) — should trigger DLQ routing
    corrupt_path = tmp_dir / "corrupt_image.jpg"
    corrupt_path.write_bytes(b"")
    files.append((corrupt_path, f"{TEST_PREFIX}corrupt_image.jpg"))
    logger.info("Generated %d test files (%d valid, 1 corrupt).", len(files), NUM_VALID_IMAGES)
    return files


# ---------------------------------------------------------------------------
# Step 2: Upload images to S3
# ---------------------------------------------------------------------------

def upload_images_to_s3(files: list) -> list:
    """
    Upload all test images to the raw S3 bucket.

    Returns:
        List of s3:// URIs for successfully uploaded files.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)
    uploaded_keys = []

    for local_path, s3_key in files:
        try:
            s3.upload_file(str(local_path), RAW_BUCKET, s3_key)
            logger.info("Uploaded: s3://%s/%s", RAW_BUCKET, s3_key)
            uploaded_keys.append(s3_key)
        except ClientError as ce:
            logger.error("Upload failed for %s: %s", local_path, ce)

    logger.info("Uploaded %d/%d files to S3.", len(uploaded_keys), len(files))
    return uploaded_keys


# ---------------------------------------------------------------------------
# Step 3 & 4: Poll Step Functions until execution completes
# ---------------------------------------------------------------------------

def wait_for_execution(sfn_arn: str) -> str:
    """
    Find the most recent Step Functions execution for this ARN and poll
    it until it reaches a terminal state (SUCCEEDED | FAILED | TIMED_OUT | ABORTED).

    We search for the most recent execution because the Lambda trigger starts
    exactly one execution per S3 batch event.

    Args:
        sfn_arn: The Step Functions state machine ARN.

    Returns:
        Final execution status string.

    Raises:
        TimeoutError: If the execution doesn't complete within the poll window.
    """
    sfn = boto3.client("stepfunctions", region_name=AWS_REGION)

    logger.info("Waiting for Lambda to trigger Step Functions execution...")
    time.sleep(10)  # Give Lambda a moment to process the S3 event

    # Find the most recently started execution
    executions = sfn.list_executions(
        stateMachineArn=sfn_arn,
        statusFilter="RUNNING",
    ).get("executions", [])

    if not executions:
        # Execution may have already completed (fast run) — check all statuses
        executions = sfn.list_executions(stateMachineArn=sfn_arn).get("executions", [])

    if not executions:
        raise RuntimeError("No Step Functions executions found. Was Lambda triggered?")

    # Sort by startDate descending — take the latest
    execution = sorted(executions, key=lambda e: e["startDate"], reverse=True)[0]
    exec_arn = execution["executionArn"]
    logger.info("Monitoring execution: %s", exec_arn)

    for attempt in range(MAX_POLL_ATTEMPTS):
        detail = sfn.describe_execution(executionArn=exec_arn)
        status = detail["status"]

        logger.info(
            "[%d/%d] Execution status: %s",
            attempt + 1,
            MAX_POLL_ATTEMPTS,
            status,
        )

        if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
            return status

        time.sleep(POLL_INTERVAL_SECS)

    raise TimeoutError(
        f"Execution did not complete within {MAX_POLL_ATTEMPTS * POLL_INTERVAL_SECS}s."
    )


# ---------------------------------------------------------------------------
# Step 5: Verify Pinecone index
# ---------------------------------------------------------------------------

def verify_pinecone_index(pc: Pinecone, expected_min_count: int) -> None:
    """
    Confirm the Pinecone index has at least expected_min_count vectors.

    We allow a small buffer — Pinecone's stats can lag by a few seconds
    after upsert completes, so we wait and retry.
    """
    index = pc.Index(PINECONE_INDEX)

    for attempt in range(5):
        stats = index.describe_index_stats()
        total = stats.get("total_vector_count", 0)
        logger.info(
            "Pinecone index '%s' — vector count: %d (expected >= %d)",
            PINECONE_INDEX, total, expected_min_count,
        )
        if total >= expected_min_count:
            return
        logger.info("Waiting for Pinecone index stats to update (attempt %d/5)...", attempt + 1)
        time.sleep(5)

    raise AssertionError(
        f"Pinecone index has {total} vectors, expected >= {expected_min_count}."
    )


# ---------------------------------------------------------------------------
# Step 7: Verify dead letter bucket
# ---------------------------------------------------------------------------

def verify_dead_letter_bucket() -> None:
    """
    Confirm a DLQ record exists in the dead letter S3 bucket for the corrupt image.

    The DLQ write is performed by dead_letter.py inside the Spark executor.
    We expect a JSON file at dlq/e2e-test/corrupt_image.jpg.json.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)
    expected_key = f"dlq/{TEST_PREFIX}corrupt_image.jpg.json"

    try:
        response = s3.get_object(Bucket=DEAD_LETTER_BUCKET, Key=expected_key)
        content = response["Body"].read().decode("utf-8")
        logger.info("DLQ record found at s3://%s/%s", DEAD_LETTER_BUCKET, expected_key)
        logger.info("DLQ content: %s", content)
    except ClientError as ce:
        if ce.response["Error"]["Code"] == "NoSuchKey":
            raise AssertionError(
                f"Expected DLQ record not found: s3://{DEAD_LETTER_BUCKET}/{expected_key}"
            )
        raise


# ---------------------------------------------------------------------------
# Main test runner
# ---------------------------------------------------------------------------

def main():
    import tempfile

    pc = Pinecone(api_key=PINECONE_API_KEY)

    logger.info("=" * 60)
    logger.info("PHASE 1: Generating test images")
    logger.info("=" * 60)
    tmp_dir = Path(tempfile.mkdtemp()) / "e2e_test_images"
    files = generate_test_images(tmp_dir)

    logger.info("=" * 60)
    logger.info("PHASE 2: Uploading to S3 — triggering the pipeline")
    logger.info("=" * 60)
    uploaded_keys = upload_images_to_s3(files)

    logger.info("=" * 60)
    logger.info("PHASE 3-4: Monitoring Step Functions execution")
    logger.info("=" * 60)
    final_status = wait_for_execution(SFN_ARN)
    logger.info("Execution final status: %s", final_status)
    assert final_status == "SUCCEEDED", (
        f"Pipeline execution did not succeed. Status: {final_status}"
    )

    logger.info("=" * 60)
    logger.info("PHASE 5: Verifying Pinecone index")
    logger.info("=" * 60)
    verify_pinecone_index(pc, expected_min_count=NUM_VALID_IMAGES)

    logger.info("=" * 60)
    logger.info("PHASE 6: Running semantic similarity query")
    logger.info("=" * 60)
    # Use the first valid image as the query
    query_local_path = str(tmp_dir / "valid_image_00.jpg")
    results = query_similar_images(query_local_path, pc, top_k=5)

    print("\nTop 5 Similarity Results:")
    print("-" * 50)
    for i, r in enumerate(results, 1):
        print(f"  {i}. Score: {r['score']:.4f}  |  ID: {r['id']}")
        print(f"     Source: {r['metadata'].get('source_uri', 'N/A')}")
    print()

    logger.info("=" * 60)
    logger.info("PHASE 7: Verifying dead letter bucket")
    logger.info("=" * 60)
    verify_dead_letter_bucket()

    print("\n✅ All end-to-end tests PASSED.")


if __name__ == "__main__":
    main()
