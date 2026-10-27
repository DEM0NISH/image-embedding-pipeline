"""
pinecone_client.py — Pinecone batch upsert client for the Spark job.

This module is intentionally kept separate from the main Pinecone setup script
(mlflow/pinecone_setup.py) to give the Spark job a minimal, self-contained
client dependency. The full setup script handles index creation and querying.
"""

import logging
import time
from typing import List, Tuple

from pinecone import Pinecone, ServerlessSpec

logger = logging.getLogger(__name__)

# Batch size recommendation from Pinecone: 100 vectors per upsert call
# balances throughput with per-request payload limits.
PINECONE_BATCH_SIZE = 100
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0


def batch_upsert_vectors(
    vectors: List[Tuple[str, List[float]]],
    index_name: str,
    api_key: str,
) -> int:
    """
    Upsert a list of (image_path, embedding) pairs to Pinecone in batches.

    Args:
        vectors:    List of (image_s3_uri, embedding_list) tuples.
        index_name: Name of the target Pinecone index.
        api_key:    Pinecone API key (from environment variable, not hardcoded).

    Returns:
        Total number of vectors successfully upserted.
    """
    pc = Pinecone(api_key=api_key)
    index = pc.Index(index_name)

    total_upserted = 0
    failed_vectors = []

    # Process in fixed-size batches to stay within Pinecone's payload limits
    for batch_start in range(0, len(vectors), PINECONE_BATCH_SIZE):
        batch = vectors[batch_start : batch_start + PINECONE_BATCH_SIZE]

        # Build Pinecone upsert format: list of {"id": ..., "values": ..., "metadata": ...}
        pinecone_records = [
            {
                "id": _sanitize_id(image_path),
                "values": embedding,
                "metadata": {"source_uri": image_path},
            }
            for image_path, embedding in batch
        ]

        success = _upsert_with_retry(index, pinecone_records)
        if success:
            total_upserted += len(batch)
        else:
            logger.error(
                "Batch upsert permanently failed for %d vector(s) starting at index %d.",
                len(batch),
                batch_start,
            )
            failed_vectors.extend(batch)

    if failed_vectors:
        logger.warning(
            "%d vector(s) could not be upserted after %d retries.",
            len(failed_vectors),
            MAX_RETRIES,
        )

    logger.info("Total vectors upserted to Pinecone: %d", total_upserted)
    return total_upserted


def _upsert_with_retry(index, records: list) -> bool:
    """
    Attempt a Pinecone upsert with exponential backoff retries.

    Args:
        index:   Pinecone Index object.
        records: List of Pinecone-formatted vector dicts.

    Returns:
        True if upsert succeeded within MAX_RETRIES, False otherwise.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            index.upsert(vectors=records)
            return True
        except Exception as exc:
            wait = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))  # Exponential backoff
            logger.warning(
                "Upsert attempt %d/%d failed: %s. Retrying in %.1fs...",
                attempt,
                MAX_RETRIES,
                exc,
                wait,
            )
            if attempt < MAX_RETRIES:
                time.sleep(wait)

    return False  # All retries exhausted


def _sanitize_id(image_path: str) -> str:
    """
    Convert an S3 URI to a Pinecone-safe vector ID.

    Pinecone IDs must be non-empty strings. S3 URIs contain '/' and '.'
    which are valid, but we keep them as-is since Pinecone accepts them.
    """
    # Strip s3:// prefix — keeps IDs readable while removing the scheme
    return image_path.replace("s3://", "").replace("/", "_").replace(".", "-")
