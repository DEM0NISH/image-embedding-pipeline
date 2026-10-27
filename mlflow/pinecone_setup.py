"""
pinecone_setup.py — Full Pinecone index management: initialization, upsert, and query.

This script is the single-responsibility owner of Pinecone operations that
require the full PineconeClient — index creation, batch upsert with retry,
and the semantic search query function.

The lighter spark_jobs/pinecone_client.py only handles upserts from executors.
This module is designed to run on the driver, locally, or in tests.

Environment variables required:
    PINECONE_API_KEY     — Your Pinecone API key (never hardcoded)
    PINECONE_INDEX_NAME  — Name of the vector index to create/use
    PINECONE_CLOUD       — Cloud provider: "aws" (default) | "gcp" | "azure"
    PINECONE_REGION      — Deployment region, e.g. "us-east-1"

Usage:
    python mlflow/pinecone_setup.py --init       # Create the index
    python mlflow/pinecone_setup.py --query path/to/image.jpg
"""

import argparse
import io
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3
import torch
from PIL import Image
from torchvision import models, transforms
from pinecone import Pinecone, ServerlessSpec

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pinecone_setup")

# -------------------------------------------------------------------
# Configuration from environment — never hardcode credentials
# -------------------------------------------------------------------
PINECONE_API_KEY    = os.environ["PINECONE_API_KEY"]
PINECONE_INDEX_NAME = os.environ.get("PINECONE_INDEX_NAME", "image-embeddings")
PINECONE_CLOUD      = os.environ.get("PINECONE_CLOUD", "aws")
PINECONE_REGION     = os.environ.get("PINECONE_REGION", "us-east-1")

EMBEDDING_DIM  = 2048          # ResNet-50 avgpool output dimension
METRIC         = "cosine"      # Cosine similarity → normalized feature comparison
BATCH_SIZE     = 100           # Maximum vectors per upsert call (Pinecone limit)
MAX_RETRIES    = 3
RETRY_BACKOFF  = 2.0           # Seconds; doubles on each retry (exponential backoff)
TOP_K_RESULTS  = 5             # Default number of nearest-neighbor results


# -------------------------------------------------------------------
# Index Initialization
# -------------------------------------------------------------------

def initialize_index(pc: Pinecone) -> None:
    """
    Create the Pinecone index if it does not already exist.

    Dimensions: 2048 — matching ResNet-50's avgpool output.
    Metric: cosine — appropriate for normalized feature vectors because it
            measures angular distance, which is invariant to vector magnitude.
            L2 (Euclidean) distance would be distorted by magnitude differences
            across images with very different brightness or contrast.

    Args:
        pc: An authenticated Pinecone client instance.
    """
    existing = [idx.name for idx in pc.list_indexes()]

    if PINECONE_INDEX_NAME in existing:
        logger.info("Index '%s' already exists. Skipping creation.", PINECONE_INDEX_NAME)
        return

    logger.info(
        "Creating Pinecone index '%s' (dim=%d, metric=%s, cloud=%s, region=%s)...",
        PINECONE_INDEX_NAME, EMBEDDING_DIM, METRIC, PINECONE_CLOUD, PINECONE_REGION,
    )

    pc.create_index(
        name=PINECONE_INDEX_NAME,
        dimension=EMBEDDING_DIM,
        metric=METRIC,
        spec=ServerlessSpec(cloud=PINECONE_CLOUD, region=PINECONE_REGION),
    )

    # Wait for index to be ready before returning — creation is async
    _wait_for_index_ready(pc)
    logger.info("Index '%s' is ready.", PINECONE_INDEX_NAME)


def _wait_for_index_ready(pc: Pinecone, timeout_seconds: int = 120) -> None:
    """
    Poll until the Pinecone index reports 'Ready' state.

    Args:
        pc:               Pinecone client.
        timeout_seconds:  Max wait time before raising a TimeoutError.

    Raises:
        TimeoutError: If index is not ready within the timeout window.
    """
    start = time.time()
    while True:
        status = pc.describe_index(PINECONE_INDEX_NAME).status
        if status.get("ready"):
            return
        elapsed = time.time() - start
        if elapsed > timeout_seconds:
            raise TimeoutError(
                f"Pinecone index '{PINECONE_INDEX_NAME}' not ready after {timeout_seconds}s."
            )
        logger.debug("Index not ready yet (elapsed=%.1fs). Waiting 5s...", elapsed)
        time.sleep(5)


# -------------------------------------------------------------------
# Batch Upsert with Retry
# -------------------------------------------------------------------

def upsert_vectors(
    vectors: List[Tuple[str, List[float], Dict[str, Any]]],
    pc: Pinecone,
    dead_letter_log: Optional[List[str]] = None,
) -> int:
    """
    Upsert vectors into Pinecone in batches of BATCH_SIZE with retry logic.

    On permanent failure (all retries exhausted), the failed vector IDs are
    appended to dead_letter_log for downstream DLQ processing.

    Args:
        vectors:         List of (vector_id, embedding_list, metadata_dict) tuples.
        pc:              Authenticated Pinecone client.
        dead_letter_log: Optional mutable list to collect permanently-failed IDs.

    Returns:
        Count of vectors successfully upserted.
    """
    index = pc.Index(PINECONE_INDEX_NAME)
    total_upserted = 0

    for batch_start in range(0, len(vectors), BATCH_SIZE):
        batch = vectors[batch_start : batch_start + BATCH_SIZE]
        records = [
            {"id": vid, "values": emb, "metadata": meta}
            for vid, emb, meta in batch
        ]

        if _upsert_with_retry(index, records):
            total_upserted += len(batch)
        else:
            failed_ids = [vid for vid, _, _ in batch]
            logger.error(
                "Permanently failed to upsert %d vector(s): %s",
                len(failed_ids),
                failed_ids[:5],  # Log only first 5 IDs to keep logs readable
            )
            if dead_letter_log is not None:
                dead_letter_log.extend(failed_ids)

    logger.info("Upserted %d / %d vectors to Pinecone.", total_upserted, len(vectors))
    return total_upserted


def _upsert_with_retry(index, records: list) -> bool:
    """Retry an upsert call with exponential backoff. Returns True on success."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            index.upsert(vectors=records)
            return True
        except Exception as exc:
            wait = RETRY_BACKOFF * (2 ** (attempt - 1))
            logger.warning(
                "Upsert attempt %d/%d failed: %s. Retrying in %.1fs...",
                attempt, MAX_RETRIES, exc, wait,
            )
            if attempt < MAX_RETRIES:
                time.sleep(wait)
    return False


# -------------------------------------------------------------------
# Query: Semantic Image Search
# -------------------------------------------------------------------

def query_similar_images(
    image_path: str,
    pc: Pinecone,
    top_k: int = TOP_K_RESULTS,
) -> List[Dict[str, Any]]:
    """
    Find the top-k most similar indexed images for a given query image.

    Flow:
        1. Load the query image from a local path or S3 URI.
        2. Extract its 2048-dim ResNet-50 embedding.
        3. Query Pinecone for the nearest neighbors using cosine similarity.
        4. Return results with scores and source metadata.

    Args:
        image_path: Path to the query image — local path or s3:// URI.
        pc:         Authenticated Pinecone client.
        top_k:      Number of results to return.

    Returns:
        List of dicts, each containing 'id', 'score', and 'metadata' keys.
    """
    logger.info("Loading query image: %s", image_path)
    embedding = _extract_query_embedding(image_path)

    index = pc.Index(PINECONE_INDEX_NAME)

    logger.info("Querying Pinecone for top %d results...", top_k)
    response = index.query(
        vector=embedding,
        top_k=top_k,
        include_metadata=True,
    )

    results = []
    for match in response.get("matches", []):
        results.append({
            "id":       match["id"],
            "score":    round(match["score"], 4),
            "metadata": match.get("metadata", {}),
        })
        logger.info(
            "  [score=%.4f] %s — %s",
            match["score"],
            match["id"],
            match.get("metadata", {}).get("source_uri", ""),
        )

    return results


def _extract_query_embedding(image_path: str) -> List[float]:
    """
    Extract a 2048-dim ResNet-50 embedding for the query image.

    Handles both local file paths and s3:// URIs transparently.
    """
    _normalize = transforms.Normalize(
        mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
    )
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        _normalize,
    ])

    # Load model — stripped FC head (same as resnet_extractor.py)
    model = models.resnet50(weights=models.ResNet50_Weights.IMAGENET1K_V1)
    model.fc = torch.nn.Identity()
    model.eval()

    # Load image from local path or S3
    if image_path.startswith("s3://"):
        bucket, _, key = image_path[5:].partition("/")
        s3 = boto3.client("s3")
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        image = Image.open(io.BytesIO(body)).convert("RGB")
    else:
        image = Image.open(image_path).convert("RGB")

    tensor = preprocess(image).unsqueeze(0)

    with torch.no_grad():
        embedding = model(tensor).squeeze(0).tolist()

    return embedding


# -------------------------------------------------------------------
# CLI Entry Point
# -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Pinecone index management CLI")
    parser.add_argument("--init",  action="store_true", help="Create the Pinecone index")
    parser.add_argument("--query", metavar="IMAGE_PATH",  help="Run a similarity query")
    parser.add_argument("--top_k", type=int, default=TOP_K_RESULTS)
    args = parser.parse_args()

    pc = Pinecone(api_key=PINECONE_API_KEY)

    if args.init:
        initialize_index(pc)

    if args.query:
        results = query_similar_images(args.query, pc, top_k=args.top_k)
        print(f"\nTop {args.top_k} similar images for: {args.query}\n")
        for i, r in enumerate(results, 1):
            print(f"  {i}. Score={r['score']:.4f} | ID={r['id']}")
            print(f"     Source: {r['metadata'].get('source_uri', 'N/A')}")
        print()


if __name__ == "__main__":
    main()
