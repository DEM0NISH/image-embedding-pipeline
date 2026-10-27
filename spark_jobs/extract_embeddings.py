"""
extract_embeddings.py — PySpark distributed image embedding extraction job.

Architecture overview:
  1. Parse CLI args for the list of S3 image paths.
  2. Parallelize the list into a Spark RDD.
  3. Apply salting to the partition key to prevent data skew (e.g., when
     one folder has 10x more images than others).
  4. Use mapPartitions to process each partition as a batch — this allows
     model loading ONCE per executor rather than once per image row.
  5. For each image:
       a. Validate: skip zero-byte or corrupt header files (routed to DLQ).
       b. Extract: run ResNet-50 forward pass inside the executor process.
       c. Return: (image_path, embedding_vector) pairs.
  6. Collect results and batch-upsert to Pinecone.
  7. Log all run metrics to MLflow.

Run on EMR via:
    spark-submit --deploy-mode cluster --master yarn extract_embeddings.py \
        --image_paths s3://bucket/img1.jpg s3://bucket/img2.jpg ...
"""

import argparse
import logging
import os
import sys
import time
from typing import Iterator, List, Tuple, Optional

from pyspark.sql import SparkSession

# MLflow and Pinecone imports — available because the Docker image pre-installs them
import mlflow

from image_validator import validate_image
from resnet_extractor import load_model, extract_embedding
from pinecone_client import batch_upsert_vectors
from dead_letter import route_to_dead_letter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("extract_embeddings")

# Constants for salting — increasing SALT_BUCKETS reduces skew at the cost
# of slightly more shuffle overhead. 10 is a reasonable default for most workloads.
SALT_BUCKETS = int(os.environ.get("SALT_BUCKETS", 10))

# Pinecone and MLflow configuration via environment variables
PINECONE_API_KEY      = os.environ["PINECONE_API_KEY"]
PINECONE_INDEX_NAME   = os.environ["PINECONE_INDEX_NAME"]
MLFLOW_TRACKING_URI   = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
DEAD_LETTER_BUCKET    = os.environ["DEAD_LETTER_BUCKET"]


# ---------------------------------------------------------------------------
# CLI Argument Parsing
# ---------------------------------------------------------------------------

def parse_args(argv: List[str]) -> argparse.Namespace:
    """
    Parse the Spark job's CLI arguments.
    The Lambda trigger passes image S3 URIs as positional items after --image_paths.
    """
    parser = argparse.ArgumentParser(description="ResNet-50 Embedding Extraction Job")
    parser.add_argument(
        "--image_paths",
        nargs="+",
        required=True,
        help="List of s3:// paths to images to process.",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Salting — Skew Prevention
# ---------------------------------------------------------------------------

def salt_key(image_path: str) -> Tuple[int, str]:
    """
    Apply salting to the image path to distribute load evenly.

    Without salting, if 90% of images come from the same S3 prefix/folder,
    the same Spark partition (and executor) bears the full load.
    Salting distributes these images across SALT_BUCKETS pseudo-random bins.

    Args:
        image_path: S3 URI of the image.

    Returns:
        Tuple of (salt_bucket_int, image_path) — becomes the RDD key.
    """
    # Deterministic hash → consistent across runs, spread across buckets
    bucket = hash(image_path) % SALT_BUCKETS
    return bucket, image_path


# ---------------------------------------------------------------------------
# Per-Partition Processing (mapPartitions)
# ---------------------------------------------------------------------------

def process_partition(
    records: Iterator[Tuple[int, str]]
) -> Iterator[Tuple[str, Optional[List[float]], str]]:
    """
    Process a single Spark partition of image paths.

    Using mapPartitions (vs map) is critical because:
      - We load the ResNet-50 model ONCE per partition (executor thread).
      - A per-row `map` would reload the 90MB+ model for every single image,
        making the job impractically slow.

    Args:
        records: Iterator of (salt_key, image_s3_path) tuples.

    Yields:
        Tuple of (image_path, embedding_or_None, status_string)
        where status is one of: "ok", "zero_byte", "corrupt", "error"
    """
    # Load model once at partition start — weight loading is expensive
    model = load_model()
    logger.info("ResNet-50 loaded for this partition.")

    for _, image_path in records:
        try:
            # --- Validation Gate ---
            validation_status = validate_image(image_path)
            if validation_status != "ok":
                logger.warning("Invalid image [%s]: %s", validation_status, image_path)
                # Route to DLQ synchronously within the executor
                route_to_dead_letter(image_path, validation_status, DEAD_LETTER_BUCKET)
                yield image_path, None, validation_status
                continue

            # --- Feature Extraction ---
            embedding = extract_embedding(model, image_path)
            yield image_path, embedding, "ok"

        except Exception as exc:
            # Swallow per-image errors — log and report rather than crashing the partition
            logger.error("Unhandled error for %s: %s", image_path, exc, exc_info=True)
            route_to_dead_letter(image_path, f"unhandled_error: {exc}", DEAD_LETTER_BUCKET)
            yield image_path, None, "error"


# ---------------------------------------------------------------------------
# Main Driver
# ---------------------------------------------------------------------------

def main():
    args = parse_args(sys.argv[1:])
    image_paths = args.image_paths

    logger.info("Starting embedding extraction for %d image(s).", len(image_paths))

    spark = (
        SparkSession.builder
        .appName("UnstructuredDataPipeline-EmbeddingExtraction")
        .getOrCreate()
    )

    # Set log level to WARN on Spark internals — our app logs at INFO
    spark.sparkContext.setLogLevel("WARN")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    start_time = time.time()

    with mlflow.start_run(run_name="embedding_extraction"):
        _log_mlflow_params()

        # --- Build the salted RDD ---
        # Salting converts a potentially skewed key distribution into a uniform one
        salted_rdd = (
            spark.sparkContext
            .parallelize(image_paths, numSlices=max(len(image_paths) // 10, 1))
            .map(salt_key)          # (salt_bucket, image_path)
            .partitionBy(SALT_BUCKETS)  # Shuffle by salt bucket for even distribution
        )

        # --- Distributed processing ---
        results_rdd = salted_rdd.mapPartitions(process_partition)

        # Collect results back to driver — embeddings are 2048-float lists, manageable
        results = results_rdd.collect()

        # --- Partition results ---
        valid_vectors = [
            (path, vec) for path, vec, status in results if status == "ok" and vec
        ]
        failed_images = [
            (path, status) for path, _, status in results if status != "ok"
        ]

        logger.info(
            "Processing complete. Valid: %d | Failed/Skipped: %d",
            len(valid_vectors),
            len(failed_images),
        )

        # --- Push to Pinecone ---
        upsert_count = 0
        if valid_vectors:
            upsert_count = batch_upsert_vectors(
                valid_vectors,
                index_name=PINECONE_INDEX_NAME,
                api_key=PINECONE_API_KEY,
            )

        # --- Log metrics to MLflow ---
        elapsed = time.time() - start_time
        mlflow.log_metrics({
            "total_images":      len(image_paths),
            "valid_images":      len(valid_vectors),
            "failed_images":     len(failed_images),
            "upsert_count":      upsert_count,
            "pipeline_duration": elapsed,
        })
        mlflow.log_param("pinecone_index", PINECONE_INDEX_NAME)

        logger.info("MLflow metrics logged. Run duration: %.2fs", elapsed)

    spark.stop()
    logger.info("Spark session stopped. Job complete.")


def _log_mlflow_params():
    """Log static pipeline configuration parameters to MLflow."""
    mlflow.log_params({
        "model_name":       "ResNet-50",
        "model_version":    "pretrained-imagenet",
        "resize_dim":       "224x224",
        "normalize_mean":   "[0.485, 0.456, 0.406]",
        "normalize_std":    "[0.229, 0.224, 0.225]",
        "embedding_dim":    2048,
        "salt_buckets":     SALT_BUCKETS,
    })


if __name__ == "__main__":
    main()
