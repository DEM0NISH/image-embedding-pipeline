"""
tracking.py — MLflow experiment tracking setup for the pipeline.

All run artifacts (model configs, metrics, run metadata) are stored in S3
so they persist beyond the lifecycle of the ephemeral EMR cluster.
The MLFLOW_S3_BUCKET env var controls where artifacts are written.

Usage:
    from mlflow.tracking import start_pipeline_run, log_pipeline_metrics

    with start_pipeline_run("embedding_extraction") as run:
        log_pipeline_params(...)
        # ... pipeline runs ...
        log_pipeline_metrics(...)
"""

import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Dict

import mlflow
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Configuration from environment variables — zero hardcoded values
# -------------------------------------------------------------------
MLFLOW_TRACKING_URI = os.environ.get(
    "MLFLOW_TRACKING_URI", "http://localhost:5000"
)
MLFLOW_S3_BUCKET = os.environ.get(
    "MLFLOW_S3_BUCKET", ""
)
MLFLOW_EXPERIMENT_NAME = os.environ.get(
    "MLFLOW_EXPERIMENT_NAME", "unstructured-data-pipeline"
)

# S3 artifact URI — MLflow writes all artifacts here when set
# Format: s3://bucket-name/mlflow-artifacts/
ARTIFACT_LOCATION = (
    f"s3://{MLFLOW_S3_BUCKET}/mlflow-artifacts/" if MLFLOW_S3_BUCKET else None
)


def configure_mlflow() -> None:
    """
    Initialize the MLflow tracking server URI and experiment.

    This must be called once at Spark driver startup before any run is created.
    Creates the experiment if it does not already exist, and sets S3 as the
    artifact backend so logs survive cluster termination.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    logger.info("MLflow tracking URI set to: %s", MLFLOW_TRACKING_URI)

    client = MlflowClient()

    # Check if experiment exists; create it with S3 artifact location if not
    experiment = client.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
    if experiment is None:
        experiment_id = client.create_experiment(
            name=MLFLOW_EXPERIMENT_NAME,
            artifact_location=ARTIFACT_LOCATION,
        )
        logger.info(
            "Created MLflow experiment '%s' (id=%s) with artifact location: %s",
            MLFLOW_EXPERIMENT_NAME,
            experiment_id,
            ARTIFACT_LOCATION,
        )
    else:
        logger.info(
            "Using existing MLflow experiment '%s' (id=%s)",
            MLFLOW_EXPERIMENT_NAME,
            experiment.experiment_id,
        )

    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)


@contextmanager
def start_pipeline_run(run_name: str = None):
    """
    Context manager that wraps an MLflow active run with timing.

    Automatically logs run duration on exit, even if an exception occurs.
    This ensures we always have timing data for debugging slow runs.

    Usage:
        with start_pipeline_run("embedding_extraction") as run:
            mlflow.log_params(...)
            mlflow.log_metrics(...)

    Yields:
        The active MLflow Run object.
    """
    configure_mlflow()

    start = time.time()
    with mlflow.start_run(run_name=run_name) as run:
        logger.info("MLflow run started: %s (id=%s)", run_name, run.info.run_id)
        try:
            yield run
        finally:
            elapsed = time.time() - start
            mlflow.log_metric("total_pipeline_duration_seconds", elapsed)
            logger.info(
                "MLflow run '%s' completed in %.2fs. Run ID: %s",
                run_name,
                elapsed,
                run.info.run_id,
            )


def log_pipeline_params(
    model_name: str,
    model_version: str,
    resize_dim: str,
    normalize_mean: str,
    normalize_std: str,
    embedding_dim: int,
    pinecone_index: str,
    salt_buckets: int,
) -> None:
    """
    Log all static pipeline configuration parameters to the active MLflow run.

    These are parameters — values that don't change during a run.
    Metrics (counts, timings) are logged separately via log_pipeline_metrics.

    Args:
        model_name:      Human-readable model name (e.g., "ResNet-50").
        model_version:   Pretrained weights identifier.
        resize_dim:      Image resize string (e.g., "224x224").
        normalize_mean:  Normalization mean values as string.
        normalize_std:   Normalization std values as string.
        embedding_dim:   Output vector dimension.
        pinecone_index:  Name of the Pinecone index being written to.
        salt_buckets:    Number of salt buckets used for skew prevention.
    """
    mlflow.log_params({
        "model_name":       model_name,
        "model_version":    model_version,
        "resize_dim":       resize_dim,
        "normalize_mean":   normalize_mean,
        "normalize_std":    normalize_std,
        "embedding_dim":    embedding_dim,
        "pinecone_index":   pinecone_index,
        "salt_buckets":     salt_buckets,
    })
    logger.info("Pipeline parameters logged to MLflow.")


def log_pipeline_metrics(
    total_images: int,
    valid_images: int,
    failed_images: int,
    upsert_count: int,
    pipeline_duration: float,
) -> None:
    """
    Log runtime pipeline metrics to the active MLflow run.

    Args:
        total_images:      Total images submitted to the pipeline.
        valid_images:      Images that passed validation and were embedded.
        failed_images:     Images that were skipped or routed to DLQ.
        upsert_count:      Vectors successfully upserted to Pinecone.
        pipeline_duration: Wall-clock time for the Spark job in seconds.
    """
    mlflow.log_metrics({
        "total_images":            total_images,
        "valid_images":            valid_images,
        "failed_images":           failed_images,
        "upsert_count":            upsert_count,
        "pipeline_duration_secs":  pipeline_duration,
        # Derived metric — highlights data quality issues at a glance
        "failure_rate_pct": (
            round((failed_images / total_images) * 100, 2) if total_images > 0 else 0.0
        ),
    })
    logger.info("Pipeline metrics logged to MLflow.")
