# ---------------------------------------------------------------------------
# Stage 1: Base image compatible with AWS EMR 6.10 Python environment
# ---------------------------------------------------------------------------
# We use python:3.9-slim-bullseye instead of the full image to minimize size.
# EMR 6.10 ships with Python 3.9 — using the same version avoids bytecode
# compatibility issues when the coordinator ships code to Spark executors.
FROM python:3.9-slim-bullseye AS base

# Metadata labels for traceability in ECR
LABEL maintainer="data-engineering-team"
LABEL project="unstructured-data-pipeline"
LABEL description="EMR worker node image for ResNet-50 embedding extraction"

# ---------------------------------------------------------------------------
# System dependencies
# ---------------------------------------------------------------------------
# libGL and libglib2 are required at runtime by OpenCV / Pillow native image
# decoders for certain formats (BMP, TIFF). Installing them here avoids
# ImportError surprises on worker nodes at inference time.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Python dependency installation
# ---------------------------------------------------------------------------
# Copy requirements first (before application code) so Docker can cache this
# layer independently. Rebuilds after code changes won't re-download packages.
WORKDIR /app

COPY requirements.txt .

# Pin PyPI index URL to ensure reproducibility across regions and build contexts.
# torch+cpu: We explicitly install the CPU wheel — EMR core (Spot) nodes
# typically don't have GPU, and the CPU inference path is sufficient for
# batch embedding extraction. GPU wheels are 3-4x larger with no benefit here.
RUN pip install --no-cache-dir --upgrade pip==24.0 && \
    pip install --no-cache-dir \
      torch==2.2.1+cpu \
      torchvision==0.17.1+cpu \
      --index-url https://download.pytorch.org/whl/cpu && \
    pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Application code
# ---------------------------------------------------------------------------
COPY spark_jobs/ /app/spark_jobs/

# ---------------------------------------------------------------------------
# Environment defaults (overridden by Spark/EMR at runtime)
# ---------------------------------------------------------------------------
ENV PYTHONPATH="/app/spark_jobs:${PYTHONPATH}"
ENV PYTHONUNBUFFERED=1

# Prevent Python from writing .pyc files to disk on the ephemeral Spot instance
ENV PYTHONDONTWRITEBYTECODE=1

# ---------------------------------------------------------------------------
# Healthcheck — useful for ECS/local testing; EMR ignores this at runtime
# ---------------------------------------------------------------------------
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import torch, torchvision, pinecone, mlflow, boto3; print('OK')"

# Default command runs the embedding job — overridden by EMR step args at runtime
ENTRYPOINT ["python", "/app/spark_jobs/extract_embeddings.py"]
