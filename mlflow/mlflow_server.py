"""
mlflow_server.py — Convenience script to start a local MLflow tracking server
backed by S3 for artifact storage.

This script is intended for local development and CI testing. In production,
run the MLflow server on an EC2 instance or ECS task using the same flags.

Usage (local):
    python mlflow/mlflow_server.py

Environment variables required:
    MLFLOW_S3_BUCKET — the S3 bucket to store run artifacts

The tracking store uses the local ./mlruns directory by default.
Set MLFLOW_BACKEND_STORE_URI to a database URI for production persistence.
"""

import os
import subprocess
import sys


def main():
    s3_bucket = os.environ.get("MLFLOW_S3_BUCKET")
    if not s3_bucket:
        print(
            "[WARNING] MLFLOW_S3_BUCKET is not set. "
            "Artifacts will be stored locally in ./mlruns."
        )
        artifact_root = "./mlruns"
    else:
        artifact_root = f"s3://{s3_bucket}/mlflow-artifacts/"

    backend_store = os.environ.get(
        "MLFLOW_BACKEND_STORE_URI", "sqlite:///mlflow.db"
    )
    host = os.environ.get("MLFLOW_HOST", "0.0.0.0")
    port = os.environ.get("MLFLOW_PORT", "5000")

    cmd = [
        sys.executable, "-m", "mlflow", "server",
        "--backend-store-uri", backend_store,
        "--default-artifact-root", artifact_root,
        "--host", host,
        "--port", port,
    ]

    print(f"Starting MLflow server:")
    print(f"  Tracking store : {backend_store}")
    print(f"  Artifact root  : {artifact_root}")
    print(f"  Listening on   : http://{host}:{port}")
    print()

    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
