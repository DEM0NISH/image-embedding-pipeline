"""
resnet_extractor.py — ResNet-50 feature extraction for the Spark executor.

Design decisions:
  - We strip the final FC classification layer (fc) by replacing it with
    nn.Identity(). This leaves the 2048-dimensional avgpool output as the
    effective embedding — the richest semantic feature representation
    before the class-specific softmax head.
  - The model is loaded in eval() mode with torch.no_grad() to disable
    gradient computation, halving memory usage and substantially speeding
    up inference on worker nodes that have no GPU.
  - Images are preprocessed with ImageNet normalization parameters
    (mean/std tuned for the dataset ResNet-50 was trained on).
  - We use BytesIO to process the S3 object bytes in-memory without writing
    to disk, which would require EMR worker node tmp space management.
"""

import io
import logging
from typing import List

import boto3
import torch
import torch.nn as nn
from PIL import Image
from torchvision import models, transforms

logger = logging.getLogger(__name__)

# ImageNet normalization — must match values used during ResNet-50 pre-training
_NORMALIZE = transforms.Normalize(
    mean=[0.485, 0.456, 0.406],
    std=[0.229, 0.224, 0.225],
)

# Standard preprocessing pipeline for ResNet input
_PREPROCESS = transforms.Compose([
    transforms.Resize(256),         # Scale shortest edge to 256 first
    transforms.CenterCrop(224),     # Take center 224×224 crop
    transforms.ToTensor(),          # HWC uint8 → CHW float32 in [0,1]
    _NORMALIZE,                     # Shift to ImageNet distribution
])

_s3_client = boto3.client("s3")


def load_model() -> nn.Module:
    """
    Load a pre-trained ResNet-50 and remove the classification head.

    The final linear layer (model.fc) is replaced with nn.Identity(),
    exposing the 2048-dim feature vector from the average pooling layer.

    Returns:
        The modified ResNet-50 model in eval mode, ready for inference.
    """
    logger.info("Loading pretrained ResNet-50...")
    # weights=IMAGENET1K_V1 silences the deprecation warning from older torchvision API
    model = models.resnet50(weights=models.ResNet50_Weights.IMAGENET1K_V1)

    # Replace the classifier head with a pass-through — no random initialization
    model.fc = nn.Identity()

    model.eval()  # Disable dropout/batchnorm training behavior
    logger.info("ResNet-50 loaded and set to eval mode.")
    return model


def extract_embedding(model: nn.Module, s3_uri: str) -> List[float]:
    """
    Download an image from S3 and extract its 2048-dim ResNet-50 embedding.

    Args:
        model:   Pre-loaded ResNet-50 model with head removed.
        s3_uri:  Full s3://bucket/key path to the image.

    Returns:
        A Python list of 2048 float values representing the image embedding.

    Raises:
        Exception: Propagated to the caller (process_partition) for DLQ routing.
    """
    bucket, key = _parse_s3_uri(s3_uri)

    # Download image bytes from S3 into memory
    response = _s3_client.get_object(Bucket=bucket, Key=key)
    image_bytes = response["Body"].read()

    # Convert to RGB — handles RGBA, grayscale, palette-mode images gracefully
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")

    # Apply preprocessing and add the batch dimension: (C,H,W) → (1,C,H,W)
    tensor = _PREPROCESS(image).unsqueeze(0)

    # Inference with gradient tracking disabled — critical for speed on CPU workers
    with torch.no_grad():
        # Output shape: (1, 2048) because we replaced fc with Identity
        embedding_tensor = model(tensor)

    # Squeeze batch dim → (2048,) and convert to plain Python list for Spark serialization
    return embedding_tensor.squeeze(0).tolist()


def _parse_s3_uri(s3_uri: str):
    """Parse an s3://bucket/key URI into (bucket, key)."""
    without_scheme = s3_uri[len("s3://"):]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key
