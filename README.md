# Scalable Unstructured Data Pipeline & Vector Embedding Engine

> Production-ready distributed image processing pipeline using AWS EMR, PySpark, ResNet-50, Pinecone, and MLflow — built for scale, cost efficiency, and observability.

---

## Architecture Overview

```
                          ┌──────────────────────────────────────────────────────┐
                          │                    AWS Cloud                         │
                          │                                                      │
    ┌────────────┐        │   ┌─────────────┐     ┌──────────────────────────┐  │
    │  Client /  │──────▶ │   │  S3 Bucket  │────▶│   Lambda Trigger         │  │
    │  Upstream  │  PUT   │   │  (raw imgs) │     │  (event_parser.py +      │  │
    └────────────┘        │   └─────────────┘     │   step_trigger.py)       │  │
                          │                       └────────────┬─────────────┘  │
                          │                                    │ StartExecution  │
                          │                       ┌────────────▼─────────────┐  │
                          │                       │  Step Functions           │  │
                          │                       │  (pipeline_orchestrator)  │  │
                          │                       └────────────┬─────────────┘  │
                          │                                    │ addStep.sync    │
                          │   ┌─────────────────────────────┐ │                 │
                          │   │         EMR Cluster          │◀┘                 │
                          │   │  ┌──────────┐ ┌──────────┐  │                   │
                          │   │  │  Master  │ │  Core ×2 │  │  (Spot Instances) │
                          │   │  │(on-demand│ │  Worker  │  │                   │
                          │   │  └──────────┘ └────┬─────┘  │                   │
                          │   └────────────────────┼────────┘                   │
                          │                        │ PySpark: mapPartitions      │
                          │                        │ • validate_image            │
                          │                        │ • ResNet-50 inference       │
                          │                        │ • 2048-dim vectors          │
                          │                        │                             │
                          │              ┌─────────▼──────────┐                 │
                          │              │  S3 Dead Letter     │                 │
                          │              │  (corrupt/skipped)  │                 │
                          │              └────────────────────┘                  │
                          └──────────────────────────────────────────────────────┘
                                                   │ batch upsert (100/req)
                          ┌────────────────────────▼──────────────────────────┐
                          │               Pinecone (2048-dim, cosine)          │
                          │               Semantic similarity search            │
                          └────────────────────────────────────────────────────┘
                                                   │ log metrics
                          ┌────────────────────────▼──────────────────────────┐
                          │               MLflow (S3 artifact backend)          │
                          │  • model name/version • image counts • timing      │
                          └────────────────────────────────────────────────────┘
```

---

## Project Structure

```
pipeline/
├── terraform/                  # AWS Infrastructure as Code
│   ├── main.tf                 # Provider + random suffix
│   ├── variables.tf            # All parameterized inputs
│   ├── outputs.tf              # Resource identifiers after deploy
│   ├── vpc.tf                  # VPC, subnets, NAT, route tables
│   ├── s3.tf                   # Raw bucket + DLQ + event notifications
│   ├── iam.tf                  # IAM roles for EMR, Lambda, Step Functions
│   ├── emr.tf                  # EMR cluster (1 master + 2 Spot workers)
│   ├── lambda.tf               # Lambda function and zip packaging
│   └── step_functions.tf       # Orchestration state machine
│
├── lambda/                     # Lambda trigger function
│   ├── lambda_function.py      # Main handler entry point
│   ├── event_parser.py         # S3 event parsing and image key filtering
│   └── step_trigger.py         # Step Functions/EMR job submission
│
├── spark_jobs/                 # PySpark distributed processing
│   ├── extract_embeddings.py   # Main driver (salting, mapPartitions, MLflow)
│   ├── image_validator.py      # Zero-byte + header validation → DLQ routing
│   ├── resnet_extractor.py     # ResNet-50 (head-stripped), 2048-dim inference
│   ├── dead_letter.py          # DLQ JSON manifest writer to S3
│   └── pinecone_client.py      # Batch upsert client with exponential retry
│
├── docker/
│   └── Dockerfile              # EMR-compatible Python 3.9 + CPU PyTorch image
│
├── mlflow/
│   ├── tracking.py             # MLflow context manager + param/metric loggers
│   ├── mlflow_server.py        # Local dev server with S3 artifact root
│   └── pinecone_setup.py       # Index init, full upsert + similarity query CLI
│
├── test_pipeline.py            # End-to-end integration test runner
├── requirements.txt            # Pinned Python dependencies
└── README.md                   # This file
```

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.9+ | Must match EMR 6.10 |
| Terraform | ≥ 1.5 | `brew install terraform` or [download](https://developer.hashicorp.com/terraform/downloads) |
| AWS CLI | ≥ 2.x | `aws configure` with your credentials |
| AWS Account | — | IAM user with admin or scoped permissions |
| Pinecone Account | — | Free tier works for testing |
| Docker | ≥ 24.x | For building the EMR worker image |

---

## Step-by-Step Setup

### 1. Clone and install local dependencies

```bash
git clone <your-repo-url>
cd pipeline
pip install -r requirements.txt
```

### 2. Configure AWS credentials

Never hardcode credentials. Use one of:

```bash
# Option A: AWS CLI profile (recommended for local dev)
aws configure

# Option B: Environment variables
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1

# Option C: IAM role (automatic on EC2/EMR — no config needed)
```

### 3. Set required environment variables

```bash
# Pinecone
export PINECONE_API_KEY="your-pinecone-api-key"
export PINECONE_INDEX_NAME="image-embeddings"
export PINECONE_CLOUD="aws"
export PINECONE_REGION="us-east-1"

# MLflow
export MLFLOW_S3_BUCKET="your-existing-s3-bucket-for-mlflow"
export MLFLOW_TRACKING_URI="http://localhost:5000"

# Pipeline runtime (set after terraform apply — see Step 5)
export DEAD_LETTER_BUCKET="..."
export RAW_BUCKET_NAME="..."
export STEP_FUNCTIONS_ARN="..."
```

### 4. Initialize and deploy Terraform

```bash
cd terraform

# Download providers
terraform init

# Preview what will be created (safe — no changes yet)
terraform plan -var="project_name=my-pipeline"

# Deploy all AWS infrastructure
terraform apply -var="project_name=my-pipeline" -auto-approve
```

After apply completes, capture the outputs:

```bash
terraform output raw_bucket_name          # → set RAW_BUCKET_NAME
terraform output dead_letter_bucket_name  # → set DEAD_LETTER_BUCKET
terraform output step_functions_arn       # → set STEP_FUNCTIONS_ARN
terraform output emr_cluster_id           # → set EMR_CLUSTER_ID
```

### 5. Upload the PySpark script to S3

The Lambda trigger passes the Spark script path to EMR. Upload it once after deploy:

```bash
RAW_BUCKET=$(terraform -chdir=terraform output -raw raw_bucket_name)
aws s3 cp spark_jobs/extract_embeddings.py s3://${RAW_BUCKET}-scripts/spark_jobs/
```

Then set:

```bash
export SPARK_SCRIPT_S3_PATH="s3://${RAW_BUCKET}-scripts/spark_jobs/extract_embeddings.py"
```

### 6. Initialize the Pinecone index

```bash
python mlflow/pinecone_setup.py --init
```

Output:
```
Creating Pinecone index 'image-embeddings' (dim=2048, metric=cosine, cloud=aws, region=us-east-1)...
Index 'image-embeddings' is ready.
```

### 7. Build and push the Docker image to ECR (optional but recommended for production)

```bash
# Authenticate Docker to ECR
aws ecr get-login-password --region us-east-1 | \
    docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com

# Build
docker build -t pipeline-emr-worker ./docker

# Tag and push
docker tag pipeline-emr-worker:latest <account>.dkr.ecr.us-east-1.amazonaws.com/pipeline-emr-worker:latest
docker push <account>.dkr.ecr.us-east-1.amazonaws.com/pipeline-emr-worker:latest
```

---

## Triggering the Pipeline

### Option A: Automatic (production path)
Upload any `.jpg`, `.jpeg`, `.png`, `.bmp`, `.tiff`, or `.webp` files to the raw S3 bucket. The S3 Event Notification fires Lambda automatically.

```bash
aws s3 cp my_images/ s3://${RAW_BUCKET_NAME}/batch-001/ --recursive
```

### Option B: Manual test run
```bash
python test_pipeline.py
```

This script:
- Generates 10 synthetic test images (9 valid, 1 corrupt)
- Uploads them to the raw bucket
- Waits for Step Functions execution to complete
- Verifies Pinecone indexing
- Prints top-5 semantic similarity results
- Confirms the corrupt file was routed to the dead letter bucket

---

## Querying the Vector Database

```bash
# Find the 5 most visually similar images to a query image
python mlflow/pinecone_setup.py --query path/to/query_image.jpg --top_k 5
```

Example output:
```
Top 5 similar images for: path/to/query_image.jpg

  1. Score=0.9821 | ID=raw-bucket_batch-001_dog1-jpg
     Source: s3://raw-bucket/batch-001/dog1.jpg

  2. Score=0.9614 | ID=raw-bucket_batch-001_dog2-jpg
     Source: s3://raw-bucket/batch-001/dog2.jpg
  ...
```

---

## Using MLflow to Trace a Pipeline Run

### Start the MLflow UI

```bash
python mlflow/mlflow_server.py
# → Open http://localhost:5000 in your browser
```

### What's logged per run

| Category | Parameters / Metrics |
|---|---|
| **Model** | `model_name=ResNet-50`, `model_version=pretrained-imagenet` |
| **Preprocessing** | `resize_dim=224x224`, `normalize_mean`, `normalize_std`, `embedding_dim=2048` |
| **Throughput** | `total_images`, `valid_images`, `failed_images`, `failure_rate_pct` |
| **Pinecone** | `pinecone_index`, `upsert_count` |
| **Performance** | `pipeline_duration_secs`, `total_pipeline_duration_seconds` |

### Query a specific run via CLI

```bash
# List all runs
mlflow runs list --experiment-name "unstructured-data-pipeline"

# Compare metrics across runs
mlflow runs search \
  --experiment-name "unstructured-data-pipeline" \
  --filter "metrics.valid_images > 100"
```

---

## Cost Estimates (Spot Instance Pricing)

> Estimates based on `us-east-1` pricing for **m5.xlarge** instances as of 2024. Actual Spot prices vary.

| Component | Config | Est. Cost/Hour |
|---|---|---|
| EMR Master | 1× m5.xlarge On-Demand | ~$0.19 |
| EMR Workers | 2× m5.xlarge Spot (~70% discount) | ~$0.12 (total) |
| Lambda | <1M invocations/month | Free tier |
| Step Functions | <4,000 state transitions | Free tier |
| S3 Storage | Depends on image volume | ~$0.023/GB |
| **Total (per hour)** | | **~$0.31/hr** |

**For a 500-image batch completing in ~20 minutes:**
- EMR cost ≈ $0.31 × (20/60) ≈ **~$0.10 per batch run**
- Spot Instances reduce the bill by **~70-80%** vs On-Demand workers

---

## Tearing Down Infrastructure

```bash
cd terraform
terraform destroy -auto-approve
```

> ⚠️ **This deletes all resources including S3 buckets and their contents.** Ensure Pinecone data is backed up if needed before running.

---

## Security Notes

- **No credentials are hardcoded** anywhere in this codebase.
- All AWS service authentication uses IAM roles (EMR instance profile, Lambda execution role, Step Functions role).
- Pinecone API keys are passed via environment variables only.
- Least-privilege IAM policies are applied — each role can only access what it needs.
