#!/usr/bin/env python3
"""
Local smoke test: download one random sample from the S3 samples prefix,
sync OCR models from S3 into a local cache (if missing), then run document
processing in Docker (local-file mode) with the cache mounted at /app/models.

Requires sample objects under s3://bucket/document-microservice-samples/.
OCR models are cached under tmp/ocr-models (gitignored via tmp/).
Override: S3_BUCKET, AWS_DEFAULT_REGION, S3_MODEL_BUCKET, S3_MODEL_PREFIX.
Optional: SMOKE_TEST_EXT (e.g. .docx) to force a specific file extension.

Example:
  uv run python scripts/run_smoke_test.py
  SMOKE_TEST_EXT=.docx uv run python scripts/run_smoke_test.py
"""

from __future__ import annotations

import os
import random
import subprocess
import sys

import boto3

from document_analysis.services.s3 import sync_models_from_s3

DEFAULT_S3_BUCKET = "flash-ai-test-upload-bucket"
SAMPLES_PREFIX = "document-microservice-samples/"
SMOKE_INPUT_BASENAME = "smoke-input"
OCR_MODEL_CACHE_DIR = "tmp/ocr-models"
DEFAULT_S3_MODEL_BUCKET = "flash-container-artifacts"
DEFAULT_S3_MODEL_PREFIX = "document-analysis-models"
RAPIDOCR_DET = "ch_PP-OCRv4_det_infer.onnx"
RAPIDOCR_REC = "ch_PP-OCRv4_rec_infer.onnx"


def list_sample_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if not key.endswith("/"):
                keys.append(key)
    return sorted(keys)


def _ocr_cache_needs_sync(cache_dir: str) -> bool:
    rapidocr = os.path.join(cache_dir, "rapidocr")
    det = os.path.join(rapidocr, RAPIDOCR_DET)
    rec = os.path.join(rapidocr, RAPIDOCR_REC)
    return not (os.path.isfile(det) and os.path.isfile(rec))


def main() -> None:
    bucket = (os.environ.get("S3_BUCKET") or "").strip() or DEFAULT_S3_BUCKET
    region = os.environ.get("AWS_DEFAULT_REGION", "us-gov-west-1")
    prefix = SAMPLES_PREFIX + "/" if not SAMPLES_PREFIX.endswith("/") else SAMPLES_PREFIX

    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    tmp_dir = os.path.join(repo_root, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    model_bucket = (os.environ.get("S3_MODEL_BUCKET") or "").strip() or DEFAULT_S3_MODEL_BUCKET
    model_prefix = (os.environ.get("S3_MODEL_PREFIX") or "").strip() or DEFAULT_S3_MODEL_PREFIX
    model_cache_dir = os.path.join(repo_root, OCR_MODEL_CACHE_DIR)
    if _ocr_cache_needs_sync(model_cache_dir):
        print(f"Syncing OCR models from s3://{model_bucket}/{model_prefix} to {model_cache_dir}...")
        os.makedirs(model_cache_dir, exist_ok=True)
        sync_models_from_s3(model_bucket, model_prefix, model_cache_dir)
    else:
        print(f"Using cached OCR models at {model_cache_dir}")

    s3 = boto3.client("s3", region_name=region)
    print(f"Listing s3://{bucket}/{prefix}...")
    keys = list_sample_keys(s3, bucket, prefix)
    if not keys:
        print(
            f"Error: No objects under s3://{bucket}/{prefix}. Add sample files there.",
            file=sys.stderr,
        )
        sys.exit(1)

    smoke_ext = (os.environ.get("SMOKE_TEST_EXT") or "").strip().lower()
    if smoke_ext and not smoke_ext.startswith("."):
        smoke_ext = f".{smoke_ext}"
    if smoke_ext:
        candidates = [k for k in keys if k.lower().endswith(smoke_ext)]
        if not candidates:
            print(
                f"Error: No samples with extension {smoke_ext} under s3://{bucket}/{prefix}",
                file=sys.stderr,
            )
            sys.exit(1)
        keys = candidates
    chosen = random.choice(keys)
    ext = os.path.splitext(chosen)[1] or ".bin"
    local_path = os.path.join(tmp_dir, f"{SMOKE_INPUT_BASENAME}{ext}")
    container_path = f"/tmp/document-analysis/{SMOKE_INPUT_BASENAME}{ext}"

    print(f"Downloading s3://{bucket}/{chosen} -> {local_path}")
    s3.download_file(bucket, chosen, local_path)

    print("Building Docker image (if needed)...")
    build = subprocess.run(["docker", "compose", "build"], cwd=repo_root)
    if build.returncode != 0:
        sys.exit(build.returncode)
    model_cache_abs = os.path.abspath(model_cache_dir)
    print("Running document processing in Docker (with OCR models mounted)...")
    result = subprocess.run(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "-v",
            f"{model_cache_abs}:/app/models",
            "-e",
            f"LOCAL_INPUT_FILE={container_path}",
            "app",
        ],
        cwd=repo_root,
        env=os.environ.copy(),
    )
    if result.returncode != 0:
        sys.exit(result.returncode)
    json_path = os.path.join(tmp_dir, f"{SMOKE_INPUT_BASENAME}.document.json")
    md_path = os.path.join(tmp_dir, f"{SMOKE_INPUT_BASENAME}.document.md")
    if not os.path.isfile(json_path) or not os.path.isfile(md_path):
        print(f"Error: Output files missing. Expected {json_path} and {md_path}", file=sys.stderr)
        sys.exit(1)
    print("Smoke test passed. Output files present:")
    print(f"  {json_path}")
    print(f"  {md_path}")


if __name__ == "__main__":
    main()
