#!/usr/bin/env python3
"""
Download RapidOCR ONNX models from Hugging Face and upload to the container-artifacts S3 bucket.

Run once from a machine with internet (e.g. laptop or CI). ECS tasks then sync from S3 at startup
and use local models (no modelscope.cn or HF at runtime in GovCloud).

Requires: boto3, requests (or use urllib). Set S3_BUCKET (default flash-container-artifacts),
S3_PREFIX (default document-analysis-models), AWS_DEFAULT_REGION.

Example:
  uv run python scripts/download_and_upload_ocr_models.py
  S3_BUCKET=my-bucket S3_PREFIX=docling-models uv run python scripts/download_and_upload_ocr_models.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Hugging Face SWHL/RapidOCR PP-OCRv4 (ONNX; no modelscope.cn)
HF_BASE = "https://huggingface.co/SWHL/RapidOCR/resolve/main/PP-OCRv4"
MODELS = [
    "ch_PP-OCRv4_det_infer.onnx",
    "ch_PP-OCRv4_rec_infer.onnx",
]
# Optional cls model (from same repo or leave empty to skip)
# Ooredoo-Group/rapidocr-models has ch_ppocr_mobile_v2.0_cls_infer.onnx
CLS_URL = "https://huggingface.co/Ooredoo-Group/rapidocr-models/resolve/main/ch_ppocr_mobile_v2.0_cls_infer.onnx"
CLS_NAME = "ch_ppocr_mobile_v2.0_cls_infer.onnx"


def download_file(url: str, dest: Path) -> None:
    import urllib.request

    dest.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, dest)


def main() -> int:
    bucket = (os.environ.get("S3_BUCKET") or "flash-container-artifacts").strip()
    prefix = (os.environ.get("S3_PREFIX") or "document-analysis-models").strip().rstrip("/")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-gov-west-1")
    out_dir = Path(os.environ.get("OUT_DIR", "./ocr-models-upload")).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    rapidocr_dir = out_dir / "rapidocr"
    rapidocr_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading to {rapidocr_dir} ...")
    for name in MODELS:
        url = f"{HF_BASE}/{name}"
        dest = rapidocr_dir / name
        print(f"  {name} ...")
        download_file(url, dest)
        print(f"    -> {dest} ({dest.stat().st_size / 1024 / 1024:.2f} MB)")

    try:
        dest = rapidocr_dir / CLS_NAME
        print(f"  {CLS_NAME} (optional) ...")
        download_file(CLS_URL, dest)
        print(f"    -> {dest}")
    except Exception as e:
        print(f"    (optional cls skipped: {e})")

    print(f"\nUploading to s3://{bucket}/{prefix}/ ...")
    import boto3

    s3 = boto3.client("s3", region_name=region)
    for f in rapidocr_dir.iterdir():
        if f.is_file():
            key = f"{prefix}/rapidocr/{f.name}"
            s3.upload_file(str(f), bucket, key)
            print(f"  s3://{bucket}/{key}")
    print(f"\nDone. Set in ECS (or env): S3_MODEL_BUCKET={bucket} S3_MODEL_PREFIX={prefix}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
