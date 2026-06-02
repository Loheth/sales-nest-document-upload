#!/usr/bin/env python3
"""
Download Docling's SmolVLM picture-description model and upload to the container-artifacts S3 bucket.

Run once from a machine with internet (e.g. laptop or CI). ECS tasks sync from S3 at startup;
artifacts_path points at the same model cache that contains rapidocr/ and this model, so
picture description (do_picture_description) can run offline in GovCloud.

Uses: docling-tools models download smolvlm -o <dir>
Same bucket/prefix as OCR models (document-analysis-models) so one sync pulls both.

Requires: docling (uv run), boto3. Set S3_BUCKET (default flash-container-artifacts),
S3_PREFIX (default document-analysis-models), AWS_DEFAULT_REGION.

Example:
  uv run python scripts/download_and_upload_picture_description_model.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    bucket = (os.environ.get("S3_BUCKET") or "flash-container-artifacts").strip()
    prefix = (os.environ.get("S3_PREFIX") or "document-analysis-models").strip().rstrip("/")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-gov-west-1")
    out_dir = Path(os.environ.get("OUT_DIR", "./picture-description-model-upload")).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Downloading SmolVLM picture description model via docling-tools...")
    print(f"  Output dir: {out_dir}")
    proc = subprocess.run(
        ["docling-tools", "models", "download", "smolvlm", "-o", str(out_dir), "--force"],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=False,
        text=True,
    )
    if proc.returncode != 0:
        print("docling-tools models download failed.", file=sys.stderr)
        return 1

    # Upload everything under out_dir to s3://bucket/prefix/<relative path>
    import boto3

    s3 = boto3.client("s3", region_name=region)
    uploaded = 0
    for root, _dirs, files in os.walk(out_dir):
        root_path = Path(root)
        for name in files:
            if name.endswith(".incomplete"):
                continue
            local_path = root_path / name
            rel = local_path.relative_to(out_dir)
            key = f"{prefix}/{rel.as_posix()}"
            s3.upload_file(str(local_path), bucket, key)
            uploaded += 1
            if uploaded <= 10 or uploaded % 20 == 0:
                print(f"  s3://{bucket}/{key}")
    if uploaded > 10:
        print(f"  ... and {uploaded - 10} more files")
    print(f"\nUploaded {uploaded} files to s3://{bucket}/{prefix}/")
    print("ECS already uses S3_MODEL_BUCKET/S3_MODEL_PREFIX; sync will include this model.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
