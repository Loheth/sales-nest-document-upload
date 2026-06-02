"""S3 download and upload for document analysis.

Downloads an object from S3 to a local temp directory for Docling conversion.
Uploads document JSON and Markdown results to S3.
Supports syncing model cache from S3 at startup (OCR/Docling artifacts).
"""

from __future__ import annotations

import json
import logging
import os

import boto3

from document_analysis.config.settings import Settings, get_settings

logger = logging.getLogger(__name__)


def sync_models_from_s3(
    bucket: str,
    prefix: str,
    local_dir: str,
    settings: Settings | None = None,
) -> None:
    """Sync all objects under s3://bucket/prefix to local_dir. Creates local_dir if needed."""
    settings = settings or get_settings()
    os.makedirs(local_dir, exist_ok=True)
    prefix = prefix.rstrip("/")
    if prefix:
        prefix = prefix + "/"

    s3_client = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
    )
    paginator = s3_client.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if key.endswith("/"):
                continue
            rel = key[len(prefix) :] if len(prefix) > 0 else key
            local_path = os.path.join(local_dir, rel)
            local_parent = os.path.dirname(local_path)
            os.makedirs(local_parent, exist_ok=True)
            s3_client.download_file(bucket, key, local_path)
            count += 1
            if count <= 5 or count % 50 == 0:
                logger.info("Downloaded s3://%s/%s -> %s", bucket, key, local_path)
    logger.info(
        "Model sync complete: %d objects from s3://%s/%s to %s", count, bucket, prefix, local_dir
    )


def download_from_s3(
    bucket: str,
    key: str,
    settings: Settings | None = None,
) -> str:
    """Download s3://<bucket>/<key> to the local temp directory.

    Returns the absolute path of the downloaded file.
    """
    settings = settings or get_settings()
    temp_dir = settings.temp_dir
    os.makedirs(temp_dir, exist_ok=True)

    filename = os.path.basename(key)
    local_path = os.path.join(temp_dir, filename)

    logger.info("Downloading s3://%s/%s -> %s", bucket, key, local_path)

    s3_client = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
    )
    s3_client.download_file(bucket, key, local_path)

    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    logger.info("Download complete: %s (%.2f MB)", local_path, file_size_mb)
    return local_path


def fetch_document_result_page_count(
    bucket: str,
    key: str,
    settings: Settings | None = None,
) -> int:
    """Read ``stats.num_pages`` from ``<prefix>.document.json`` in S3.

    Used when Docling output already exists but we still emit a completion event
    (e.g. user retry). Returns ``0`` if the object is missing or JSON is malformed.
    """
    settings = settings or get_settings()
    s3_client = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
    )
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        data = json.loads(raw.decode("utf-8"))
        stats = data.get("stats") or {}
        n = stats.get("num_pages")
        if isinstance(n, bool):
            return 0
        if isinstance(n, (int, float)):
            return int(n)
        return 0
    except Exception as exc:
        logger.warning("Could not read page count from s3://%s/%s: %s", bucket, key, exc)
        return 0


def cleanup_local_file(path: str) -> None:
    """Remove a local file if it exists (best-effort)."""
    try:
        if os.path.exists(path):
            os.remove(path)
            logger.debug("Cleaned up: %s", path)
    except OSError as exc:
        logger.warning("Failed to clean up %s: %s", path, exc)


def upload_document_result(
    bucket: str,
    key_prefix: str,
    result: dict,
    settings: Settings | None = None,
) -> None:
    """Upload document JSON and Markdown to S3.

    Writes <key_prefix>.document.json and <key_prefix>.document.md.
    """
    settings = settings or get_settings()
    s3_client = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
    )
    json_key = f"{key_prefix}.document.json"
    md_key = f"{key_prefix}.document.md"
    s3_client.put_object(
        Bucket=bucket,
        Key=json_key,
        Body=json.dumps(result, indent=2, ensure_ascii=False),
        ContentType="application/json",
    )
    logger.info("Uploaded s3://%s/%s", bucket, json_key)
    markdown = result.get("markdown", "")
    s3_client.put_object(
        Bucket=bucket,
        Key=md_key,
        Body=markdown,
        ContentType="text/markdown",
    )
    logger.info("Uploaded s3://%s/%s", bucket, md_key)


def write_result_to_disk(path_prefix: str, result: dict) -> None:
    """Write document JSON and Markdown to disk.

    path_prefix: full path without extension (e.g. /path/to/doc).
    Writes <path_prefix>.document.json and <path_prefix>.document.md.
    """
    json_path = f"{path_prefix}.document.json"
    md_path = f"{path_prefix}.document.md"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    logger.info("Wrote %s", json_path)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(result.get("markdown", ""))
    logger.info("Wrote %s", md_path)
