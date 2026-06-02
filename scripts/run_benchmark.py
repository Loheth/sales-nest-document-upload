#!/usr/bin/env python3
"""
Benchmark: publish document.processing.requested for N sample objects in S3,
poll for completion, record per-file timings, and write a results file.

N is determined by listing objects under the samples prefix in the S3 bucket
(s3://bucket/document-microservice-samples/). Put sample PDFs/docs/xlsx there; the script
lists keys under that prefix (optionally filtered by file type) and runs one job per file.

Requires KAFKA_BOOTSTRAP_SERVERS when publishing from outside the VPC.

Override: S3_BUCKET, AWS_DEFAULT_REGION.
Optional: BENCHMARK_SAMPLES_PREFIX, BENCHMARK_TIMEOUT_SECONDS, BENCHMARK_POLL_INTERVAL_SECONDS.

Example:
  uv run python scripts/run_benchmark.py
  uv run python scripts/run_benchmark.py --file-type pdf
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import boto3
from aiokafka import AIOKafkaProducer


def _ensure_flash_events_on_path() -> None:
    root = Path(__file__).resolve().parents[1]
    for base in (root.parent, root.parent.parent):
        cand = base / "flash-event-schemas" / "src"
        if cand.is_dir():
            sys.path.insert(0, str(cand))
            return


# Dev defaults (override with env)
DEFAULT_S3_BUCKET = "flash-ai-test-upload-bucket"
SAMPLES_PREFIX = "document-microservice-samples/"

# m5.large on-demand us-gov-west-1 (update from AWS GovCloud pricing if needed)
M5_LARGE_US_GOV_WEST_1_HOURLY_USD = 0.115

RESULTS_FILENAME = "benchmark_results.txt"


def key_prefix(key: str) -> str:
    return key.rsplit(".", 1)[0] if "." in key else key


# Input extensions the worker supports for benchmarking (outputs like .document.json are excluded)
SUPPORTED_EXTENSIONS = (".pdf", ".docx", ".xlsx")
SUPPORTED_FILE_TYPES = ("pdf", "docx", "xlsx")


def _extensions_for_file_types(file_types: tuple[str, ...]) -> tuple[str, ...]:
    """Map file type names (e.g. 'pdf') to extensions (e.g. '.pdf')."""
    if not file_types:
        return SUPPORTED_EXTENSIONS
    normalized = [ft.lower().lstrip(".") for ft in file_types]
    out = []
    for ext in SUPPORTED_EXTENSIONS:
        if ext.lstrip(".") in normalized:
            out.append(ext)
    return tuple(out)


def list_sample_keys(
    s3_client, bucket: str, prefix: str, extensions: tuple[str, ...] = SUPPORTED_EXTENSIONS
) -> list[str]:
    """List object keys under prefix that are supported inputs. Excludes outputs and dirs."""
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if key.endswith("/"):
                continue
            if key.lower().endswith(extensions):
                keys.append(key)
    return sorted(keys)


def _max_page_from_document_json(doc: dict) -> int | None:
    """Extract max page number from document_json by scanning prov[].page_no."""
    max_page: int | None = None

    def visit(obj: dict) -> None:
        nonlocal max_page
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "page_no" and isinstance(v, int):
                    if max_page is None or v > max_page:
                        max_page = v
                else:
                    visit(v)
        elif isinstance(obj, list):
            for item in obj:
                visit(item)

    visit(doc)
    return max_page


def get_page_count_from_result(result: dict) -> int | None:
    """Return page count from result JSON (stats.num_pages or max page_no in document_json)."""
    stats = result.get("stats") or {}
    num_pages = stats.get("num_pages")
    if num_pages is not None and isinstance(num_pages, int):
        return num_pages
    doc = result.get("document_json") or {}
    max_page = _max_page_from_document_json(doc)
    return (max_page + 1) if max_page is not None else None


async def _publish_jobs(
    *,
    bucket: str,
    s3_keys: list[str],
) -> None:
    _ensure_flash_events_on_path()
    from flash_events.document import DocumentProcessingRequestedEvent

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap:
        print(
            "KAFKA_BOOTSTRAP_SERVERS is not set (MSK bootstrap string).",
            file=sys.stderr,
        )
        raise SystemExit(2)

    sec = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip() or "PLAINTEXT"
    topic = os.environ.get("KAFKA_INBOUND_TOPIC", "document.processing.requested").strip()
    case_id = os.environ.get("BENCHMARK_CASE_ID", "benchmark-case").strip() or "benchmark-case"
    user_id = os.environ.get("BENCHMARK_USER_ID", "benchmark-user").strip() or "benchmark-user"

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        security_protocol=sec,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()
    try:
        for key in s3_keys:
            evidence_id = str(uuid.uuid4())
            out_prefix = key_prefix(key)
            event = DocumentProcessingRequestedEvent(
                evidence_id=evidence_id,
                case_id=case_id,
                user_id=user_id,
                s3_bucket=bucket,
                s3_key=key,
                output_key_prefix=out_prefix,
                source="run_benchmark",
                trace_id=str(uuid.uuid4()),
            )
            await producer.send_and_wait(
                topic,
                key=event.partition_key(),
                value=event.to_kafka_value(),
            )
    finally:
        await producer.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark document analysis jobs from S3 samples via Kafka.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Supported file types: pdf, docx, xlsx. Default: all.",
    )
    parser.add_argument(
        "--file-type",
        "-t",
        dest="file_types",
        action="append",
        metavar="TYPE",
        choices=SUPPORTED_FILE_TYPES,
        help="Restrict benchmark to this file type (repeat for multiple). Default: all.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    file_types = tuple(args.file_types) if args.file_types else ()
    extensions = _extensions_for_file_types(file_types)

    bucket = (os.environ.get("S3_BUCKET") or "").strip() or DEFAULT_S3_BUCKET
    region = os.environ.get("AWS_DEFAULT_REGION", "us-gov-west-1")
    timeout_seconds = int(os.environ.get("BENCHMARK_TIMEOUT_SECONDS", "600"))
    poll_interval_seconds = int(os.environ.get("BENCHMARK_POLL_INTERVAL_SECONDS", "5"))
    samples_prefix = (os.environ.get("BENCHMARK_SAMPLES_PREFIX") or "").strip() or SAMPLES_PREFIX
    if not samples_prefix.endswith("/"):
        samples_prefix = samples_prefix + "/"

    s3 = boto3.client("s3", region_name=region)

    print(f"S3 bucket: {bucket}")
    print(f"Samples prefix: {samples_prefix}")
    print(
        f"File types: {', '.join(ext.lstrip('.') for ext in extensions) if file_types else 'all (pdf, docx, xlsx)'}"
    )
    print("Listing sample files...")
    s3_keys = list_sample_keys(s3, bucket, samples_prefix, extensions)
    n = len(s3_keys)
    if not s3_keys:
        print(
            f"Error: No objects under s3://{bucket}/{samples_prefix}. Add sample files there.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"  Found {n} file(s): {s3_keys}")
    print()

    # Delete existing result objects so the poller only sees output from this run
    print("Cleaning up existing result objects...")
    for key in s3_keys:
        prefix = key_prefix(key)
        for suffix in (".document.json", ".document.md"):
            out_key = f"{prefix}{suffix}"
            try:
                s3.delete_object(Bucket=bucket, Key=out_key)
            except s3.exceptions.ClientError as e:
                if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                    raise
    print("  Done.")
    print()

    send_time = time.perf_counter()
    asyncio.run(_publish_jobs(bucket=bucket, s3_keys=s3_keys))
    print(f"Sent {n} Kafka job(s) at t=0")

    # Poll for each job's output; record completion time per key
    completed: dict[str, float] = {}
    json_keys = {key: f"{key_prefix(key)}.document.json" for key in s3_keys}
    start = time.monotonic()
    while (time.monotonic() - start) < timeout_seconds:
        for key in s3_keys:
            if key in completed:
                continue
            try:
                s3.head_object(Bucket=bucket, Key=json_keys[key])
                completed[key] = time.perf_counter() - send_time
            except s3.exceptions.ClientError as e:
                if e.response["Error"]["Code"] != "404":
                    raise
        if len(completed) == len(s3_keys):
            break
        elapsed = time.monotonic() - start
        print(f"  Waiting... {elapsed:.0f}s ({len(completed)}/{n} done)")
        time.sleep(poll_interval_seconds)
    else:
        print(
            f"Timeout: only {len(completed)}/{n} completed after {timeout_seconds}s",
            file=sys.stderr,
        )
        sys.exit(1)

    total_wall_clock = time.perf_counter() - send_time
    durations = [completed[key] for key in s3_keys]
    average_seconds = sum(durations) / len(durations)
    cost_hours = total_wall_clock / 3600.0
    estimated_cost_usd = cost_hours * M5_LARGE_US_GOV_WEST_1_HOURLY_USD

    # Fetch result JSONs and extract page counts
    pages_per_key: dict[str, int] = {}
    for key in s3_keys:
        resp = s3.get_object(Bucket=bucket, Key=json_keys[key])
        result = json.loads(resp["Body"].read().decode())
        num_pages = get_page_count_from_result(result)
        if num_pages is not None:
            pages_per_key[key] = num_pages
    total_pages = sum(pages_per_key.values())
    cost_per_page_usd = (estimated_cost_usd / total_pages) if total_pages else None

    # Write results file to tmp (gitignored)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    tmp_dir = os.path.join(repo_root, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    results_path = os.path.join(tmp_dir, RESULTS_FILENAME)
    with open(results_path, "w") as f:
        f.write("Document Analysis Benchmark Results\n")
        f.write("=" * 60 + "\n")
        f.write(f"Timestamp: {datetime.now(UTC).isoformat()}\n")
        f.write(f"S3 bucket: {bucket}\n")
        f.write("Instance: m5.large, Region: us-gov-west-1\n")
        f.write("\n")
        f.write("Per-file duration (seconds):\n")
        for key in s3_keys:
            f.write(f"  {key}: {completed[key]:.2f}\n")
        if pages_per_key:
            f.write("\n")
            f.write("Per-file pages:\n")
            for key in s3_keys:
                if key in pages_per_key:
                    f.write(f"  {key}: {pages_per_key[key]}\n")
        f.write("\n")
        f.write(f"Average time (seconds): {average_seconds:.2f}\n")
        f.write(f"Total wall-clock (seconds): {total_wall_clock:.2f}\n")
        f.write(f"Estimated cost (USD): ${estimated_cost_usd:.4f}\n")
        if total_pages:
            f.write(f"Total pages: {total_pages}\n")
            if cost_per_page_usd is not None:
                f.write(f"Approximate cost per page (USD): ${cost_per_page_usd:.4f}\n")
    print()
    print(f"Results written to {results_path}")
    print(f"  Average time: {average_seconds:.2f}s")
    print(f"  Total wall-clock: {total_wall_clock:.2f}s")
    print(f"  Estimated cost: ${estimated_cost_usd:.4f}")
    if total_pages and cost_per_page_usd is not None:
        print(f"  Total pages: {total_pages}")
        print(f"  Approximate cost per page: ${cost_per_page_usd:.4f}")


if __name__ == "__main__":
    main()
