#!/usr/bin/env python3
"""
End-to-end test: upload a local PDF to S3, publish document.processing.requested to Kafka,
poll for output, then display results and timing.

Defaults (dev): flash-ai-test-upload-bucket. Override with S3_BUCKET.
Requires KAFKA_BOOTSTRAP_SERVERS when publishing from outside the VPC.

Optional: INPUT_FILE (default test.pdf), E2E_TIMEOUT_SECONDS, E2E_POLL_INTERVAL_SECONDS.

Example:
  uv run python scripts/run_e2e_test.py
  KAFKA_BOOTSTRAP_SERVERS=b-1...:9092,... uv run python scripts/run_e2e_test.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

import boto3


def _ensure_flash_events_on_path() -> None:
    root = Path(__file__).resolve().parents[1]
    for base in (root.parent, root.parent.parent):
        cand = base / "flash-event-schemas" / "src"
        if cand.is_dir():
            sys.path.insert(0, str(cand))
            return


# Hardcoded dev defaults (override with env)
DEFAULT_S3_BUCKET = "flash-ai-test-upload-bucket"


async def _publish_document_requested(
    *,
    bucket: str,
    s3_key: str,
    output_key_prefix: str,
    evidence_id: str,
    case_id: str,
    user_id: str,
) -> None:
    _ensure_flash_events_on_path()
    from aiokafka import AIOKafkaProducer
    from flash_events.document import DocumentProcessingRequestedEvent

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    if not bootstrap:
        print(
            "KAFKA_BOOTSTRAP_SERVERS is not set (MSK bootstrap string).",
            file=sys.stderr,
        )
        raise SystemExit(2)

    sec = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip() or "PLAINTEXT"
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        security_protocol=sec,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    topic = os.environ.get("KAFKA_INBOUND_TOPIC", "document.processing.requested").strip()
    await producer.start()
    try:
        event = DocumentProcessingRequestedEvent(
            evidence_id=evidence_id,
            case_id=case_id,
            user_id=user_id,
            s3_bucket=bucket,
            s3_key=s3_key,
            output_key_prefix=output_key_prefix,
            source="run_e2e_test",
            trace_id=str(uuid.uuid4()),
        )
        await producer.send_and_wait(
            topic,
            key=event.partition_key(),
            value=event.to_kafka_value(),
        )
    finally:
        await producer.stop()


def main() -> None:
    bucket = (os.environ.get("S3_BUCKET") or "").strip() or DEFAULT_S3_BUCKET
    input_file = os.environ.get("INPUT_FILE", "test.pdf").strip() or "test.pdf"

    if not os.path.isfile(input_file):
        print(f"Error: INPUT_FILE is not a file: {input_file}", file=sys.stderr)
        sys.exit(1)

    region = os.environ.get("AWS_DEFAULT_REGION", "us-gov-west-1")
    timeout_seconds = int(os.environ.get("E2E_TIMEOUT_SECONDS", "300"))
    poll_interval_seconds = int(os.environ.get("E2E_POLL_INTERVAL_SECONDS", "5"))

    filename = os.path.basename(input_file)
    s3_key = f"test-inputs/{filename}"

    evidence_id = str(uuid.uuid4())
    case_id = os.environ.get("E2E_CASE_ID", "e2e-case").strip() or "e2e-case"
    user_id = os.environ.get("E2E_USER_ID", "e2e-user").strip() or "e2e-user"

    output_prefix = s3_key.rsplit(".", 1)[0] if "." in s3_key else s3_key

    s3 = boto3.client("s3", region_name=region)

    print(f"Input file: {input_file}")
    print(f"S3 bucket: {bucket}")
    print(f"S3 key: {s3_key}")
    print(f"Kafka topic: {os.environ.get('KAFKA_INBOUND_TOPIC', 'document.processing.requested')}")
    print()

    t0 = time.perf_counter()

    # Upload to S3
    print("Uploading to S3...")
    with open(input_file, "rb") as f:
        s3.put_object(Bucket=bucket, Key=s3_key, Body=f.read(), ContentType="application/pdf")
    print(f"  Uploaded s3://{bucket}/{s3_key}")
    upload_done = time.perf_counter() - t0

    print("Publishing document.processing.requested...")
    asyncio.run(
        _publish_document_requested(
            bucket=bucket,
            s3_key=s3_key,
            output_key_prefix=output_prefix,
            evidence_id=evidence_id,
            case_id=case_id,
            user_id=user_id,
        )
    )
    print("  Event published")
    job_sent = time.perf_counter() - t0

    # Poll for output
    json_key = f"{output_prefix}.document.json"
    md_key = f"{output_prefix}.document.md"
    print(
        f"Polling for s3://{bucket}/{json_key} (timeout {timeout_seconds}s, interval {poll_interval_seconds}s)..."
    )
    start = time.monotonic()
    while (time.monotonic() - start) < timeout_seconds:
        try:
            s3.head_object(Bucket=bucket, Key=json_key)
            break
        except s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise
        time.sleep(poll_interval_seconds)
        elapsed = time.monotonic() - start
        print(f"  Waiting... {elapsed:.0f}s")
    else:
        print(f"Timeout: output not found after {timeout_seconds}s", file=sys.stderr)
        sys.exit(1)

    poll_done = time.perf_counter() - t0
    print(f"  Output found after {poll_done - job_sent:.1f}s")

    # Download and display results
    resp = s3.get_object(Bucket=bucket, Key=json_key)
    result_json = json.loads(resp["Body"].read().decode("utf-8"))
    try:
        resp_md = s3.get_object(Bucket=bucket, Key=md_key)
        result_md = resp_md["Body"].read().decode("utf-8")
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            result_md = "(markdown not found)"
        else:
            raise

    total_seconds = time.perf_counter() - t0

    # Print timing
    print()
    print("=" * 60)
    print("TIMING")
    print("=" * 60)
    print(f"  Upload:        {upload_done:.2f}s")
    print(f"  Job sent at:   {job_sent:.2f}s")
    print(f"  Output ready:  {poll_done:.2f}s (waited {poll_done - job_sent:.1f}s)")
    print(f"  Total:         {total_seconds:.2f}s")
    if "stats" in result_json and "processing_time_s" in result_json["stats"]:
        print(f"  Docling (in worker): {result_json['stats']['processing_time_s']}s")
    print()

    # Print result JSON (compact; markdown can be long)
    print("=" * 60)
    print("RESULT (JSON keys)")
    print("=" * 60)
    print(list(result_json.keys()) if isinstance(result_json, dict) else result_json)
    print()
    print("Markdown preview (first 500 chars):")
    print(result_md[:500] + ("..." if len(result_md) > 500 else ""))


if __name__ == "__main__":
    main()
