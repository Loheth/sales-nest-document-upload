"""Document analysis worker.

Consumes jobs from SQS (payloads produced by the Kafka→SQS bridge), runs Docling,
and emits ``document.processing.completed`` on Kafka.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import os

from document_analysis.config.settings import get_settings
from document_analysis.job import DocumentJob
from document_analysis.services.document_conversion import convert_document
from document_analysis.services.kafka_producer import (
    emit_document_processing_completed,
    stop_producer,
)
from document_analysis.services.s3 import (
    cleanup_local_file,
    download_from_s3,
    fetch_document_result_page_count,
    sync_models_from_s3,
    upload_document_result,
    write_result_to_disk,
)
from document_analysis.services.sqs import (
    ReceivedSqsMessage,
    delete_message,
    extend_visibility,
    receive_messages,
)
from document_analysis.telemetry.otel_instrumentation import (
    init_opentelemetry,
    job_span,
    shutdown_opentelemetry,
)

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    )


async def process_document(job: DocumentJob) -> dict:
    """Download document from S3, convert with Docling, return result dict for upload."""
    settings = get_settings()
    local_path: str | None = None
    try:
        local_path = download_from_s3(job.bucket, job.key, settings)
        # Run CPU-bound Docling in a thread to avoid blocking the event loop
        result = await asyncio.to_thread(convert_document, local_path)
        return result
    finally:
        if local_path:
            cleanup_local_file(local_path)


async def process_document_local(local_path: str) -> dict:
    """Run the pipeline on a local file (no S3). Returns result dict for writing to disk."""
    return await asyncio.to_thread(convert_document, local_path)


async def _run_local_file() -> None:
    """Process LOCAL_INPUT_FILE and write results to disk; then exit."""
    settings = get_settings()
    local_path = os.path.abspath(settings.local_input_file)
    if not os.path.isfile(local_path):
        logger.error("LOCAL_INPUT_FILE is not a file: %s", local_path)
        return
    output_dir = settings.local_output_dir.strip() or None
    if output_dir:
        output_dir = os.path.abspath(output_dir)
        os.makedirs(output_dir, exist_ok=True)
    stem = os.path.splitext(os.path.basename(local_path))[0]
    path_prefix = os.path.join(output_dir or os.path.dirname(local_path), stem)
    logger.info("Local mode: %s -> %s.{document.json,document.md}", local_path, path_prefix)
    result = await process_document_local(local_path)
    write_result_to_disk(path_prefix, result)
    logger.info("Done.")


async def _process_and_emit(job: DocumentJob, settings) -> None:
    """Process a document job and emit the Kafka completion event."""
    with job_span(job.key, facility_id=job.facility_id or None):
        result = await process_document(job)
        bucket = job.output_bucket or job.bucket
        prefix = job.output_key_prefix or job.key.rsplit(".", 1)[0]
        upload_document_result(bucket, prefix, result, settings)

        page_count = (result.get("stats") or {}).get("num_pages") or 0
        doc_ext = job.key.rsplit(".", 1)[-1] if "." in job.key else ""
        await emit_document_processing_completed(
            s3_key=job.key,
            bucket=bucket,
            document_s3_key=f"{prefix}.document.json",
            page_count=page_count,
            document_type=doc_ext,
        )


def _s3_output_exists(settings, bucket: str, key: str) -> bool:
    import boto3

    s3 = boto3.client("s3", region_name=settings.aws_default_region)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


async def _extend_visibility_loop(receipt_handle: str, settings) -> None:
    """Periodically reset visibility while Docling runs (best-effort)."""
    try:
        while True:
            await asyncio.sleep(settings.sqs_visibility_extension_interval_sec)
            await asyncio.to_thread(
                extend_visibility,
                settings,
                receipt_handle,
                settings.sqs_visibility_timeout_seconds,
            )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("Visibility extension failed (message may retry sooner): %s", exc)


async def _process_one_sqs_message(msg: ReceivedSqsMessage, settings) -> None:
    """Decode SQS payload, run Docling, delete on success (SQS DLQ handles poison after N tries)."""
    from flash_events.document import DocumentProcessingRequestedEvent

    receipt = msg.receipt_handle
    body = msg.body
    raw_b64 = body.get("value_b64")
    if raw_b64 is None or raw_b64 == "":
        logger.error("SQS message missing value_b64; deleting")
        await asyncio.to_thread(delete_message, settings, receipt)
        return

    try:
        raw = base64.b64decode(raw_b64)
    except Exception as exc:
        logger.error("Invalid base64 in SQS body; deleting: %s", exc)
        await asyncio.to_thread(delete_message, settings, receipt)
        return

    try:
        event = DocumentProcessingRequestedEvent.from_kafka_value(raw)
    except Exception as exc:
        logger.exception("Cannot parse DocumentProcessingRequestedEvent; deleting: %s", exc)
        await asyncio.to_thread(delete_message, settings, receipt)
        return

    evidence_id = event.evidence_id
    json_key = f"{event.output_key_prefix}.document.json"
    if await asyncio.to_thread(_s3_output_exists, settings, event.s3_bucket, json_key):
        logger.info(
            "Output already exists, skipping conversion: s3://%s/%s evidence=%s — "
            "re-emitting document.processing.completed",
            event.s3_bucket,
            json_key,
            evidence_id,
        )
        page_count = await asyncio.to_thread(
            fetch_document_result_page_count,
            event.s3_bucket,
            json_key,
            settings,
        )
        doc_ext = event.s3_key.rsplit(".", 1)[-1] if "." in event.s3_key else ""
        await emit_document_processing_completed(
            s3_key=event.s3_key,
            bucket=event.s3_bucket,
            document_s3_key=json_key,
            page_count=page_count,
            document_type=doc_ext,
        )
        await asyncio.to_thread(delete_message, settings, receipt)
        logger.info(
            "Completed document processing (existing output): evidence=%s trace_id=%s (SQS deleted)",
            evidence_id,
            event.trace_id,
        )
        return

    job = DocumentJob.from_kafka_event(event)
    extender = asyncio.create_task(_extend_visibility_loop(receipt, settings))
    try:
        await _process_and_emit(job, settings)
    except Exception as exc:
        logger.exception(
            "Document processing failed; leaving message for SQS retry/DLQ: evidence=%s err=%s",
            evidence_id,
            exc,
        )
        return
    finally:
        extender.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await extender

    await asyncio.to_thread(delete_message, settings, receipt)
    logger.info(
        "Completed document processing: evidence=%s trace_id=%s (SQS deleted)",
        evidence_id,
        event.trace_id,
    )


async def _run_sqs_worker() -> None:
    """Poll SQS for jobs enqueued by the Kafka bridge."""
    settings = get_settings()
    if not settings.sqs_queue_url.strip():
        logger.error("SQS_QUEUE_URL is required for worker mode")
        return

    if settings.s3_model_bucket and settings.s3_model_bucket.strip():
        await asyncio.to_thread(
            sync_models_from_s3,
            settings.s3_model_bucket,
            settings.s3_model_prefix,
            settings.model_cache_dir,
            settings,
        )

    logger.info("Consuming from SQS queue URL ending ...%s", settings.sqs_queue_url[-48:])

    try:
        while True:
            messages = await asyncio.to_thread(receive_messages, settings)
            if not messages:
                continue
            for sqs_msg in messages:
                await _process_one_sqs_message(sqs_msg, settings)
    except asyncio.CancelledError:
        pass
    finally:
        await stop_producer()


async def main() -> None:
    _setup_logging()
    init_opentelemetry()

    import atexit

    atexit.register(shutdown_opentelemetry)
    settings = get_settings()

    if settings.local_input_file and settings.local_input_file.strip():
        await _run_local_file()
        return

    if settings.entrypoint_mode == "bridge":
        from document_analysis.bridge_main import run_bridge

        logger.info("Starting Kafka→SQS bridge")
        await run_bridge(settings)
        return

    if settings.entrypoint_mode == "worker":
        logger.info("Starting SQS → Docling worker")
        await _run_sqs_worker()
        return

    logger.error(
        "ENTRYPOINT_MODE must be 'bridge', 'worker', 'partitioner', 'unit_worker', "
        "'aggregator' (got %r); or set LOCAL_INPUT_FILE",
        settings.entrypoint_mode,
    )


def run() -> None:
    settings = get_settings()

    # Local one-shot and bridge/worker run under asyncio.run(main()). Partitioned Kafka
    # entrypoints install their own asyncio.run() — never nest them inside main()'s loop.
    if settings.local_input_file and settings.local_input_file.strip():
        asyncio.run(main())
        return

    mode = settings.entrypoint_mode
    if mode == "partitioner":
        from document_analysis.partitioner.main import run as run_partitioner

        run_partitioner()
        return
    if mode == "unit_worker":
        from document_analysis.worker.main import run as run_unit_worker

        run_unit_worker()
        return
    if mode == "aggregator":
        from document_analysis.aggregator.main import run as run_doc_aggregator

        run_doc_aggregator()
        return

    asyncio.run(main())
