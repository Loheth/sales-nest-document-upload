"""Document partitioner: split large PDFs and emit document.unit.requested via outbox."""

from __future__ import annotations

import json
import logging
import os
import shutil
import uuid
from uuid import uuid4

import boto3
import structlog
from sqlalchemy import select

from document_analysis.common.db import ensure_schema, get_async_session
from document_analysis.common.models import ProcessingManifest
from document_analysis.common.outbox import enqueue_outbox, run_outbox_dispatcher
from document_analysis.config.settings import Settings, get_settings
from document_analysis.partitioner.splitter import (
    build_page_ranges,
    pdf_page_count,
    split_pdf_to_files,
)
from document_analysis.services.kafka_consumer import create_consumer
from document_analysis.services.kafka_producer import stop_producer
from document_analysis.services.s3 import download_from_s3
from document_analysis.telemetry.otel_instrumentation import (
    init_opentelemetry,
    shutdown_opentelemetry,
)

logger = logging.getLogger(__name__)


def _safe_ext(s3_key: str) -> str:
    base = s3_key.rsplit("/", 1)[-1]
    if "." not in base:
        return ""
    return base.rsplit(".", 1)[-1].lower()


async def _process_one(
    settings: Settings,
    *,
    event,
) -> None:
    from flash_events.document import DocumentUnitRequestedEvent

    session = await get_async_session(settings.aurora_secret_name)
    try:
        result = await session.execute(
            select(ProcessingManifest).where(
                ProcessingManifest.evidence_id == event.evidence_id,
                ProcessingManifest.modality == "document",
                ProcessingManifest.status != "failed",
            )
        )
        existing = result.scalar_one_or_none()
        if existing is not None:
            structlog.get_logger("partitioner").info(
                "Manifest already exists for evidence=%s status=%s — skipping",
                event.evidence_id,
                existing.status,
            )
            return
    finally:
        await session.close()

    work_dir = os.path.join(settings.temp_dir, f"part_{event.evidence_id}_{uuid.uuid4().hex[:8]}")
    os.makedirs(work_dir, exist_ok=True)

    ext = _safe_ext(event.s3_key)

    try:
        import asyncio as asyncio_mod

        loop = asyncio_mod.get_event_loop()

        tmp_downloaded = await loop.run_in_executor(
            None,
            lambda: download_from_s3(event.s3_bucket, event.s3_key, settings),
        )
        local_original = os.path.join(work_dir, f"original.{ext}" if ext else "original.bin")
        shutil.move(tmp_downloaded, local_original)
        units: list[tuple[str, int, int, str]] = []
        manifest_id = uuid4()
        manifest_s3_key = f"{event.output_key_prefix}.document_manifest.json"
        segments_prefix = f"{event.output_key_prefix}_segments"

        if ext == "pdf":
            try:
                n_pages = pdf_page_count(local_original)
            except Exception as exc:
                logger.warning(
                    "pdf_page_count failed; using single unit: evidence=%s err=%s",
                    event.evidence_id,
                    exc,
                )
                n_pages = -1

            if n_pages <= 0 or n_pages <= settings.doc_partition_threshold_pages:
                units.append((event.s3_key, 0, 0, ext))
            else:
                ranges = build_page_ranges(
                    total_pages=n_pages, pages_per_unit=settings.doc_pages_per_unit
                )
                if len(ranges) > settings.doc_max_units:
                    raise ValueError(
                        f"Too many units {len(ranges)} > doc_max_units={settings.doc_max_units}"
                    )
                seg_paths = split_pdf_to_files(
                    local_original,
                    ranges,
                    os.path.join(work_dir, "segments"),
                    prefix="segment",
                )
                s3_client = boto3.client("s3", region_name=settings.aws_default_region)
                for i, ((p0, p1), seg_path) in enumerate(zip(ranges, seg_paths, strict=True)):
                    seg_key = f"{segments_prefix}/segment_{i:03d}.pdf"
                    await loop.run_in_executor(
                        None,
                        s3_client.upload_file,
                        str(seg_path),
                        event.s3_bucket,
                        seg_key,
                    )
                    units.append((seg_key, p0, p1, ext))
        else:
            units.append((event.s3_key, 0, 0, ext))

        manifest_payload = {
            "manifest_id": str(manifest_id),
            "evidence_id": event.evidence_id,
            "case_id": event.case_id,
            "user_id": event.user_id,
            "original_s3_key": event.s3_key,
            "output_key_prefix": event.output_key_prefix,
            "total_units": len(units),
            "units": [
                {
                    "segment_s3_key": u[0],
                    "page_start": u[1],
                    "page_end": u[2],
                }
                for u in units
            ],
        }
        s3_json = boto3.client("s3", region_name=settings.aws_default_region)
        s3_json.put_object(
            Bucket=event.s3_bucket,
            Key=manifest_s3_key,
            Body=json.dumps(manifest_payload, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

        session = await get_async_session(settings.aurora_secret_name)
        try:
            pm = ProcessingManifest(
                manifest_id=manifest_id,
                evidence_id=event.evidence_id,
                case_id=event.case_id,
                user_id=event.user_id,
                modality="document",
                s3_bucket=event.s3_bucket,
                manifest_s3_key=manifest_s3_key,
                output_key_prefix=event.output_key_prefix,
                total_units=len(units),
                status="pending",
                trace_id=event.trace_id or "",
                job_context_json={"document_type": ext or ""},
            )
            session.add(pm)

            for i, (seg_key, p0, p1, dext) in enumerate(units):
                unit_ev = DocumentUnitRequestedEvent(
                    source="document-partitioner",
                    trace_id=event.trace_id or "",
                    manifest_id=str(manifest_id),
                    evidence_id=event.evidence_id,
                    case_id=event.case_id,
                    user_id=event.user_id,
                    unit_index=i,
                    total_units=len(units),
                    s3_bucket=event.s3_bucket,
                    segment_s3_key=seg_key,
                    manifest_s3_key=manifest_s3_key,
                    output_key_prefix=event.output_key_prefix,
                    page_start=p0,
                    page_end=p1,
                    document_type=dext or ext,
                )
                await enqueue_outbox(
                    session,
                    topic=unit_ev.TOPIC,
                    partition_key=unit_ev.partition_key(),
                    value=unit_ev.to_kafka_value(),
                )

            await session.commit()
        finally:
            await session.close()

        structlog.get_logger("partitioner").info(
            "Partitioned document evidence=%s units=%d manifest=%s",
            event.evidence_id,
            len(units),
            manifest_id,
        )

    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


async def _run(settings: Settings) -> None:
    await ensure_schema(settings.aurora_secret_name)

    settings.kafka_consumer_group = settings.kafka_consumer_group_partitioner
    settings.kafka_inbound_topic = settings.kafka_topic_partition_requested
    settings.kafka_inbound_topics = ""
    consumer = await create_consumer(
        settings,
        topics=[settings.kafka_topic_partition_requested],
    )
    import asyncio

    stop_outbox = asyncio.Event()
    outbox_task = asyncio.create_task(
        run_outbox_dispatcher(
            aurora_secret_name=settings.aurora_secret_name,
            stop_event=stop_outbox,
        )
    )

    structlog.get_logger().info(
        "Document partitioner started",
        topic=settings.kafka_topic_partition_requested,
        group=settings.kafka_consumer_group_partitioner,
    )

    from flash_events.document import DocumentPartitionRequestedEvent

    try:
        async for msg in consumer:
            try:
                event = DocumentPartitionRequestedEvent.from_kafka_value(msg.value)
                await _process_one(settings, event=event)
                await consumer.commit()
            except Exception:
                logger.exception("partitioner message failed")
                await asyncio.sleep(2)
    finally:
        stop_outbox.set()
        outbox_task.cancel()
        with __import__("contextlib").suppress(asyncio.CancelledError):
            await outbox_task
        await consumer.stop()
        await stop_producer()


def run() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    init_opentelemetry()
    import atexit

    atexit.register(shutdown_opentelemetry)
    settings = get_settings()
    import asyncio

    asyncio.run(_run(settings))
