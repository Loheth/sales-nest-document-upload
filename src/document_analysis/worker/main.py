"""Kafka consumer for ``document.unit.requested`` — Docling one chunk + outbox completion."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import boto3
import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from document_analysis.common.db import (
    ensure_schema,
    get_async_engine,
    get_async_session,
    sync_database_url_from_async,
)
from document_analysis.common.lease import LeaseRefresher, release_lease_sync
from document_analysis.common.models import ProcessingUnitLease, ProcessingUnitResult
from document_analysis.common.outbox import enqueue_outbox, run_outbox_dispatcher
from document_analysis.config.settings import Settings, get_settings
from document_analysis.services.document_conversion import convert_document
from document_analysis.services.kafka_consumer import create_consumer
from document_analysis.services.kafka_producer import stop_producer
from document_analysis.services.s3 import cleanup_local_file, download_from_s3
from document_analysis.telemetry.otel_instrumentation import (
    init_opentelemetry,
    job_span,
    shutdown_opentelemetry,
)

logger = logging.getLogger(__name__)


async def _process_unit(settings: Settings, event) -> None:
    from flash_events.document import DocumentUnitCompletedEvent, DocumentUnitRequestedEvent

    if not isinstance(event, DocumentUnitRequestedEvent):
        raise TypeError("expected DocumentUnitRequestedEvent")

    mid = UUID(event.manifest_id)
    unit_id = uuid4()
    worker_id = (os.environ.get("HOSTNAME", "document-unit-worker"))[:255]

    session = await get_async_session(settings.aurora_secret_name)
    try:
        try:
            session.add(
                ProcessingUnitResult(
                    id=unit_id,
                    manifest_id=mid,
                    unit_index=event.unit_index,
                    status="processing",
                    metadata_json={"worker_id": worker_id},
                )
            )
            session.add(
                ProcessingUnitLease(
                    unit_id=unit_id,
                    manifest_id=mid,
                    worker_id=worker_id,
                    acquired_at=datetime.now(UTC),
                    expires_at=datetime.now(UTC) + timedelta(seconds=settings.lease_ttl_seconds),
                )
            )
            await session.commit()
        except IntegrityError:
            await session.rollback()
            r = await session.execute(
                select(ProcessingUnitResult).where(
                    ProcessingUnitResult.manifest_id == mid,
                    ProcessingUnitResult.unit_index == event.unit_index,
                )
            )
            existing = r.scalar_one_or_none()
            if existing and existing.status == "completed":
                structlog.get_logger("unit_worker").info(
                    "skip duplicate completed evidence=%s unit=%d",
                    event.evidence_id,
                    event.unit_index,
                )
                return
            if existing and existing.status in ("processing", "failed"):
                structlog.get_logger("unit_worker").info(
                    "skip in-flight duplicate evidence=%s unit=%d status=%s",
                    event.evidence_id,
                    event.unit_index,
                    existing.status,
                )
                return
            raise
    finally:
        await session.close()

    async_engine = get_async_engine(settings.aurora_secret_name)
    sync_url = sync_database_url_from_async(str(async_engine.url))
    refresher = LeaseRefresher(
        sync_url,
        unit_id,
        ttl_seconds=settings.lease_ttl_seconds,
        interval_seconds=settings.lease_refresh_interval_seconds,
    )
    refresher.start()

    t0 = time.perf_counter()
    local_path: str | None = None
    try:
        with job_span(event.segment_s3_key, facility_id=getattr(event, "facility_id", None)):
            loop = asyncio.get_running_loop()
            local_path = await loop.run_in_executor(
                None,
                lambda: download_from_s3(event.s3_bucket, event.segment_s3_key, settings),
            )
            result = await asyncio.to_thread(convert_document, local_path)

        partial_key = (
            f"{event.output_key_prefix}_units/unit_{event.unit_index:03d}.document.partial.json"
        )
        body = json.dumps(result, ensure_ascii=False).encode("utf-8")
        s3 = boto3.client("s3", region_name=settings.aws_default_region)
        await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: s3.put_object(
                Bucket=event.s3_bucket,
                Key=partial_key,
                Body=body,
                ContentType="application/json",
            ),
        )

        elapsed = time.perf_counter() - t0
        page_n = int((result.get("stats") or {}).get("num_pages") or 0)

        session = await get_async_session(settings.aurora_secret_name)
        try:
            ur = await session.get(ProcessingUnitResult, unit_id)
            if ur:
                ur.status = "completed"
                ur.result_s3_key = partial_key
                ur.duration_seconds = elapsed
                ur.metadata_json = {"page_count": page_n}
                ur.completed_at = datetime.now(UTC)
            completed_ev = DocumentUnitCompletedEvent(
                source="document-unit-worker",
                trace_id=event.trace_id,
                manifest_id=event.manifest_id,
                evidence_id=event.evidence_id,
                case_id=event.case_id,
                user_id=event.user_id,
                unit_index=event.unit_index,
                total_units=event.total_units,
                s3_bucket=event.s3_bucket,
                result_s3_key=partial_key,
                page_start=event.page_start,
                page_end=event.page_end,
                page_count=page_n,
                status="completed",
                processing_time_s=round(elapsed, 3),
            )
            await enqueue_outbox(
                session,
                topic=completed_ev.TOPIC,
                partition_key=completed_ev.partition_key(),
                value=completed_ev.to_kafka_value(),
            )
            await session.commit()
        finally:
            await session.close()

    except Exception as exc:
        logger.exception(
            "unit processing failed evidence=%s unit=%d",
            event.evidence_id,
            event.unit_index,
        )
        elapsed = time.perf_counter() - t0
        session = await get_async_session(settings.aurora_secret_name)
        try:
            ur = await session.get(ProcessingUnitResult, unit_id)
            if ur:
                ur.status = "failed"
                ur.error_message = str(exc)[:2000]
                ur.completed_at = datetime.now(UTC)
            failed_ev = DocumentUnitCompletedEvent(
                source="document-unit-worker",
                trace_id=event.trace_id,
                manifest_id=event.manifest_id,
                evidence_id=event.evidence_id,
                case_id=event.case_id,
                user_id=event.user_id,
                unit_index=event.unit_index,
                total_units=event.total_units,
                s3_bucket=event.s3_bucket,
                result_s3_key="",
                page_start=event.page_start,
                page_end=event.page_end,
                page_count=0,
                status="failed",
                error=str(exc)[:1000],
                processing_time_s=round(elapsed, 3),
            )
            await enqueue_outbox(
                session,
                topic=failed_ev.TOPIC,
                partition_key=failed_ev.partition_key(),
                value=failed_ev.to_kafka_value(),
            )
            await session.commit()
        finally:
            await session.close()
        raise

    finally:
        refresher.stop()
        release_lease_sync(sync_url, unit_id)
        if local_path:
            cleanup_local_file(local_path)


async def _run(settings: Settings) -> None:
    if settings.s3_model_bucket and settings.s3_model_bucket.strip():
        from document_analysis.services.s3 import sync_models_from_s3

        await asyncio.to_thread(
            sync_models_from_s3,
            settings.s3_model_bucket,
            settings.s3_model_prefix,
            settings.model_cache_dir,
            settings,
        )

    await ensure_schema(settings.aurora_secret_name)

    settings.kafka_consumer_group = settings.kafka_consumer_group_unit_worker
    settings.kafka_inbound_topics = ""
    settings.kafka_inbound_topic = settings.kafka_topic_unit_requested
    consumer = await create_consumer(
        settings,
        topics=[settings.kafka_topic_unit_requested],
        max_poll_interval_ms=1_200_000,
    )

    stop_outbox = asyncio.Event()
    outbox_task = asyncio.create_task(
        run_outbox_dispatcher(
            aurora_secret_name=settings.aurora_secret_name,
            stop_event=stop_outbox,
        )
    )

    from flash_events.document import DocumentUnitRequestedEvent

    structlog.get_logger().info(
        "Document unit worker started",
        topic=settings.kafka_topic_unit_requested,
    )

    try:
        async for msg in consumer:
            try:
                du_event = DocumentUnitRequestedEvent.from_kafka_value(msg.value)
                await _process_unit(settings, du_event)
                await consumer.commit()
            except Exception:
                logger.exception("unit_worker message error")
                await asyncio.sleep(2)
    finally:
        stop_outbox.set()
        outbox_task.cancel()
        import contextlib

        with contextlib.suppress(asyncio.CancelledError):
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
    asyncio.run(_run(get_settings()))
