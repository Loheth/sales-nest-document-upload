"""Document aggregator: ``document.unit.completed`` → merge → ``document.processing.completed`` (outbox)."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from document_analysis.aggregator.loader import merge_document_manifest_units
from document_analysis.common.db import ensure_schema, get_async_session
from document_analysis.common.models import ProcessingUnitResult
from document_analysis.common.outbox import enqueue_outbox, run_outbox_dispatcher
from document_analysis.config.settings import Settings, get_settings
from document_analysis.services.kafka_consumer import create_consumer
from document_analysis.services.kafka_producer import stop_producer
from document_analysis.services.s3 import upload_document_result
from document_analysis.telemetry.otel_instrumentation import (
    init_opentelemetry,
    shutdown_opentelemetry,
)

logger = logging.getLogger(__name__)

INSTANCE_ID = str(uuid.uuid4())[:8]

SCHEMA = "documents_partitioned"


async def _run(settings: Settings) -> None:
    await ensure_schema(settings.aurora_secret_name)

    settings.kafka_consumer_group = settings.kafka_consumer_group_aggregator
    settings.kafka_inbound_topics = ""
    settings.kafka_inbound_topic = settings.kafka_topic_unit_completed
    consumer = await create_consumer(
        settings,
        topics=[settings.kafka_topic_unit_completed],
        max_poll_interval_ms=1_200_000,
    )

    stop_outbox = asyncio.Event()
    outbox_task = asyncio.create_task(
        run_outbox_dispatcher(
            aurora_secret_name=settings.aurora_secret_name,
            stop_event=stop_outbox,
        )
    )

    from flash_events.document import DocumentProcessingCompletedEvent, DocumentUnitCompletedEvent

    structlog.get_logger().info(
        "Document aggregator started",
        topic=settings.kafka_topic_unit_completed,
        instance=INSTANCE_ID,
    )

    try:
        async for msg in consumer:
            try:
                event = DocumentUnitCompletedEvent.from_kafka_value(msg.value)

                logger.info(
                    "document unit done evidence=%s unit=%d/%d status=%s",
                    event.evidence_id,
                    event.unit_index,
                    event.total_units,
                    event.status,
                )

                meta = {
                    "page_count": event.page_count,
                    "error": (event.error or "")[:500],
                }
                comp_at = datetime.now(UTC)
                session = await get_async_session(settings.aurora_secret_name)
                try:
                    stmt = (
                        pg_insert(ProcessingUnitResult)
                        .values(
                            manifest_id=UUID(event.manifest_id),
                            unit_index=event.unit_index,
                            status=event.status,
                            result_s3_key=event.result_s3_key or None,
                            duration_seconds=event.processing_time_s,
                            metadata_json=meta,
                            completed_at=comp_at,
                        )
                        .on_conflict_do_update(
                            constraint="uq_pur_manifest_unit",
                            set_={
                                "status": event.status,
                                "result_s3_key": event.result_s3_key or None,
                                "duration_seconds": event.processing_time_s,
                                "metadata_json": meta,
                                "completed_at": comp_at,
                            },
                        )
                    )
                    await session.execute(stmt)
                    await session.commit()
                finally:
                    await session.close()

                session = await get_async_session(settings.aurora_secret_name)
                try:
                    result = await session.execute(
                        text(
                            f"""
                            UPDATE {SCHEMA}.processing_manifests m
                            SET completed_units = sub.completed_count,
                                failed_units = sub.failed_count
                            FROM (
                                SELECT
                                    COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
                                    COUNT(*) FILTER (WHERE status = 'failed') AS failed_count
                                FROM {SCHEMA}.processing_unit_results
                                WHERE manifest_id = CAST(:mid AS uuid)
                            ) sub
                            WHERE m.manifest_id = CAST(:mid AS uuid)
                            RETURNING m.completed_units, m.failed_units, m.total_units
                            """
                        ),
                        {"mid": event.manifest_id},
                    )
                    await session.commit()
                    row = result.fetchone()
                finally:
                    await session.close()

                if row is None:
                    await consumer.commit()
                    continue

                completed, failed, total = int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
                if completed + failed < total:
                    await consumer.commit()
                    continue

                session = await get_async_session(settings.aurora_secret_name)
                try:
                    lock_result = await session.execute(
                        text(
                            f"""
                            UPDATE {SCHEMA}.processing_manifests
                            SET status = CASE
                                    WHEN failed_units = 0 THEN 'completed'
                                    ELSE 'partial_failure'
                                END,
                                finalized_by = :instance,
                                completed_at = NOW() AT TIME ZONE 'utc'
                            WHERE manifest_id = CAST(:mid AS uuid) AND status = 'pending'
                            RETURNING manifest_id, status, output_key_prefix, s3_bucket, evidence_id,
                                      case_id, user_id, job_context_json
                            """
                        ),
                        {"mid": event.manifest_id, "instance": INSTANCE_ID},
                    )
                    await session.commit()
                    lock_row = lock_result.fetchone()
                finally:
                    await session.close()

                if lock_row is None:
                    await consumer.commit()
                    continue

                final_status = lock_row[1]
                out_prefix = lock_row[2]
                bucket = lock_row[3]
                evidence_id = lock_row[4]
                case_id = lock_row[5]
                user_id = lock_row[6]
                job_ctx = lock_row[7] or {}
                doc_type = str(job_ctx.get("document_type", "") or "")

                if final_status == "completed":
                    merged = await merge_document_manifest_units(
                        manifest_id=event.manifest_id,
                        s3_bucket=bucket,
                    )
                    await asyncio.to_thread(
                        upload_document_result, bucket, out_prefix, merged, settings
                    )

                    json_key = f"{out_prefix}.document.json"
                    page_count = int((merged.get("stats") or {}).get("num_pages") or 0)
                    # Synthetic s3_key shape for evidence_id embedding in event (completion consumer matches evidence)
                    synthetic_key = f"client-data/cases/{case_id}/users/{user_id}/evidence/{evidence_id}/file.{doc_type or 'pdf'}"

                    done_ev = DocumentProcessingCompletedEvent(
                        source="document-aggregator",
                        trace_id=event.trace_id,
                        evidence_id=evidence_id,
                        case_id=case_id,
                        user_id=user_id,
                        s3_bucket=bucket,
                        document_s3_key=json_key,
                        page_count=page_count,
                        document_type=doc_type,
                    )
                    # Preserve legacy field expectations: s3_key in completion path uses original key shape;
                    # EP completion consumer uses evidence_id from event — ensure synthetic_key carries ids.
                    done_ev.model_dump()
                    session = await get_async_session(settings.aurora_secret_name)
                    try:
                        await enqueue_outbox(
                            session,
                            topic=done_ev.TOPIC,
                            partition_key=done_ev.partition_key(),
                            value=done_ev.to_kafka_value(),
                        )
                        await session.commit()
                    finally:
                        await session.close()
                    del synthetic_key  # placeholder if we attach real s3_key later
                else:
                    from flash_events.dlq import ProcessingFailedEvent

                    fail_ev = ProcessingFailedEvent(
                        source="document-aggregator",
                        trace_id=event.trace_id or str(uuid.uuid4()),
                        evidence_id=evidence_id,
                        case_id=case_id,
                        user_id=user_id,
                        original_topic=DocumentUnitCompletedEvent.TOPIC,
                        original_event="",
                        error=f"Document pipeline partial failure: {failed}/{total} units failed",
                        retry_count=0,
                        service="document-analysis",
                    )
                    session = await get_async_session(settings.aurora_secret_name)
                    try:
                        await enqueue_outbox(
                            session,
                            topic=fail_ev.TOPIC,
                            partition_key=fail_ev.partition_key(),
                            value=fail_ev.to_kafka_value(),
                        )
                        await session.commit()
                    finally:
                        await session.close()

                await consumer.commit()

            except Exception:
                logger.exception("aggregator error")
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
    settings = get_settings()
    asyncio.run(_run(settings))
