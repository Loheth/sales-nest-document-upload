"""Kafka event producer for the document-analysis-microservice.

Emits DocumentProcessingCompletedEvent to MSK/Kafka after a successful
document conversion upload. Retries with backoff on failure.

If all retries fail, logs a CRITICAL with the full event payload.
The evidence-processor's CompletionWaiter will timeout and handle
the fallback path (the S3 result is already uploaded at this point).
"""

from __future__ import annotations

import asyncio

import structlog

from document_analysis.config.settings import get_settings

logger = structlog.get_logger(__name__)

_producer = None
_enabled: bool | None = None

_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0
_BACKOFF_MAX = 10.0


def _is_enabled() -> bool:
    global _enabled
    if _enabled is None:
        _enabled = bool(get_settings().kafka_bootstrap_servers)
    return _enabled


async def _get_producer():
    global _producer
    if not _is_enabled():
        return None
    if _producer is not None:
        return _producer

    from aiokafka import AIOKafkaProducer

    settings = get_settings()
    _producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        security_protocol=settings.kafka_security_protocol,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await _producer.start()
    return _producer


async def get_producer():
    """Public alias for producers (outbox dispatcher, workers)."""
    return await _get_producer()


async def stop_producer() -> None:
    """Gracefully close the Kafka producer (call on shutdown)."""
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


async def emit_document_processing_completed(
    *,
    s3_key: str,
    bucket: str,
    document_s3_key: str,
    page_count: int,
    document_type: str,
) -> None:
    """Emit a DocumentProcessingCompletedEvent to Kafka with retry."""
    if not _is_enabled():
        return

    from flash_events.document import DocumentProcessingCompletedEvent

    from document_analysis.services.s3_key_parser import parse_s3_key

    ids = parse_s3_key(s3_key)
    if ids is None:
        return

    event = DocumentProcessingCompletedEvent(
        source="document-analysis-microservice",
        evidence_id=ids["evidence_id"],
        case_id=ids["case_id"],
        user_id=ids["user_id"],
        s3_bucket=bucket,
        document_s3_key=document_s3_key,
        page_count=page_count,
        document_type=document_type,
    )

    last_error = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            producer = await _get_producer()
            if producer is None:
                return

            await producer.send_and_wait(
                event.TOPIC,
                key=event.partition_key(),
                value=event.to_kafka_value(),
            )
            logger.info(
                "Emitted document.processing.completed",
                evidence_id=ids["evidence_id"],
                case_id=ids["case_id"],
            )
            return  # success

        except Exception as exc:
            last_error = exc
            if attempt < _MAX_RETRIES:
                backoff = min(_BACKOFF_BASE * (2 ** (attempt - 1)), _BACKOFF_MAX)
                logger.warning(
                    "Kafka emit failed (attempt %d/%d) — retrying in %.1fs",
                    attempt,
                    _MAX_RETRIES,
                    backoff,
                    evidence_id=ids["evidence_id"],
                    exc_info=True,
                )
                await asyncio.sleep(backoff)

    # S3 result is already uploaded — CompletionWaiter timeout will handle fallback
    logger.critical(
        "Kafka emit failed after %d retries — S3 result exists, CompletionWaiter will timeout: %s",
        _MAX_RETRIES,
        event.model_dump_json(),
        evidence_id=ids["evidence_id"],
        exc_info=last_error,
    )
