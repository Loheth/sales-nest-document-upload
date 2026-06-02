"""Thin Kafka consumer: forwards ``document.processing.requested`` payloads to SQS."""

from __future__ import annotations

import asyncio
import base64
import logging

from aiokafka.errors import CommitFailedError, IllegalStateError
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from document_analysis.config.settings import Settings, get_settings
from document_analysis.services.kafka_consumer import create_consumer
from document_analysis.services.sqs import send_job

logger = logging.getLogger(__name__)

_LIVENESS_POLL_MS = 1000


async def run_bridge(settings: Settings | None = None) -> None:
    """Read Kafka, send raw event bytes to SQS, commit offset only after successful enqueue."""
    settings = settings or get_settings()
    if not settings.kafka_bootstrap_servers.strip():
        logger.error("KAFKA_BOOTSTRAP_SERVERS is required for bridge mode")
        return
    if not settings.sqs_queue_url.strip():
        logger.error("SQS_QUEUE_URL is required for bridge mode")
        return

    consumer = await create_consumer(settings)
    logger.info(
        "Bridge consuming topic=%s group=%s -> SQS %s",
        settings.kafka_inbound_topic,
        settings.kafka_consumer_group,
        settings.sqs_queue_url,
    )

    async def _safe_commit(tp: TopicPartition, next_offset: int) -> bool:
        try:
            await consumer.commit({tp: OffsetAndMetadata(next_offset, "")})
            return True
        except (CommitFailedError, IllegalStateError) as exc:
            logger.warning(
                "Bridge commit failed (rebalance?): tp=%s next_offset=%s err=%s",
                tp,
                next_offset,
                exc,
            )
            return False

    try:
        while True:
            try:
                batches = await consumer.getmany(timeout_ms=_LIVENESS_POLL_MS)
            except asyncio.CancelledError:
                raise
            except Exception as fetch_exc:
                logger.warning("Bridge fetch failed; retrying: %s", fetch_exc)
                await asyncio.sleep(1)
                continue

            if not batches:
                continue

            for tp, messages in batches.items():
                for msg in messages:
                    payload = {
                        "value_b64": base64.b64encode(msg.value).decode("ascii")
                        if msg.value is not None
                        else "",
                        "kafka_partition": tp.partition,
                        "kafka_offset": msg.offset,
                    }
                    try:
                        await asyncio.to_thread(send_job, settings, payload)
                    except Exception as send_exc:
                        logger.exception(
                            "Bridge SQS send failed; not committing Kafka offset: %s", send_exc
                        )
                        continue

                    ok = await _safe_commit(tp, msg.offset + 1)
                    if ok:
                        logger.info(
                            "Bridge forwarded partition=%s offset=%s to SQS",
                            tp.partition,
                            msg.offset,
                        )
    except asyncio.CancelledError:
        raise
    finally:
        await consumer.stop()
