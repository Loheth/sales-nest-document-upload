"""Kafka consumer for document processing job intake."""

from __future__ import annotations

import logging
from typing import Any

from aiokafka import AIOKafkaConsumer

from document_analysis.config.settings import Settings

logger = logging.getLogger(__name__)


async def create_consumer(
    settings: Settings,
    *,
    topics: list[str] | None = None,
    group_id: str | None = None,
    session_timeout_ms: int | None = None,
    heartbeat_interval_ms: int | None = None,
    max_poll_interval_ms: int | None = None,
) -> AIOKafkaConsumer:
    """Create and start a Kafka consumer.

    Uses ``topics`` when provided; otherwise ``settings.kafka_inbound_topics_list``.
    Uses ``group_id`` when provided; otherwise ``settings.kafka_consumer_group``.
    """
    consumer_kwargs: dict[str, Any] = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "group_id": group_id or settings.kafka_consumer_group,
        "security_protocol": settings.kafka_security_protocol,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "max_poll_records": settings.kafka_max_poll_records,
    }
    if session_timeout_ms is not None:
        consumer_kwargs["session_timeout_ms"] = session_timeout_ms
    if heartbeat_interval_ms is not None:
        consumer_kwargs["heartbeat_interval_ms"] = heartbeat_interval_ms
    if max_poll_interval_ms is not None:
        consumer_kwargs["max_poll_interval_ms"] = max_poll_interval_ms

    topic_list = topics if topics is not None else settings.kafka_inbound_topics_list
    if not topic_list:
        raise ValueError("kafka_inbound_topic(s) must be set for Kafka consumer mode")

    consumer = AIOKafkaConsumer(*topic_list, **consumer_kwargs)
    await consumer.start()
    logger.info(
        "Kafka consumer started: topics=%s group=%s servers=%s",
        topic_list,
        consumer_kwargs["group_id"],
        settings.kafka_bootstrap_servers,
    )
    return consumer
