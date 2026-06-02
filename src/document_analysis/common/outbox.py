"""Transactional outbox: insert rows in DB transactions; dispatcher ships to Kafka."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import structlog
from sqlalchemy import select, text

from document_analysis.common.db import SCHEMA_NAME, get_async_session
from document_analysis.common.models import KafkaOutbox

logger = structlog.get_logger(__name__)


async def enqueue_outbox(
    session,
    *,
    topic: str,
    partition_key: str,
    value: bytes,
) -> None:
    """Insert one outbox row (call inside the same transaction as domain writes)."""
    row = KafkaOutbox(
        topic=topic,
        partition_key=partition_key.encode("utf-8"),
        value=value,
        created_at=datetime.now(UTC),
    )
    session.add(row)


async def run_outbox_dispatcher(
    *,
    aurora_secret_name: str,
    stop_event: asyncio.Event,
    poll_interval_sec: float = 0.25,
    batch_size: int = 50,
) -> None:
    """Background task: ship pending ``kafka_outbox`` rows to Kafka."""
    from document_analysis.services.kafka_producer import get_producer

    while not stop_event.is_set():
        try:
            session = await get_async_session(aurora_secret_name)
            try:
                result = await session.execute(
                    text(
                        f"""
                        SELECT id FROM {SCHEMA_NAME}.kafka_outbox
                        WHERE shipped_at IS NULL
                        ORDER BY id ASC
                        LIMIT :lim
                        FOR UPDATE SKIP LOCKED
                        """
                    ),
                    {"lim": batch_size},
                )
                ids = [r[0] for r in result.fetchall()]
                if not ids:
                    await session.commit()
                    await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_sec)
                    continue

                rows = (
                    (
                        await session.execute(
                            select(KafkaOutbox)
                            .where(KafkaOutbox.id.in_(ids))
                            .order_by(KafkaOutbox.id)
                        )
                    )
                    .scalars()
                    .all()
                )

                producer = await get_producer()
                if producer is None:
                    await session.commit()
                    await asyncio.sleep(1.0)
                    continue

                for row in rows:
                    key = row.partition_key.decode("utf-8") if row.partition_key else ""
                    try:
                        await producer.send_and_wait(row.topic, key=key, value=row.value)
                        await session.execute(
                            text(
                                f"""
                                UPDATE {SCHEMA_NAME}.kafka_outbox
                                SET shipped_at = NOW() AT TIME ZONE 'utc'
                                WHERE id = :id
                                """
                            ),
                            {"id": row.id},
                        )
                    except Exception as exc:
                        await session.execute(
                            text(
                                f"""
                                UPDATE {SCHEMA_NAME}.kafka_outbox
                                SET attempts = attempts + 1, last_error = :err
                                WHERE id = :id
                                """
                            ),
                            {"err": str(exc)[:2000], "id": row.id},
                        )
                        logger.warning(
                            "outbox_ship_failed", id=row.id, topic=row.topic, err=str(exc)
                        )

                await session.commit()
            finally:
                await session.close()
        except TimeoutError:
            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger(__name__).exception("outbox dispatcher iteration failed")
            await asyncio.sleep(1.0)
