"""Parse Flash S3 key convention to extract evidence identifiers."""

from __future__ import annotations

import structlog

logger = structlog.get_logger(__name__)


def parse_s3_key(s3_key: str) -> dict[str, str] | None:
    try:
        parts = s3_key.split("/")
        return {"user_id": parts[1], "case_id": parts[3], "evidence_id": parts[5]}
    except (IndexError, ValueError):
        logger.warning("Failed to parse S3 key for Kafka event", s3_key=s3_key)
        return None
