"""Load partial JSON artifacts from S3 and merge into final document output."""

from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

import boto3

from document_analysis.aggregator.merger import merge_document_units
from document_analysis.common.db import get_async_session
from document_analysis.common.models import ProcessingUnitResult
from document_analysis.config.settings import get_settings

logger = logging.getLogger(__name__)


async def merge_document_manifest_units(
    *,
    manifest_id: str,
    s3_bucket: str,
) -> dict[str, Any]:
    """Fetch all completed unit JSON blobs from S3 and merge."""
    settings = get_settings()
    mid = UUID(manifest_id)
    session = await get_async_session(settings.aurora_secret_name)
    try:
        from sqlalchemy import select

        result = await session.execute(
            select(ProcessingUnitResult)
            .where(
                ProcessingUnitResult.manifest_id == mid,
                ProcessingUnitResult.status == "completed",
            )
            .order_by(ProcessingUnitResult.unit_index)
        )
        rows = result.scalars().all()
    finally:
        await session.close()

    if not rows:
        raise ValueError(f"No completed units for manifest {manifest_id}")

    partials: list[tuple[int, dict[str, Any]]] = []
    s3 = boto3.client("s3", region_name=settings.aws_default_region)
    for row in rows:
        if not row.result_s3_key:
            continue
        obj = s3.get_object(Bucket=s3_bucket, Key=row.result_s3_key)
        raw = obj["Body"].read().decode("utf-8")
        data = json.loads(raw)
        partials.append((row.unit_index, data))

    partials.sort(key=lambda x: x[0])
    return merge_document_units(partials)
