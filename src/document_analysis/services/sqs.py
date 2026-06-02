"""SQS helpers for the Kafka → SQS bridge and Docling worker."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import boto3

from document_analysis.config.settings import Settings

logger = logging.getLogger(__name__)


def send_job(settings: Settings, payload: dict[str, Any]) -> None:
    """Publish a job to the worker queue."""
    if not settings.sqs_queue_url:
        raise ValueError("sqs_queue_url is not configured")
    client = boto3.client("sqs", region_name=settings.aws_default_region)
    client.send_message(
        QueueUrl=settings.sqs_queue_url,
        MessageBody=json.dumps(payload),
    )


@dataclass(frozen=True)
class ReceivedSqsMessage:
    """One message from ``receive_messages``."""

    receipt_handle: str
    body: dict[str, Any]


def receive_messages(settings: Settings) -> list[ReceivedSqsMessage]:
    """Long-poll receive (blocking). Returns zero or more messages."""
    if not settings.sqs_queue_url:
        raise ValueError("sqs_queue_url is not configured")
    client = boto3.client("sqs", region_name=settings.aws_default_region)
    resp = client.receive_message(
        QueueUrl=settings.sqs_queue_url,
        MaxNumberOfMessages=settings.sqs_max_messages,
        WaitTimeSeconds=settings.sqs_wait_time_seconds,
        VisibilityTimeout=settings.sqs_visibility_timeout_seconds,
        AttributeNames=["All"],
    )
    raw = resp.get("Messages") or []
    out: list[ReceivedSqsMessage] = []
    for m in raw:
        handle = m.get("ReceiptHandle")
        body_raw = m.get("Body")
        if not handle or body_raw is None:
            continue
        try:
            body = json.loads(body_raw)
        except json.JSONDecodeError as exc:
            logger.error("Malformed SQS body; deleting to avoid poison loop: %s", exc)
            delete_message(settings, handle)
            continue
        if not isinstance(body, dict):
            logger.error("SQS body is not a JSON object; deleting: %r", body)
            delete_message(settings, handle)
            continue
        out.append(ReceivedSqsMessage(receipt_handle=handle, body=body))
    return out


def delete_message(settings: Settings, receipt_handle: str) -> None:
    if not settings.sqs_queue_url:
        raise ValueError("sqs_queue_url is not configured")
    client = boto3.client("sqs", region_name=settings.aws_default_region)
    client.delete_message(QueueUrl=settings.sqs_queue_url, ReceiptHandle=receipt_handle)


def extend_visibility(settings: Settings, receipt_handle: str, seconds: int) -> None:
    """Reset visibility timeout (cap at SQS max 12 hours)."""
    if not settings.sqs_queue_url:
        raise ValueError("sqs_queue_url is not configured")
    capped = max(0, min(seconds, 43_200))
    client = boto3.client("sqs", region_name=settings.aws_default_region)
    client.change_message_visibility(
        QueueUrl=settings.sqs_queue_url,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=capped,
    )
