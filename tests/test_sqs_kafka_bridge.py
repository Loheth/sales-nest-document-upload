"""SQS bridge payload: base64 wrap of same bytes Kafka would deliver."""

from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

from document_analysis.config.settings import Settings
from document_analysis.services.sqs import send_job


def test_document_event_round_trips_through_value_b64() -> None:
    from flash_events.document import DocumentProcessingRequestedEvent

    event = DocumentProcessingRequestedEvent(
        source="test",
        evidence_id="ev-1",
        case_id="case-1",
        user_id="user-1",
        s3_bucket="my-bucket",
        s3_key="cases/c1/evidence/e1/original/doc.pdf",
        output_key_prefix="cases/c1/evidence/e1/original/doc",
    )
    wire = event.to_kafka_value()
    payload = {
        "value_b64": base64.b64encode(wire).decode("ascii"),
        "kafka_partition": 3,
        "kafka_offset": 42,
    }
    raw = base64.b64decode(payload["value_b64"])
    decoded = DocumentProcessingRequestedEvent.from_kafka_value(raw)
    assert decoded.evidence_id == event.evidence_id
    assert decoded.s3_key == event.s3_key
    assert payload["kafka_partition"] == 3


@patch("document_analysis.services.sqs.boto3.client")
def test_send_job_serializes_json_body(mock_client: MagicMock) -> None:
    mock_client.return_value.send_message = MagicMock()
    settings = Settings(
        aws_default_region="us-gov-west-1",
        sqs_queue_url="https://sqs.us-gov-west-1.amazonaws.com/123/queue",
    )
    send_job(settings, {"value_b64": "e30=", "kafka_partition": 0, "kafka_offset": 0})
    call_kw = mock_client.return_value.send_message.call_args.kwargs
    assert call_kw["QueueUrl"] == settings.sqs_queue_url
    assert '"kafka_offset": 0' in call_kw["MessageBody"]
