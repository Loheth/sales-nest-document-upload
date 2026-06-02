"""Base Kafka event serialization compatible with document-analysis consumers."""

from __future__ import annotations

from typing import ClassVar, Self

from pydantic import BaseModel, ConfigDict


class KafkaEvent(BaseModel):
    """JSON Kafka value encoding shared by document pipeline events."""

    model_config = ConfigDict(extra="allow")

    TOPIC: ClassVar[str] = ""

    source: str = ""
    trace_id: str = ""

    def partition_key(self) -> str:
        case_id = getattr(self, "case_id", None)
        if case_id:
            return str(case_id)
        evidence_id = getattr(self, "evidence_id", None)
        return str(evidence_id) if evidence_id else ""

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json(exclude_none=True).encode("utf-8")

    @classmethod
    def from_kafka_value(cls, data: bytes) -> Self:
        return cls.model_validate_json(data)
