"""SQLAlchemy models for partitioned document processing.

Uses a dedicated ``documents_partitioned`` schema. The doc pipeline tables
(``kafka_outbox`` in particular) intentionally do not share definitions with
the audio/video ``partitioned_processing`` schema — those services use a
JSONB outbox keyed on ``sent_at``/``status``, whereas the doc pipeline uses a
binary-payload outbox keyed on ``shipped_at``. Keeping a separate schema
avoids ``CREATE TABLE IF NOT EXISTS`` no-ops silently masking schema drift.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase

SCHEMA = "documents_partitioned"


class Base(DeclarativeBase):
    pass


class ProcessingManifest(Base):
    """Tracks a partitioned processing job across its full lifecycle."""

    __tablename__ = "processing_manifests"
    __table_args__ = (
        Index("idx_pm_evidence_id", "evidence_id"),
        Index("idx_pm_status", "status"),
        {"schema": SCHEMA},
    )

    manifest_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    evidence_id = Column(String(255), nullable=False)
    case_id = Column(String(255), nullable=False)
    user_id = Column(String(255), nullable=False)
    modality = Column(String(20), nullable=False)  # document
    s3_bucket = Column(String(512), nullable=False)
    manifest_s3_key = Column(String(1024), nullable=False)
    output_key_prefix = Column(String(1024), nullable=False)
    total_units = Column(Integer, nullable=False)
    completed_units = Column(Integer, nullable=False, default=0)
    failed_units = Column(Integer, nullable=False, default=0)
    status = Column(String(20), nullable=False, default="pending")
    finalized_by = Column(String(255), nullable=True)
    trace_id = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    completed_at = Column(DateTime(timezone=True), nullable=True)
    job_kind = Column(String(20), nullable=False, default="evidence")
    job_context_json = Column(JSONB, nullable=False, default=dict)


class ProcessingUnitResult(Base):
    """Tracks one unit of work within a partitioned job."""

    __tablename__ = "processing_unit_results"
    __table_args__ = (
        UniqueConstraint("manifest_id", "unit_index", name="uq_pur_manifest_unit"),
        Index("idx_pur_manifest_id", "manifest_id"),
        {"schema": SCHEMA},
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    manifest_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.processing_manifests.manifest_id"),
        nullable=False,
    )
    unit_index = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False, default="pending")
    result_s3_key = Column(String(1024), nullable=True)
    duration_seconds = Column(Float, nullable=True)
    metadata_json = Column(JSONB, nullable=False, default=dict)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    completed_at = Column(DateTime(timezone=True), nullable=True)


class ProcessingUnitLease(Base):
    """Liveness lease for one unit (GIL-independent refresher extends ``expires_at``)."""

    __tablename__ = "processing_unit_leases"
    __table_args__ = (
        Index("idx_pul_expires", "expires_at"),
        Index("idx_pul_manifest", "manifest_id"),
        {"schema": SCHEMA},
    )

    unit_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.processing_unit_results.id", ondelete="CASCADE"),
        primary_key=True,
    )
    manifest_id = Column(UUID(as_uuid=True), nullable=False)
    worker_id = Column(String(255), nullable=False)
    acquired_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    expires_at = Column(DateTime(timezone=True), nullable=False)
    released_at = Column(DateTime(timezone=True), nullable=True)


class KafkaOutbox(Base):
    """Transactional outbox for Kafka emits (at-least-once delivery)."""

    __tablename__ = "kafka_outbox"
    __table_args__ = ({"schema": SCHEMA},)

    id = Column(Integer, primary_key=True, autoincrement=True)
    topic = Column(String(512), nullable=False)
    partition_key = Column(LargeBinary, nullable=False)
    value = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    shipped_at = Column(DateTime(timezone=True), nullable=True)
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
