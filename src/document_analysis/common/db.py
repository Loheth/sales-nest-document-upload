"""Aurora PostgreSQL connection for the partitioned document pipeline (same DB as EP / audio)."""

from __future__ import annotations

import json
import logging
import os

import boto3
from botocore.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

logger = logging.getLogger(__name__)

_async_engine = None
_async_session_factory: async_sessionmaker[AsyncSession] | None = None

SCHEMA_NAME = "documents_partitioned"


def _get_aws_credentials() -> dict[str, str | None]:
    region = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-gov-west-1"))
    session = boto3.Session(region_name=region)
    creds = session.get_credentials()
    if creds:
        frozen = creds.get_frozen_credentials()
        return {
            "aws_access_key_id": frozen.access_key,
            "aws_secret_access_key": frozen.secret_key,
            "aws_session_token": frozen.token,
        }
    raise RuntimeError("No AWS credentials available")


def _build_database_url(aurora_secret_name: str) -> str:
    if not aurora_secret_name:
        fallback = os.getenv(
            "DATABASE_URL",
            "postgresql+asyncpg://postgres:postgres@localhost:5432/flash_dev",
        )
        logger.info("AURORA_SECRET_NAME not set, using DATABASE_URL fallback")
        return fallback

    region = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-gov-west-1"))
    is_ecs = any(
        os.getenv(key)
        for key in ["ECS_CONTAINER_CREDENTIALS_RELATIVE_URI", "ECS_CONTAINER_METADATA_URI", "ECS"]
    )
    config = Config(
        connect_timeout=30 if is_ecs else 10,
        read_timeout=30 if is_ecs else 10,
        retries={"max_attempts": 3 if is_ecs else 2, "mode": "adaptive"},
    )

    creds = _get_aws_credentials()
    client = boto3.client(
        "secretsmanager",
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
        aws_session_token=creds["aws_session_token"],
        region_name=region,
        config=config,
    )
    response = client.get_secret_value(SecretId=aurora_secret_name)
    db_creds = json.loads(response["SecretString"])

    host = db_creds.get("host")
    port = db_creds.get("port", 5432)
    database = db_creds.get("database", "postgres")
    username = db_creds.get("username")
    password = db_creds.get("password")

    if not all([host, username, password]):
        raise ValueError("Missing required Aurora credentials in Secrets Manager")

    url = f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{database}"
    logger.info("Aurora connection configured: %s@%s:%s/%s", username, host, port, database)
    return url


def get_async_engine(aurora_secret_name: str = ""):
    global _async_engine
    if _async_engine is None:
        secret = aurora_secret_name or os.getenv("AURORA_SECRET_NAME", "")
        _async_engine = create_async_engine(
            _build_database_url(secret),
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
    return _async_engine


def get_async_session_factory(aurora_secret_name: str = "") -> async_sessionmaker[AsyncSession]:
    global _async_session_factory
    if _async_session_factory is None:
        _async_session_factory = async_sessionmaker(
            get_async_engine(aurora_secret_name),
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )
    return _async_session_factory


async def get_async_session(aurora_secret_name: str = "") -> AsyncSession:
    factory = get_async_session_factory(aurora_secret_name)
    return factory()


def sync_database_url_from_async(async_url: str) -> str:
    """Build a sync psycopg URL for lease refresher threads."""
    if "+asyncpg" in async_url:
        return async_url.replace("postgresql+asyncpg", "postgresql+psycopg", 1)
    return async_url.replace("postgresql://", "postgresql+psycopg://", 1)


async def ensure_schema(aurora_secret_name: str = "") -> None:
    """Create the documents_partitioned schema/tables (idempotent)."""
    engine = get_async_engine(aurora_secret_name)
    async with engine.begin() as conn:
        await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))

    from document_analysis.common.models import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS idx_kafka_outbox_pending
                ON {SCHEMA_NAME}.kafka_outbox (id)
                WHERE shipped_at IS NULL
                """
            )
        )

    logger.info("Schema '%s' ensured for document pipeline", SCHEMA_NAME)
