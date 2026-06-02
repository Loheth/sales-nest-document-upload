"""Thread-based lease refresh: independent of asyncio GIL / Docling CPU work."""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

SCHEMA = "documents_partitioned"


class LeaseRefresher:
    """Refresh ``processing_unit_leases.expires_at`` from a daemon thread."""

    def __init__(
        self,
        sync_database_url: str,
        unit_id: uuid.UUID,
        *,
        ttl_seconds: float = 30.0,
        interval_seconds: float = 10.0,
    ) -> None:
        self._url = sync_database_url
        self._unit_id = unit_id
        self._ttl = ttl_seconds
        self._interval = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name=f"lease-{self._unit_id}", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval + 5.0)

    def _run(self) -> None:
        import psycopg

        while not self._stop.is_set():
            try:
                with psycopg.connect(self._url, connect_timeout=10) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            UPDATE {SCHEMA}.processing_unit_leases
                            SET expires_at = NOW() AT TIME ZONE 'utc' + %s::interval
                            WHERE unit_id = %s AND released_at IS NULL
                            """,
                            (timedelta(seconds=self._ttl), str(self._unit_id)),
                        )
                    conn.commit()
            except Exception:
                logger.warning(
                    "Lease refresh failed unit_id=%s",
                    self._unit_id,
                    exc_info=True,
                )
            self._stop.wait(self._interval)


def release_lease_sync(sync_database_url: str, unit_id: uuid.UUID) -> None:
    """Mark lease released (sync, call from worker finally block)."""
    import psycopg

    try:
        with psycopg.connect(sync_database_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {SCHEMA}.processing_unit_leases
                    SET released_at = NOW() AT TIME ZONE 'utc'
                    WHERE unit_id = %s AND released_at IS NULL
                    """,
                    (str(unit_id),),
                )
            conn.commit()
    except Exception:
        logger.warning("release_lease_sync failed unit_id=%s", unit_id, exc_info=True)
