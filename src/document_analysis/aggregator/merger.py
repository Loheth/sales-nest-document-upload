"""Merge per-unit Docling outputs into one ``convert_document``-shaped result."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def merge_document_units(
    sorted_units: list[tuple[int, dict[str, Any]]],
) -> dict[str, Any]:
    """Merge partial results ordered by ``unit_index``.

    Each partial dict matches ``document_conversion.convert_document`` output:
    ``document_json``, ``markdown``, ``pages``, ``stats``.
    """
    if not sorted_units:
        return {
            "document_json": {},
            "markdown": "",
            "pages": [],
            "stats": {"num_pages": 0, "processing_time_s": 0.0},
        }

    merged_md: list[str] = []
    merged_pages: list[dict[str, Any]] = []
    merged_json_parts: list[Any] = []
    total_pages = 0
    total_time = 0.0
    page_cursor = 0

    for _unit_index, partial in sorted_units:
        stats = partial.get("stats") or {}
        n = int(stats.get("num_pages") or 0)
        chunk_pages = partial.get("pages") or []

        for p in chunk_pages:
            if isinstance(p, dict):
                raw_no = p.get("page_no")
                try:
                    local_no = int(raw_no) if raw_no is not None else 1
                except (TypeError, ValueError):
                    local_no = 1
                new_no = page_cursor + local_no
                row = dict(p)
                row["page_no"] = new_no
                merged_pages.append(row)
            else:
                merged_pages.append(p)

        if chunk_pages:
            max_local = max(
                (int(p.get("page_no") or 0) for p in chunk_pages if isinstance(p, dict)),
                default=0,
            )
            page_cursor += max_local
        else:
            page_cursor += n

        total_pages += n
        total_time += float(stats.get("processing_time_s") or 0.0)

        md = (partial.get("markdown") or "").strip()
        if md:
            merged_md.append(md)

        dj = partial.get("document_json")
        if dj is not None:
            merged_json_parts.append(dj)

    merged_markdown = "\n\n".join(merged_md)
    merged_document_json: dict[str, Any]
    if len(merged_json_parts) == 1:
        merged_document_json = (
            merged_json_parts[0]
            if isinstance(merged_json_parts[0], dict)
            else {"body": merged_json_parts[0]}
        )
    else:
        merged_document_json = {
            "merged_from_units": True,
            "units": merged_json_parts,
        }

    return {
        "document_json": merged_document_json,
        "markdown": merged_markdown,
        "pages": merged_pages,
        "stats": {
            "num_pages": total_pages,
            "processing_time_s": round(total_time, 3),
        },
    }
