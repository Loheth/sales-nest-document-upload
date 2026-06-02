"""Physically split a PDF into page-range files for parallel Docling."""

from __future__ import annotations

import logging
from pathlib import Path

import pikepdf

logger = logging.getLogger(__name__)


def pdf_page_count(local_path: str | Path) -> int:
    """Return total pages; raises if file is not a readable PDF."""
    path = Path(local_path)
    with pikepdf.open(path) as pdf:
        return len(pdf.pages)


def build_page_ranges(*, total_pages: int, pages_per_unit: int) -> list[tuple[int, int]]:
    """Return inclusive 1-based (start, end) page ranges."""
    if total_pages <= 0:
        return []
    ranges: list[tuple[int, int]] = []
    start = 1
    while start <= total_pages:
        end = min(start + pages_per_unit - 1, total_pages)
        ranges.append((start, end))
        start = end + 1
    return ranges


def split_pdf_to_files(
    local_pdf: str | Path,
    ranges: list[tuple[int, int]],
    out_dir: str | Path,
    *,
    prefix: str = "segment",
) -> list[Path]:
    """Write one PDF file per range; return paths in order.

    ``ranges`` use 1-based inclusive page numbers (pdf.pages index is 0-based).
    """
    local_pdf = Path(local_pdf)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not ranges:
        return []

    out_paths: list[Path] = []
    with pikepdf.open(local_pdf) as src:
        for i, (p0, p1) in enumerate(ranges):
            if p0 < 1 or p1 < p0:
                raise ValueError(f"Invalid page range ({p0}, {p1})")
            # 1-based -> 0-based slice
            a = p0 - 1
            b = p1
            if b > len(src.pages):
                raise ValueError(f"Page range end {p1} exceeds document ({len(src.pages)} pages)")
            dst = pikepdf.Pdf.new()
            for j in range(a, b):
                dst.pages.append(src.pages[j])
            out_path = out_dir / f"{prefix}_{i:03d}.pdf"
            dst.save(out_path)
            out_paths.append(out_path)
            logger.info("Wrote PDF segment %s pages %d-%d", out_path.name, p0, p1)
    return out_paths
