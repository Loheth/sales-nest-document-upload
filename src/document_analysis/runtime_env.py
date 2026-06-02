"""Normalize process env before importing Docling (reads DOCLING_* settings at import-time)."""

from __future__ import annotations

import os

_DEFAULT_DOCLING_ELEMENTS_BATCH = "2"


def configure_windows_hf_hub_cache_without_symlinks() -> None:
    """Avoid Hugging Face hub symlinks on Windows without Developer Mode.

    Without this, first-time PDF layout model downloads can fail with WinError 1314.
    """
    if os.name != "nt":
        return
    try:
        import huggingface_hub.file_download as hf_fd
    except ImportError:
        return

    def _symlinks_unsupported(cache_dir=None) -> bool:
        return False

    hf_fd.are_symlinks_supported = _symlinks_unsupported
    hf_fd._are_symlinks_supported_in_dir.clear()


def configure_docling_process_env_before_docling_import() -> None:
    """Expose tuning knobs backed by Docling's DOCLING_* env prefix.

    Docling parses ``DOCLING_PERF_ELEMENTS_BATCH_SIZE`` when
    ``docling.datamodel.settings`` is imported, so callers must invoke this before
    any ``docling`` import path that loads settings.

    Overrides:
      ``DOCUMENT_ANALYSIS_DOCLING_ELEMENTS_BATCH_SIZE``
        → defaults ``DOCLING_PERF_ELEMENTS_BATCH_SIZE`` if unset.
    """
    raw = os.getenv(
        "DOCUMENT_ANALYSIS_DOCLING_ELEMENTS_BATCH_SIZE", _DEFAULT_DOCLING_ELEMENTS_BATCH
    )
    batch = raw.strip() if raw else ""
    if batch.isdigit():
        os.environ.setdefault("DOCLING_PERF_ELEMENTS_BATCH_SIZE", batch)
