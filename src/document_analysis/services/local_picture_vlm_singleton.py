"""Lazy singleton Docling SmolVLM engine for offline picture description (fallback path)."""

from __future__ import annotations

import logging
import threading
from pathlib import Path

from docling.datamodel.accelerator_options import AcceleratorOptions
from docling.datamodel.pipeline_options import PictureDescriptionVlmEngineOptions
from docling.models.stages.picture_description.picture_description_vlm_engine_model import (
    PictureDescriptionVlmEngineModel,
)

from document_analysis.config.settings import Settings

logger = logging.getLogger(__name__)

_model: PictureDescriptionVlmEngineModel | None = None
_model_lock = threading.Lock()


def smolvlm_options_for_fallback(settings: Settings) -> PictureDescriptionVlmEngineOptions:
    """Match ``document_conversion._picture_description_engine_options`` for SmolVLM."""
    return PictureDescriptionVlmEngineOptions.from_preset("smolvlm").model_copy(
        update={
            "batch_size": settings.picture_description_vlm_batch_size,
            "scale": settings.picture_description_preset_scale,
            "picture_area_threshold": settings.picture_description_area_fraction_min,
            "generation_config": {"max_new_tokens": 200, "do_sample": False},
        }
    )


def get_local_fallback_vlm_model(settings: Settings) -> PictureDescriptionVlmEngineModel | None:
    """Return a shared SmolVLM engine, or ``None`` if ``model_cache_dir`` is missing."""

    global _model
    model_dir = Path(settings.model_cache_dir)
    if not model_dir.is_dir():
        logger.warning(
            "local picture VLM fallback skipped: model_cache_dir does not exist: %s",
            model_dir,
        )
        return None

    with _model_lock:
        if _model is None:
            opts = smolvlm_options_for_fallback(settings)
            _model = PictureDescriptionVlmEngineModel(
                enabled=True,
                enable_remote_services=False,
                artifacts_path=str(model_dir),
                options=opts,
                accelerator_options=AcceleratorOptions(),
            )
            logger.info("Initialized SmolVLM singleton for pdf_image_fallback (local backend)")
        return _model
