"""pikepdf-based extraction of embedded PDF images when Docling misses them (full-bleed photos)."""

from __future__ import annotations

import logging
import re
import threading
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pikepdf
from PIL import Image
from PIL.Image import Resampling

from document_analysis.config.settings import Settings
from document_analysis.services.bedrock_chat_completions_transport import (
    describe_image_via_bedrock_with_usage,
)
from document_analysis.services.local_picture_vlm_singleton import (
    get_local_fallback_vlm_model,
)

logger = logging.getLogger(__name__)

# Match ``document_conversion._MODEL_ARTIFACT_RE`` (avoid importing that module).
_MODEL_ARTIFACT_RE = re.compile(r"<end_of_utteranc\w*>?")


def _strip_model_artifacts(text: str) -> str:
    return _MODEL_ARTIFACT_RE.sub("", text)


# Serialize local VLM inference: Docling transformers engine is not safe for concurrent use.
_LOCAL_VLM_INFER_LOCK = threading.Lock()

# Match Docling PictureDescriptionApiOptions default prompt for consistency.
_FALLBACK_PROMPT = "Describe this image in a few sentences."


def _resize_for_vlm(im: Image.Image, max_long_edge_px: int) -> Image.Image:
    if max_long_edge_px <= 0:
        return im
    w, h = im.size
    longest = max(w, h)
    if longest <= max_long_edge_px:
        return im
    scale = max_long_edge_px / float(longest)
    new_w = max(1, round(w * scale))
    new_h = max(1, round(h * scale))
    resized = im.resize((new_w, new_h), Resampling.LANCZOS)
    return resized if resized.mode == "RGB" else resized.convert("RGB")


def iter_pdf_page_images(local_path: Path, settings: Settings) -> Iterator[tuple[int, Image.Image]]:
    """Yield ``(page_no, pil)`` for each embedded XObject image that passes size filters."""

    min_edge = settings.pdf_image_fallback_min_long_edge_px
    max_px = settings.picture_crop_max_long_edge_px

    with pikepdf.Pdf.open(local_path) as pdf:
        for page_no, page in enumerate(pdf.pages, start=1):
            for _name, xobj in page.images.items():
                try:
                    pil = pikepdf.PdfImage(xobj).as_pil_image()
                except Exception:
                    logger.debug(
                        "pikepdf could not rasterize image on page %s", page_no, exc_info=True
                    )
                    continue
                w, h = pil.size
                if max(w, h) < min_edge:
                    continue
                yield page_no, _resize_for_vlm(pil.convert("RGB"), max_px)


def describe_missing_pages(
    local_path: Path,
    *,
    doclng_described_pages: set[int],
    settings: Settings,
) -> dict[int, list[str]]:
    """Describe embedded PDF images on pages Docling left without picture text.

    Backend ``bedrock`` uses Chat Completions (ThreadPool concurrency).
    Backend ``local`` uses the bundled SmolVLM engine with serialized batched inference.
    """

    if not settings.pdf_image_fallback_enabled:
        return {}

    page_buckets: dict[int, list[tuple[Image.Image, int]]] = defaultdict(list)
    try:
        for page_no, pil in iter_pdf_page_images(local_path, settings):
            w, h = pil.size
            page_buckets[page_no].append((pil, w * h))
    except Exception:
        logger.warning("pdf_image_fallback: could not open %s", local_path, exc_info=True)
        return {}

    max_total = settings.pdf_image_fallback_max_total_images
    max_per = settings.pdf_image_fallback_max_images_per_page

    tasks: list[tuple[int, Image.Image]] = []
    total = 0
    for page_no in sorted(page_buckets.keys()):
        if page_no in doclng_described_pages:
            continue
        ranked = sorted(page_buckets[page_no], key=lambda x: -x[1])
        for pil, _ in ranked[:max_per]:
            if total >= max_total:
                break
            tasks.append((page_no, pil))
            total += 1
        if total >= max_total:
            break

    if not tasks:
        return {}

    out: dict[int, list[str]] = defaultdict(list)
    backend = settings.picture_description_backend

    if backend == "bedrock":
        tokens_acc = 0

        def _describe(item: tuple[int, Image.Image]) -> tuple[int, str | None, int | None]:
            pg, pil = item
            text, tok, _fr = describe_image_via_bedrock_with_usage(
                pil,
                prompt=_FALLBACK_PROMPT,
                settings=settings,
            )
            return pg, text, tok

        with ThreadPoolExecutor(max_workers=settings.picture_description_api_concurrency) as pool:
            futures = [pool.submit(_describe, t) for t in tasks]
            for fut in as_completed(futures):
                try:
                    pg, text, tok = fut.result()
                except Exception:
                    logger.warning("pdf_image_fallback describe task failed", exc_info=True)
                    continue
                if tok is not None:
                    tokens_acc += tok
                if text:
                    out[pg].append(f"[Image]\n{_strip_model_artifacts(text.strip())}")

        logger.info(
            "pdf_image_fallback(bedrock) path=%s tasks=%s tokens_reported_sum=%s pages_with_output=%s",
            local_path.name,
            len(tasks),
            tokens_acc,
            len(out),
        )
        return dict(out)

    if backend == "local":
        model = get_local_fallback_vlm_model(settings)
        if model is None:
            return {}

        batch_size = max(1, settings.picture_description_vlm_batch_size)
        with _LOCAL_VLM_INFER_LOCK:
            i = 0
            while i < len(tasks):
                chunk = tasks[i : i + batch_size]
                pgs = [pg for pg, _ in chunk]
                pils = [pil for _, pil in chunk]
                try:
                    texts = list(model._annotate_images(pils))
                except Exception:
                    logger.warning(
                        "pdf_image_fallback local SmolVLM batch failed path=%s",
                        local_path.name,
                        exc_info=True,
                    )
                    texts = [""] * len(chunk)
                if len(texts) < len(chunk):
                    texts.extend([""] * (len(chunk) - len(texts)))
                for pg, raw in zip(pgs, texts, strict=True):
                    body = _strip_model_artifacts((raw or "").strip())
                    if body:
                        out[pg].append(f"[Image]\n{body}")
                i += batch_size

        logger.info(
            "pdf_image_fallback(local) path=%s tasks=%s pages_with_output=%s",
            local_path.name,
            len(tasks),
            len(out),
        )
        return dict(out)

    logger.warning(
        "pdf_image_fallback unsupported picture_description_backend=%r; skipping",
        backend,
    )
    return {}
