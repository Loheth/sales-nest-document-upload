"""Docling-based document conversion: PDF, DOCX, XLSX, PPTX, images, HTML to JSON + Markdown."""

from __future__ import annotations

from document_analysis.runtime_env import configure_docling_process_env_before_docling_import

configure_docling_process_env_before_docling_import()

import logging
import re
import tempfile
import threading
import time
from pathlib import Path

from PIL.Image import Resampling

from docling.datamodel.base_models import InputFormat, ItemAndImageEnrichmentElement
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import (
    PaginatedPipelineOptions,
    PictureDescriptionApiOptions,
    PictureDescriptionBaseOptions,
    PictureDescriptionVlmEngineOptions,
    RapidOcrOptions,
    ThreadedPdfPipelineOptions,
)
from docling.document_converter import DocumentConverter, PdfFormatOption, WordFormatOption
from docling.models.picture_description_base_model import PictureDescriptionBaseModel
from docling_core.types.doc import NodeItem
from docling_core.types.doc.document import DoclingDocument, PictureItem

from document_analysis.config.settings import get_settings
from document_analysis.services.pdf_image_fallback import describe_missing_pages

logger = logging.getLogger(__name__)

_CONVERT_LOCK = threading.Lock()
_PICTURE_PREP_PATCH_INSTALLED = False

# RapidOCR ONNX model filenames (synced from S3 under model_cache_dir/rapidocr/)
RAPIDOCR_DET = "ch_PP-OCRv4_det_infer.onnx"
RAPIDOCR_REC = "ch_PP-OCRv4_rec_infer.onnx"
RAPIDOCR_CLS = "ch_ppocr_mobile_v2.0_cls_infer.onnx"

# Extensions Docling can handle natively (see InputFormat in docling)
SUPPORTED_EXTENSIONS = {
    ".pdf",
    ".docx",
    ".xlsx",
    ".doc",
    ".pptx",
    ".ppt",
    ".html",
    ".htm",
    ".png",
    ".jpg",
    ".jpeg",
    ".tiff",
    ".tif",
    ".bmp",
    ".gif",
}

# Image extensions we accept by first converting them to JPEG before
# handing off to Docling. Docling's image backends don't cover these.
PRECONVERT_IMAGE_EXTENSIONS = {
    ".webp",
    ".heic",
    ".svg",
    ".psd",
    ".rgba",
}

# Full set of accepted extensions (native + those we pre-convert).
ACCEPTED_EXTENSIONS = SUPPORTED_EXTENSIONS | PRECONVERT_IMAGE_EXTENSIONS


def is_supported(path: str | Path) -> bool:
    """Return True if the file extension is accepted (natively or via pre-conversion)."""
    ext = Path(path).suffix.lower()
    return ext in ACCEPTED_EXTENSIONS


def _ensure_picture_crop_limit_patch_installed() -> None:
    """Downscale oversized picture-description crops without forking upstream Docling.

    Docling's picture stage passes full-resolution cropped figures to SmolVLM; we cap long edge
    to keep peak RSS predictable on ECS.
    """

    global _PICTURE_PREP_PATCH_INSTALLED
    if _PICTURE_PREP_PATCH_INSTALLED:
        return

    PDB = PictureDescriptionBaseModel

    raw_prepare_element = PDB.prepare_element

    def prepare_element_bound(
        self: PictureDescriptionBaseModel,
        conv_res: ConversionResult,
        element: NodeItem,
    ) -> ItemAndImageEnrichmentElement | None:
        enriched = raw_prepare_element(self, conv_res, element)
        if enriched is None:
            return None
        max_px = get_settings().picture_crop_max_long_edge_px
        if max_px <= 0:
            return enriched
        im = enriched.image
        w, h = im.size
        longest = max(w, h)
        if longest <= max_px:
            return enriched
        scale = max_px / float(longest)
        new_w = max(1, round(w * scale))
        new_h = max(1, round(h * scale))
        resized = im.resize((new_w, new_h), Resampling.LANCZOS)
        rgb = resized if resized.mode == "RGB" else resized.convert("RGB")
        return enriched.model_copy(update={"image": rgb})

    PDB.prepare_element = prepare_element_bound  # type: ignore[method-assign]
    _PICTURE_PREP_PATCH_INSTALLED = True


def _preconvert_image_to_jpeg(local_path: Path) -> Path:
    """Convert a non-native image format (webp/heic/svg/psd/rgba) to JPEG.

    Returns a path to the temporary JPEG that callers must clean up.
    """
    from PIL import Image

    ext = local_path.suffix.lower()

    # HEIC requires pillow-heif to register the decoder; register lazily so the
    # import cost is paid only for HEIC inputs.
    if ext == ".heic":
        try:
            from pillow_heif import register_heif_opener

            register_heif_opener()
        except ImportError as e:
            raise ValueError(
                "HEIC pre-conversion requires pillow-heif; install it to enable HEIC support"
            ) from e

    if ext == ".svg":
        # Pillow cannot decode SVG. Rasterize with cairosvg to PNG bytes first.
        try:
            import cairosvg
        except ImportError as e:
            raise ValueError(
                "SVG pre-conversion requires cairosvg; install it to enable SVG support"
            ) from e

        png_bytes = cairosvg.svg2png(url=str(local_path))
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            out_path = Path(tmp.name)
        import io as _io

        with Image.open(_io.BytesIO(png_bytes)) as img:
            if img.mode in ("RGBA", "LA", "P"):
                img = img.convert("RGB")
            img.save(out_path, "JPEG", quality=95, optimize=True)
        return out_path

    if ext == ".psd":
        # psd-tools has a richer PSD backend, but Pillow's PSD reader is good
        # enough for a flat composite which is what we want for OCR.
        pass

    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
        out_path = Path(tmp.name)

    with Image.open(local_path) as img:
        if img.mode in ("RGBA", "LA", "P"):
            img = img.convert("RGB")
        img.save(out_path, "JPEG", quality=95, optimize=True)

    logger.info("Pre-converted %s image %s -> %s", ext, local_path.name, out_path)
    return out_path


def _picture_description_bedrock_api_options() -> PictureDescriptionApiOptions:
    settings = get_settings()
    headers: dict[str, str] = {}
    if settings.picture_description_bedrock_use_bearer_auth:
        tok = settings.picture_description_bedrock_bearer_token
        assert tok is not None
        headers["Authorization"] = f"Bearer {tok.get_secret_value().strip()}"

    params: dict[str, object] = {
        "model": settings.picture_description_bedrock_model_id.strip(),
        "max_tokens": settings.picture_description_bedrock_max_tokens,
        "temperature": settings.picture_description_bedrock_temperature,
    }

    return PictureDescriptionApiOptions(
        url=settings.picture_description_bedrock_chat_url_resolved,
        headers=headers,
        params=params,
        timeout=settings.picture_description_api_timeout,
        concurrency=settings.picture_description_api_concurrency,
        prompt="Describe this image in a few sentences.",
        provenance="amazon-bedrock-chat-completions",
        batch_size=settings.picture_description_vlm_batch_size,
        scale=settings.picture_description_preset_scale,
        picture_area_threshold=settings.picture_description_area_fraction_min,
    )


def _picture_description_engine_options(
    *,
    artifacts_path_present: bool,
) -> PictureDescriptionVlmEngineOptions | None:
    settings = get_settings()
    if not artifacts_path_present:
        return None
    return PictureDescriptionVlmEngineOptions.from_preset("smolvlm").model_copy(
        update={
            # Default Docling preset is 8; shrinking keeps predict_batch payloads smaller even
            # when enrichment uses multiple elements concurrently.
            "batch_size": settings.picture_description_vlm_batch_size,
            "scale": settings.picture_description_preset_scale,
            "picture_area_threshold": settings.picture_description_area_fraction_min,
            # Match the tighter cap used by the bundled engine stage (upstream preset allows 4096).
            "generation_config": {"max_new_tokens": 200, "do_sample": False},
        }
    )


def _pdf_pipeline_options() -> ThreadedPdfPipelineOptions:
    """Build PDF pipeline options with optional RapidOCR and picture description (Bedrock or local SmolVLM)."""
    settings = get_settings()
    model_dir = Path(settings.model_cache_dir)
    artifacts_path = str(model_dir) if model_dir.exists() else None

    rapidocr_dir = model_dir / "rapidocr"
    det_path = rapidocr_dir / RAPIDOCR_DET
    rec_path = rapidocr_dir / RAPIDOCR_REC
    cls_path = rapidocr_dir / RAPIDOCR_CLS

    if det_path.is_file() and rec_path.is_file():
        ocr_options = RapidOcrOptions(
            backend="onnxruntime",
            det_model_path=str(det_path),
            rec_model_path=str(rec_path),
            cls_model_path=str(cls_path) if cls_path.is_file() else None,
        )
        do_ocr = True
        logger.info("Using local RapidOCR models from %s", rapidocr_dir)
    else:
        ocr_options = None
        do_ocr = False
        if rapidocr_dir.exists():
            logger.warning(
                "RapidOCR dir exists but missing models (e.g. %s, %s); OCR disabled to avoid remote download",
                RAPIDOCR_DET,
                RAPIDOCR_REC,
            )

    backend = settings.picture_description_backend
    do_picture_description = False
    enable_remote_services = False
    pic_opts: PictureDescriptionBaseOptions | None = None

    if backend == "bedrock":
        do_picture_description = True
        enable_remote_services = True
        pic_opts = _picture_description_bedrock_api_options()
        logger.info(
            "PDF picture description via Bedrock model=%s url=%s",
            settings.picture_description_bedrock_model_id.strip(),
            settings.picture_description_bedrock_chat_url_resolved,
        )
    elif backend == "local" and artifacts_path is not None:
        do_picture_description = True
        enable_remote_services = False
        pic_opts = _picture_description_engine_options(artifacts_path_present=True)
        assert pic_opts is not None
        logger.info(
            "PDF picture description enabled (local SmolVLM artifacts_path=%s)",
            artifacts_path,
        )

    pdf_kwargs: dict[str, object] = {
        "do_ocr": do_ocr,
        "artifacts_path": artifacts_path,
        "do_picture_description": do_picture_description,
        "enable_remote_services": enable_remote_services,
        "images_scale": settings.picture_page_raster_scale,
    }
    if ocr_options is not None:
        pdf_kwargs["ocr_options"] = ocr_options
    if do_picture_description and pic_opts is not None:
        pdf_kwargs["picture_description_options"] = pic_opts

    return ThreadedPdfPipelineOptions(**pdf_kwargs)


def _docx_pipeline_options() -> PaginatedPipelineOptions:
    """Build DOCX pipeline options with optional picture description (Bedrock or local SmolVLM)."""
    settings = get_settings()
    model_dir = Path(settings.model_cache_dir)
    artifacts_path = str(model_dir) if model_dir.exists() else None
    raster = settings.picture_page_raster_scale
    backend = settings.picture_description_backend

    if backend == "bedrock":
        pic_opts = _picture_description_bedrock_api_options()
        logger.info(
            "DOCX picture description via Bedrock model=%s",
            settings.picture_description_bedrock_model_id.strip(),
        )
        return PaginatedPipelineOptions(
            do_picture_description=True,
            enable_remote_services=True,
            artifacts_path=artifacts_path,
            images_scale=raster,
            picture_description_options=pic_opts,
        )
    if backend == "local" and artifacts_path:
        pic_opts = _picture_description_engine_options(artifacts_path_present=True)
        assert pic_opts is not None
        logger.info(
            "DOCX picture description enabled (local SmolVLM artifacts_path=%s)",
            artifacts_path,
        )
        return PaginatedPipelineOptions(
            do_picture_description=True,
            enable_remote_services=False,
            artifacts_path=artifacts_path,
            images_scale=raster,
            picture_description_options=pic_opts,
        )
    return PaginatedPipelineOptions(
        do_picture_description=False,
        enable_remote_services=False,
        artifacts_path=artifacts_path,
        images_scale=raster,
    )


_MODEL_ARTIFACT_RE = re.compile(r"<end_of_utteranc\w*>?")


def _strip_model_artifacts(text: str) -> str:
    """Remove special tokens leaked by the picture-description model (e.g. SmolVLM)."""
    return _MODEL_ARTIFACT_RE.sub("", text)


def _clean_dict_strings(obj: object) -> object:
    """Recursively strip model artifacts from all string values in a nested dict/list."""
    if isinstance(obj, str):
        return _strip_model_artifacts(obj)
    if isinstance(obj, dict):
        return {k: _clean_dict_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_dict_strings(item) for item in obj]
    return obj


def _picture_visual_text_segments(doc: DoclingDocument, pic: PictureItem) -> list[str]:
    """Caption + VLM description lines for downstream per-page NLP (evidence-processor)."""
    out: list[str] = []
    try:
        cap = pic.caption_text(doc).strip()
    except Exception:
        logger.debug("Picture caption_text failed", exc_info=True)
        cap = ""
    if cap:
        out.append(_strip_model_artifacts(cap))
    meta_obj = getattr(pic, "meta", None)
    if meta_obj is not None:
        desc = getattr(meta_obj, "description", None)
        if desc is not None:
            body = getattr(desc, "text", None) or ""
            body = _strip_model_artifacts(body.strip())
            if body:
                out.append(body)
    return out


def _collect_picture_text_chunks_by_page(doc: DoclingDocument) -> dict[int, list[str]]:
    """Gather ``[Image]`` caption/description blocks keyed by Docling page number."""
    by_page: dict[int, list[str]] = {}
    try:
        for element, _level in doc.iterate_items():
            if not isinstance(element, PictureItem):
                continue
            segs = _picture_visual_text_segments(doc, element)
            if not segs:
                continue
            chunk = "[Image]\n" + "\n".join(segs)
            for prov in element.prov:
                by_page.setdefault(prov.page_no, []).append(chunk)
    except Exception:
        logger.warning("Could not extract picture text for downstream export", exc_info=True)
        return {}
    return by_page


def _merge_picture_text_into_pages(
    picture_by_page: dict[int, list[str]], pages_by_num: dict[int, list[str]]
) -> None:
    """Append figure captions/VLM prose to ``pages_by_num``.

    Evidence-processor runs ``extract_entities_and_summary_per_page`` from the serialized
    ``pages`` JSON, which historically only echoed ``doc.texts`` --- so captions on
    ``PictureItem`` never reached entities or graph even when markdown mentioned them.
    """
    for page_no, chunks in picture_by_page.items():
        pages_by_num.setdefault(page_no, []).extend(chunks)


def _append_picture_text_to_markdown(markdown: str, picture_by_page: dict[int, list[str]]) -> str:
    """Ensure ``export_to_markdown()`` output carries the same figure text as ``pages[]``.

    Evidence-processor builds the canonical ``.txt`` and embedding chunks from
    ``.document.md``, so captions and VLM descriptions must appear here---not only in JSON.
    """
    if not picture_by_page:
        return markdown
    base = markdown.rstrip()
    sections: list[str] = []
    for page_no in sorted(picture_by_page.keys()):
        blocks = picture_by_page[page_no]
        if not blocks:
            continue
        sections.append(f"### Page {page_no}\n\n")
        sections.append("\n\n".join(blocks))
        sections.append("\n\n")
    supplement = "".join(sections).strip()
    return f"{base}\n\n---\n\n## Figure and image text\n\n{supplement}\n"


_ensure_picture_crop_limit_patch_installed()

_sig_settings = get_settings()
if (
    _sig_settings.picture_description_backend == "bedrock"
    and not _sig_settings.picture_description_bedrock_use_bearer_auth
):
    from document_analysis.services.bedrock_chat_completions_transport import (
        ensure_docling_api_image_request_uses_sigv4_for_bedrock,
    )

    ensure_docling_api_image_request_uses_sigv4_for_bedrock()


def convert_document(local_path: str) -> dict:
    """Convert a document with Docling and return result dict (document_json, markdown, stats).

    Raises ValueError if the file format is not supported.
    """
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(f"Not a file: {local_path}")
    ext = path.suffix.lower()
    if ext not in ACCEPTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported format: {ext}. Supported: {', '.join(sorted(ACCEPTED_EXTENSIONS))}."
        )

    # For image formats Docling doesn't natively support, convert to JPEG first.
    converted_path: Path | None = None
    if ext in PRECONVERT_IMAGE_EXTENSIONS:
        converted_path = _preconvert_image_to_jpeg(path)
        docling_input = converted_path
    else:
        docling_input = path

    pdf_opts = _pdf_pipeline_options()
    docx_opts = _docx_pipeline_options()
    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_opts),
            InputFormat.DOCX: WordFormatOption(pipeline_options=docx_opts),
        }
    )
    pdb = PictureDescriptionBaseModel

    settings = get_settings()

    try:
        with _CONVERT_LOCK:
            outer_scale_backup = pdb.images_scale
            try:
                pdb.images_scale = settings.picture_enrichment_crop_scale
                t0 = time.perf_counter()
                result = converter.convert(str(docling_input))
                conversion_time_s = time.perf_counter() - t0
            finally:
                pdb.images_scale = outer_scale_backup
    finally:
        if converted_path is not None and converted_path.exists():
            try:
                converted_path.unlink()
            except OSError:
                logger.debug("Could not remove pre-converted image %s", converted_path)

    doc = result.document
    markdown = _strip_model_artifacts(doc.export_to_markdown())
    picture_by_page = _collect_picture_text_chunks_by_page(doc)
    if ext == ".pdf" and settings.pdf_image_fallback_enabled:
        described_pages = {pg for pg, chunks in picture_by_page.items() if chunks}
        extra = describe_missing_pages(
            path,
            doclng_described_pages=described_pages,
            settings=settings,
        )
        for pg, chunks in extra.items():
            picture_by_page.setdefault(pg, []).extend(chunks)
    markdown = _append_picture_text_to_markdown(markdown, picture_by_page)
    document_dict = _clean_dict_strings(doc.export_to_dict())

    # Build per-page text from Docling provenance metadata so downstream
    # consumers can run entity extraction per page.
    pages_by_num: dict[int, list[str]] = {}
    try:
        for item in doc.texts:
            for p in item.prov:
                pages_by_num.setdefault(p.page_no, []).append(_strip_model_artifacts(item.text))
    except Exception:
        logger.debug("Could not extract per-page text from Docling document", exc_info=True)

    _merge_picture_text_into_pages(picture_by_page, pages_by_num)

    per_page_text = [
        {"page_no": pg, "text": "\n".join(texts)} for pg, texts in sorted(pages_by_num.items())
    ]

    num_pages = getattr(result, "num_pages", None)
    if num_pages is None and hasattr(doc, "pages"):
        try:
            num_pages = len(doc.pages) if doc.pages else None
        except Exception:
            num_pages = None

    stats = {
        "processing_time_s": round(conversion_time_s, 3),
        "num_pages": num_pages,
    }

    return {
        "document_json": document_dict,
        "markdown": markdown,
        "pages": per_page_text,
        "stats": stats,
    }
