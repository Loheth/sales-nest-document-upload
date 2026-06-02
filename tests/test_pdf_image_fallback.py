"""Tests for pikepdf PDF image fallback."""

from __future__ import annotations

from pathlib import Path

import pikepdf
import pytest
from PIL import Image

from document_analysis.config.settings import Settings
from document_analysis.services import pdf_image_fallback as fb


def _pdf_one_page_two_images(path: Path, sizes: tuple[tuple[int, int], tuple[int, int]]) -> None:
    pdf = pikepdf.Pdf.new()
    page = pdf.add_blank_page(page_size=(612, 792))
    page["/Resources"] = pikepdf.Dictionary(XObject=pikepdf.Dictionary())
    for idx, (w, h) in enumerate(sizes):
        img = Image.new("RGB", (w, h), color=(idx * 80, 100, 120))
        pikepdf.PdfImage._from_pil_image(pdf=pdf, page=page, name=f"/Im{idx}", image=img)
    pdf.save(path)


def test_iter_pdf_page_images_filters_small_edge(tmp_path: Path) -> None:
    pdf_path = tmp_path / "tiny.pdf"
    _pdf_one_page_two_images(pdf_path, ((50, 50), (512, 512)))

    settings = Settings()
    settings.pdf_image_fallback_min_long_edge_px = 256

    pages = list(fb.iter_pdf_page_images(pdf_path, settings))
    assert len(pages) == 1
    pg_no, pil = pages[0]
    assert pg_no == 1
    assert pil.size == (512, 512)


def test_describe_missing_pages_skips_already_described(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pdf_path = tmp_path / "skip.pdf"
    _pdf_one_page_two_images(pdf_path, ((512, 512), (600, 600)))

    calls: list[int] = []

    def _fake(img: Image.Image, *, prompt: str, settings: Settings):
        calls.append(1)
        return "dummy", 10, "stop"

    monkeypatch.setattr(fb, "describe_image_via_bedrock_with_usage", _fake)

    settings = Settings()
    settings.pdf_image_fallback_enabled = True
    settings.picture_description_backend = "bedrock"
    settings.pdf_image_fallback_min_long_edge_px = 256
    settings.picture_description_api_concurrency = 4

    out = fb.describe_missing_pages(
        pdf_path,
        doclng_described_pages={1},
        settings=settings,
    )
    assert out == {}
    assert calls == []


def test_describe_missing_pages_per_page_cap_and_bedrock_mock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pdf_path = tmp_path / "cap.pdf"
    _pdf_one_page_two_images(pdf_path, ((512, 512), (640, 640)))

    calls: list[int] = []

    def _fake(img: Image.Image, *, prompt: str, settings: Settings):
        calls.append(img.size[0] * img.size[1])
        return "caption-text", 3, "stop"

    monkeypatch.setattr(fb, "describe_image_via_bedrock_with_usage", _fake)

    settings = Settings()
    settings.pdf_image_fallback_enabled = True
    settings.picture_description_backend = "bedrock"
    settings.pdf_image_fallback_min_long_edge_px = 256
    settings.pdf_image_fallback_max_images_per_page = 1
    settings.picture_description_api_concurrency = 2

    out = fb.describe_missing_pages(pdf_path, doclng_described_pages=set(), settings=settings)

    assert len(calls) == 1
    assert calls[0] == 640 * 640
    assert 1 in out
    assert len(out[1]) == 1
    assert "[Image]\ncaption-text" in out[1][0]


def test_describe_missing_pages_local_smolvlm_mock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pdf_path = tmp_path / "local.pdf"
    _pdf_one_page_two_images(pdf_path, ((512, 512), (640, 640)))

    batches: list[int] = []

    class _Fake:
        def _annotate_images(self, images):
            lst = list(images)
            batches.append(len(lst))
            for i, _im in enumerate(lst):
                yield f"desc-{i}"

    monkeypatch.setattr(fb, "get_local_fallback_vlm_model", lambda _settings: _Fake())

    settings = Settings()
    settings.pdf_image_fallback_enabled = True
    settings.picture_description_backend = "local"
    settings.pdf_image_fallback_min_long_edge_px = 256
    settings.pdf_image_fallback_max_images_per_page = 2
    settings.picture_description_vlm_batch_size = 2

    out = fb.describe_missing_pages(pdf_path, doclng_described_pages=set(), settings=settings)
    assert batches == [2]
    assert 1 in out
    assert len(out[1]) == 2


def test_describe_missing_pages_disabled_returns_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pdf_path = tmp_path / "off.pdf"
    _pdf_one_page_two_images(pdf_path, ((512, 512), (640, 640)))

    def _fake(*args, **kwargs):
        raise AssertionError("should not call Bedrock when disabled")

    monkeypatch.setattr(fb, "describe_image_via_bedrock_with_usage", _fake)

    settings = Settings()
    settings.pdf_image_fallback_enabled = False

    assert (
        fb.describe_missing_pages(pdf_path, doclng_described_pages=set(), settings=settings) == {}
    )
