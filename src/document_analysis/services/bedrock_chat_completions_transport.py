"""Bedrock OpenAI-compatible Chat Completions for vision (SigV4 or Bearer)."""

from __future__ import annotations

import base64
import json
import logging
from io import BytesIO

import boto3
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from docling.datamodel.base_models import OpenAiApiResponse, VlmStopReason
from PIL import Image
from pydantic import AnyUrl

from document_analysis.config.settings import Settings

logger = logging.getLogger(__name__)

_PATCH_INSTALLED = False


def post_chat_completions_sigv4(
    *,
    url: str,
    body_json: str,
    region: str,
    timeout: float,
) -> requests.Response:
    """POST JSON to Bedrock Chat Completions with AWS SigV4 (task-role / ambient credentials)."""
    session = boto3.Session(region_name=region)
    credentials = session.get_credentials()
    if credentials is None:
        raise RuntimeError(
            "No AWS credentials available for SigV4 Bedrock Chat Completions requests"
        )

    body_bytes = body_json.encode("utf-8")
    aws_req = AWSRequest(
        method="POST",
        url=url,
        data=body_bytes,
        headers={"Content-Type": "application/json"},
    )
    SigV4Auth(credentials, "bedrock", region).add_auth(aws_req)

    prepared_headers = dict(aws_req.headers)
    return requests.post(
        url,
        data=body_bytes,
        headers=prepared_headers,
        timeout=timeout,
    )


def describe_image_via_bedrock_with_usage(
    image: Image.Image,
    *,
    prompt: str,
    settings: Settings,
    timeout: float | None = None,
    payload_extra: dict[str, object] | None = None,
    request_headers_extra: dict[str, str] | None = None,
) -> tuple[str | None, int | None, str | None]:
    """Multimodal Chat Completions for one PIL image.

    Returns ``(text, total_tokens, finish_reason)`` where ``finish_reason`` is the OpenAI-style
    ``finish_reason`` string when parsing succeeded.
    """
    img_io = BytesIO()
    pil = image.copy()
    pil = pil.convert("RGBA")
    try:
        pil.save(img_io, "PNG")
    except Exception as e:
        logger.error("Could not PNG-encode image for Bedrock size=%s: %s", pil.size, e)
        return None, None, None

    image_base64 = base64.b64encode(img_io.getvalue()).decode("utf-8")
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{image_base64}"},
                },
                {"type": "text", "text": prompt},
            ],
        }
    ]

    params: dict[str, object] = {
        "model": settings.picture_description_bedrock_model_id.strip(),
        "max_tokens": settings.picture_description_bedrock_max_tokens,
        "temperature": settings.picture_description_bedrock_temperature,
    }
    if payload_extra:
        params.update(payload_extra)

    payload = {"messages": messages, **params}
    timeout_s = timeout if timeout is not None else settings.picture_description_api_timeout
    url = settings.picture_description_bedrock_chat_url_resolved
    region = settings.picture_description_bedrock_region_resolved

    try:
        if settings.picture_description_bedrock_use_bearer_auth:
            tok = settings.picture_description_bedrock_bearer_token
            if tok is None:
                logger.error(
                    "Bearer auth selected but picture_description_bedrock_bearer_token is unset"
                )
                return None, None, None
            hdrs: dict[str, str] = {
                "Authorization": f"Bearer {tok.get_secret_value().strip()}",
                "Content-Type": "application/json",
            }
            if request_headers_extra:
                hdrs.update(request_headers_extra)
            r = requests.post(url, headers=hdrs, json=payload, timeout=timeout_s)
        else:
            if request_headers_extra:
                logger.warning(
                    "Ignoring extra headers on SigV4 Bedrock path keys=%s",
                    list(request_headers_extra.keys()),
                )
            body_json = json.dumps(payload, ensure_ascii=False)
            r = post_chat_completions_sigv4(
                url=url,
                body_json=body_json,
                region=region,
                timeout=timeout_s,
            )

        if not r.ok:
            logger.error(
                "Bedrock Chat Completions HTTP error status=%s body=%s", r.status_code, r.text
            )
            return None, None, None

        api_resp = OpenAiApiResponse.model_validate_json(r.text)
        raw_content = api_resp.choices[0].message.content or ""
        text = raw_content.strip() or None
        num_tokens = api_resp.usage.total_tokens
        finish_reason = api_resp.choices[0].finish_reason
        return text, num_tokens, finish_reason
    except Exception as e:
        logger.error("Bedrock Chat Completions request failed: %s", e)
        return None, None, None


def describe_image_via_bedrock(
    image: Image.Image,
    *,
    prompt: str,
    settings: Settings,
    timeout: float | None = None,
    payload_extra: dict[str, object] | None = None,
) -> str | None:
    """Return model text only (convenience wrapper)."""
    text, _, _ = describe_image_via_bedrock_with_usage(
        image,
        prompt=prompt,
        settings=settings,
        timeout=timeout,
        payload_extra=payload_extra,
    )
    return text


def api_image_request_sigv4(
    image: Image.Image,
    prompt: str,
    url: AnyUrl,
    timeout: float = 20,
    headers: dict[str, str] | None = None,
    **params: object,
) -> tuple[str, int | None, VlmStopReason]:
    """Docling-compatible tuple; SigV4 path ignores URL params in favor of ``get_settings()``."""

    from document_analysis.config.settings import get_settings

    settings = get_settings()
    text, num_tokens, finish_reason = describe_image_via_bedrock_with_usage(
        image,
        prompt=prompt,
        settings=settings,
        timeout=timeout,
        payload_extra=dict(params) if params else None,
        request_headers_extra=headers,
    )
    if not text:
        return "", num_tokens if num_tokens is not None else 0, VlmStopReason.UNSPECIFIED
    stop_reason = (
        VlmStopReason.LENGTH if finish_reason == "length" else VlmStopReason.END_OF_SEQUENCE
    )
    return text, num_tokens, stop_reason


def ensure_docling_api_image_request_uses_sigv4_for_bedrock() -> None:
    """Replace Docling's ``api_image_request`` once so picture-description uses IAM SigV4."""
    global _PATCH_INSTALLED
    if _PATCH_INSTALLED:
        return

    import docling.utils.api_image_request as api_mod

    api_mod.api_image_request = api_image_request_sigv4  # type: ignore[assignment]
    _PATCH_INSTALLED = True
