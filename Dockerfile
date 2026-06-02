# syntax=docker/dockerfile:1.7
# SalesNest document-analysis: Docling SQS worker (CPU-only, no private PyPI).

FROM python:3.11-slim-bookworm AS builder

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY --link uv.lock pyproject.toml /app/
COPY --link src /app/src
WORKDIR /app

RUN curl -LsSf https://astral.sh/uv/0.8.22/install.sh | sh \
    && . /root/.local/bin/env \
    && uv sync --locked --no-dev

# Runtime stage
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    libcairo2 \
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && ldconfig

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --link src /app/src

RUN useradd --create-home --shell /bin/bash app \
    && mkdir -p /tmp/document-analysis /app/models \
    && chown app:app /tmp/document-analysis /app/models
USER app

ENTRYPOINT ["/app/.venv/bin/python", "-m", "document_analysis"]
