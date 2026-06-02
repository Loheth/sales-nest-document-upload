# syntax=docker/dockerfile:1.7
# document-analysis-microservice: Docling SQS worker
# Python 3.11, uv, CPU-only (no CUDA).

FROM python:3.11-slim-bookworm AS builder

# awscli installed here only — runtime copies only /app/.venv.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl unzip \
    && curl -LsSf "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip \
    && unzip -q /tmp/awscliv2.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/awscliv2.zip /tmp/aws /var/lib/apt/lists/*

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

ARG PYPI_BUCKET=flash-pypi-dev
ARG AWS_REGION=us-gov-west-1

COPY --link uv.lock pyproject.toml /app/
COPY --link src /app/src
WORKDIR /app

RUN --mount=type=secret,id=aws_credentials,target=/root/.aws/credentials \
    curl -LsSf https://astral.sh/uv/0.8.22/install.sh | sh \
    && . /root/.local/bin/env \
    && mkdir -p /tmp/wheels \
    && aws s3 sync "s3://${PYPI_BUCKET}/simple/flash-event-schemas/" /tmp/wheels/ \
         --region "${AWS_REGION}" --exclude "*.html" \
    && uv sync --locked --no-dev --no-install-project \
    && uv pip install --python /app/.venv/bin/python \
         --find-links /tmp/wheels \
         --extra-index-url https://pypi.org/simple/ \
         "flash-event-schemas==0.5.3" \
    && rm -rf /tmp/wheels

# Runtime stage
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1-mesa-glx \
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
