#!/usr/bin/env bash
# Local dev setup. Prereqs: Python 3.11, uv, AWS CLI v2, flash-pypi-dev read.

set -euo pipefail

PYPI_BUCKET="${PYPI_BUCKET:-flash-pypi-dev}"
AWS_REGION="${AWS_REGION:-us-gov-west-1}"
WHEELS_DIR="${WHEELS_DIR:-.wheels}"

echo "==> Syncing wheels from s3://${PYPI_BUCKET}/simple/flash-event-schemas/"
mkdir -p "${WHEELS_DIR}"
aws s3 sync "s3://${PYPI_BUCKET}/simple/flash-event-schemas/" "${WHEELS_DIR}/" \
  --region "${AWS_REGION}" --exclude "*.html"

echo "==> uv sync"
uv sync --no-install-project

echo "==> Installing flash-event-schemas from local wheel"
uv pip install \
  --find-links "${WHEELS_DIR}" \
  --extra-index-url https://pypi.org/simple/ \
  "flash-event-schemas==0.5.3"

echo ""
echo "Done."
