# Document Analysis Microservice - SQS worker
#
# Usage:
#   make run              - run worker via Docker (set SQS_QUEUE_URL)
#   make run-local-bridge - run Kafka→SQS bridge on host (needs KAFKA_*, SQS_QUEUE_URL)
#   make run-local-file   - process one file on disk (no SQS/S3). INPUT=path [OUTPUT_DIR=dir]
#   make install          - install dependencies (uv sync --extra dev)
#   make build            - build Docker image only
#
# Local file: INPUT=/path/to/doc.pdf [OUTPUT_DIR=/path/to/output] make run-local-file

.PHONY: install run run-local run-local-bridge run-local-file build help

help:
	@echo "Targets: run, run-local, run-local-bridge, run-local-file (INPUT=...), install, build"

install:
	uv sync --extra dev

build:
	docker compose build

build-verbose:
	DOCKER_BUILDKIT=1 docker compose build --progress=plain

run: build
	docker compose up

run-local:
	@mkdir -p ./tmp 2>/dev/null || true
	TEMP_DIR=./tmp ENTRYPOINT_MODE=worker uv run python -m document_analysis

run-local-bridge:
	@mkdir -p ./tmp 2>/dev/null || true
	TEMP_DIR=./tmp ENTRYPOINT_MODE=bridge uv run python -m document_analysis

run-local-file:
	@mkdir -p ./tmp 2>/dev/null || true
	@test -n "$(INPUT)" || (echo "Usage: INPUT=/path/to/doc.pdf make run-local-file" && exit 1)
	TEMP_DIR=./tmp LOCAL_INPUT_FILE="$(INPUT)" LOCAL_OUTPUT_DIR="$(OUTPUT_DIR)" uv run python -m document_analysis
