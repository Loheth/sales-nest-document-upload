# SalesNest — Document Upload & Analysis

Convert uploaded documents (PDF, Word, PowerPoint, Excel, HTML, images) into structured JSON and Markdown using [Docling](https://github.com/docling-project/docling).

For **SalesNest**, the workflow you need day to day is **local file conversion** — no Kafka, SQS, or AWS required.

## What you get locally

For an input file `report.pdf`, the tool writes:

| Output | Contents |
|--------|----------|
| `report.document.json` | Docling document export plus `markdown` and `stats` (e.g. processing time, page count) |
| `report.document.md` | Plain Markdown version of the document |

If you set `LOCAL_OUTPUT_DIR`, both files go there. Otherwise they are written next to the input file.

## Prerequisites

- **Python 3.11** (3.12 is not supported yet)
- **[uv](https://docs.astral.sh/uv/)** — package manager (`pip install uv` or see uv docs)
- **~2 GB disk** for dependencies (Docling pulls in PyTorch and related packages on first install)

Optional for richer PDF handling:

- **RapidOCR models** under `tmp/models/rapidocr/` — needed for scanned PDFs (OCR). Without them, PDFs still work if they already contain selectable text.
- **SmolVLM / picture models** — only if you enable AI image descriptions (not required for basic local use)

## Quick start (Windows PowerShell)

From the repo root (`sales-nest-document-upload`):

```powershell
# 1. Install dependencies (first time; can take a few minutes)
uv sync --extra dev

# 2. Convert a document
$env:TEMP_DIR = ".\tmp"
$env:LOCAL_INPUT_FILE = "C:\path\to\your\document.pdf"
$env:LOCAL_OUTPUT_DIR = ".\tmp"
uv run python -m document_analysis
```

Check outputs in `.\tmp\`:

- `your\document.document.json`
- `your\document.document.md`

Use any supported file instead of `.pdf` (see [Supported formats](#supported-formats)).

### Quick start (macOS / Linux)

```bash
uv sync --extra dev

export TEMP_DIR=./tmp
export LOCAL_INPUT_FILE=/path/to/your/document.pdf
export LOCAL_OUTPUT_DIR=./tmp
uv run python -m document_analysis
```

Or with Make (Git Bash / WSL / macOS / Linux):

```bash
make install
INPUT=/path/to/document.pdf OUTPUT_DIR=./tmp make run-local-file
```

## Try the included sample

A small HTML sample is in `tmp/sample.html`:

```powershell
$env:TEMP_DIR = ".\tmp"
$env:LOCAL_INPUT_FILE = ".\tmp\sample.html"
$env:LOCAL_OUTPUT_DIR = ".\tmp"
uv run python -m document_analysis
```

You should see log lines ending with `Done.` and files `tmp\sample.document.json` and `tmp\sample.document.md`.

## Supported formats

| Type | Extensions |
|------|------------|
| PDF | `.pdf` |
| Word | `.docx`, `.doc` |
| PowerPoint | `.pptx`, `.ppt` |
| Excel | `.xlsx` |
| HTML | `.html`, `.htm` |
| Images | `.png`, `.jpg`, `.jpeg`, `.tiff`, `.tif`, `.bmp`, `.gif` |

## Local configuration

Only these matter for local runs:

| Variable | Example | Description |
|----------|---------|-------------|
| `LOCAL_INPUT_FILE` | `.\docs\contract.pdf` | **Required** — file to convert |
| `LOCAL_OUTPUT_DIR` | `.\tmp` | Optional — where to write `.document.json` / `.document.md` |
| `TEMP_DIR` | `.\tmp` | Scratch space (recommended on Windows instead of `/tmp/...`) |
| `LOG_LEVEL` | `debug` | More verbose logging |
| `MODEL_CACHE_DIR` | `.\tmp\models` | Folder for OCR / VLM models (optional) |

You can put them in a `.env` file in the repo root (loaded automatically):

```env
TEMP_DIR=./tmp
LOCAL_INPUT_FILE=./tmp/sample.html
LOCAL_OUTPUT_DIR=./tmp
LOG_LEVEL=info
```

### PDF tips (local)

- **Text-based PDFs** — work out of the box; first run may download Docling assets and feel slow.
- **Scanned PDFs** — need RapidOCR ONNX files in `MODEL_CACHE_DIR/rapidocr/` (`ch_PP-OCRv4_det_infer.onnx`, `ch_PP-OCRv4_rec_infer.onnx`). Without them, OCR is off and you only get embedded text (often empty on scans).
- **Image descriptions** — default backend is `local` (SmolVLM). If you do not have models under `MODEL_CACHE_DIR`, picture description is skipped; conversion still succeeds.

To disable Bedrock/picture extras explicitly for a lightweight run:

```powershell
$env:PICTURE_DESCRIPTION_BACKEND = "local"
$env:PDF_IMAGE_FALLBACK_ENABLED = "false"
```

## Verify everything works

```powershell
uv run pytest
uv run ruff check src/ tests/
```

Expected: **9 passed**, 1 skipped (Kafka event schema test — not needed for local file mode).

## Troubleshooting

| Problem | What to do |
|---------|------------|
| `LOCAL_INPUT_FILE is not a file` | Use an absolute path or path relative to the repo; check spelling and extension. |
| `Unsupported format` | See [Supported formats](#supported-formats). |
| First conversion very slow | Normal — Docling initializes pipelines and may download models. |
| PDF fails on Windows with `WinError 1314` / symlink | Fixed automatically when you run `uv run python -m document_analysis` (entrypoint patches Hugging Face cache). First PDF also downloads layout models (~30s+). |
| `ocr_options` / validation error | Fixed in this repo; pull latest or ensure `document_conversion.py` does not pass `ocr_options=None`. |
| Out of memory on huge PDFs | Try a smaller file first; tune `PICTURE_PAGE_RASTER_SCALE` (see `.env` / settings). |
| `make` not found on Windows | Use the PowerShell `uv run` commands above instead of Make. |

## Project layout (what matters locally)

```text
src/document_analysis/          # Application code
  main.py                       # Entry: local file, SQS worker, or Kafka bridge
  services/document_conversion.py
tmp/                            # Local temp + sample outputs (gitignored)
tests/                          # Unit tests
```

## What you do **not** need locally

You can ignore these until you deploy SalesNest to the cloud:

- Kafka / MSK (`KAFKA_BOOTSTRAP_SERVERS`)
- SQS (`SQS_QUEUE_URL`)
- Shared Kafka event schemas (only required for cloud worker/bridge modes)
- Terraform under `infrastructure/` (AWS deployment; not needed for local conversion)
- `scripts/run_e2e_test.py`, `scripts/run_smoke_test.py` (AWS + Kafka integration tests)
- Docker / ECS (optional; use local `uv run` instead)

## Development commands

```powershell
uv sync --extra dev
uv run pytest
uv run ruff check src/ tests/
uv run ruff format src/ tests/
```

## Production / cloud (later)

A full deployment uses Kafka → SQS → ECS workers → S3. That code remains in the repo (`ENTRYPOINT_MODE=bridge|worker`, `infrastructure/`, Docker). When SalesNest moves to AWS, configure SalesNest-specific buckets, queues, event schemas, and Terraform state in those folders.

For now, **if local conversion produces `.document.json` and `.document.md`, you are good.**
