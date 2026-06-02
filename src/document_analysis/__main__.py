"""Run with ENTRYPOINT_MODE=worker|bridge, or LOCAL_INPUT_FILE for one-off local conversion."""

from document_analysis.runtime_env import (
    configure_docling_process_env_before_docling_import,
    configure_windows_hf_hub_cache_without_symlinks,
)

configure_windows_hf_hub_cache_without_symlinks()
configure_docling_process_env_before_docling_import()

from document_analysis.main import run

if __name__ == "__main__":
    run()
