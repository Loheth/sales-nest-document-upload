"""Smoke tests: ensure package and key modules import."""


def test_document_analysis_imports() -> None:
    """Import main package and config."""
    import document_analysis  # noqa: F401
    from document_analysis.config.settings import get_settings

    settings = get_settings()
    assert settings.temp_dir
    assert settings.aws_default_region


def test_job_from_kafka_event() -> None:
    """DocumentJob from Kafka event."""
    from document_analysis.job import DocumentJob

    class _FakeEvent:
        s3_bucket = "my-bucket"
        s3_key = "evidence/doc.pdf"
        output_key_prefix = "evidence/doc"

    job = DocumentJob.from_kafka_event(_FakeEvent())
    assert job.bucket == "my-bucket"
    assert job.key == "evidence/doc.pdf"
    assert job.output_bucket == "my-bucket"
    assert job.output_key_prefix == "evidence/doc"


def test_is_supported() -> None:
    """Supported and unsupported extensions."""
    from document_analysis.services.document_conversion import is_supported

    assert is_supported("/tmp/x.pdf") is True
    assert is_supported("/tmp/x.docx") is True
    assert is_supported("/tmp/x.xlsx") is True
