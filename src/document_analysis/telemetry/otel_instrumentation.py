import logging
import os
import socket
from collections.abc import Generator
from contextlib import contextmanager

logger = logging.getLogger(__name__)

_initialized = False
_tracer_provider = None
_meter_provider = None
_logger_provider = None
_tracer = None
_jobs_processed = None
_jobs_failed = None
_job_duration = None
_sqs_gauges_registered = False


def _register_sqs_gauges(meter, environment: str) -> None:
    """Register ObservableGauge instruments that report SQS queue depth.

    Called once during OTel init. The callback queries SQS on every metric
    export cycle (~15s) and emits three gauges:
      - document_analysis_sqs_messages_visible
      - document_analysis_sqs_messages_in_flight
      - document_analysis_sqs_messages_dlq
    """
    global _sqs_gauges_registered
    if _sqs_gauges_registered:
        return

    from document_analysis.config.settings import get_settings

    settings = get_settings()
    queue_url = (settings.sqs_queue_url or "").strip()
    if not queue_url:
        logger.info("[OTel] SQS_QUEUE_URL not set — skipping SQS queue gauges")
        return

    region = settings.aws_default_region

    # Derive DLQ URL by convention: <queue-name>-dlq
    dlq_url = queue_url.rsplit("/", 1)
    dlq_url = f"{dlq_url[0]}/{dlq_url[1]}-dlq" if len(dlq_url) == 2 else ""

    import boto3

    sqs_client = boto3.client("sqs", region_name=region)

    common_attrs = {"deployment.environment": environment}

    def _observe_sqs_depth(_options):
        """Callback for ObservableGauge — returns current SQS depths."""
        from opentelemetry.metrics import Observation

        observations = []
        try:
            resp = sqs_client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                ],
            )
            attrs = resp.get("Attributes", {})
            visible = int(attrs.get("ApproximateNumberOfMessages", 0))
            in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
            observations.append(Observation(visible, {**common_attrs, "state": "visible"}))
            observations.append(Observation(in_flight, {**common_attrs, "state": "in_flight"}))
        except Exception as exc:
            logger.debug("SQS gauge poll failed (main queue): %s", exc)

        return observations

    def _observe_sqs_dlq(_options):
        """Callback for ObservableGauge — returns DLQ depth."""
        from opentelemetry.metrics import Observation

        if not dlq_url:
            return []
        try:
            resp = sqs_client.get_queue_attributes(
                QueueUrl=dlq_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            count = int(resp.get("Attributes", {}).get("ApproximateNumberOfMessages", 0))
            return [Observation(count, common_attrs)]
        except Exception as exc:
            logger.debug("SQS gauge poll failed (DLQ): %s", exc)
            return []

    meter.create_observable_gauge(
        "document_analysis_sqs_messages",
        callbacks=[_observe_sqs_depth],
        description="Current SQS queue message count by state (visible, in_flight)",
    )
    meter.create_observable_gauge(
        "document_analysis_sqs_dlq_messages",
        callbacks=[_observe_sqs_dlq],
        description="Current SQS dead-letter queue message count",
    )

    _sqs_gauges_registered = True
    logger.info("[OTel] SQS queue depth gauges registered")


def _read_cgroup_memory_limit_bytes() -> int | None:
    from pathlib import Path

    for path in (
        Path("/sys/fs/cgroup/memory.max"),
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    ):
        try:
            raw = path.read_text().strip()
            if raw == "max":
                continue
            limit = int(raw)
            if limit > 0:
                return limit
        except (OSError, ValueError):
            continue

    env_limit_mb = os.getenv("ECS_TASK_MEMORY_MB", "").strip()
    if env_limit_mb.isdigit():
        return int(env_limit_mb) * 1024 * 1024
    return None


def _read_process_rss_bytes() -> int | None:
    try:
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


def _register_task_memory_utilization_gauge(meter) -> None:
    limit_bytes = _read_cgroup_memory_limit_bytes()
    if limit_bytes is None:
        logger.info("[OTel] Task memory limit unavailable — skipping memory utilization gauge")
        return

    def _observe_task_memory_utilization(_options):
        from opentelemetry.metrics import Observation

        rss = _read_process_rss_bytes()
        if rss is None:
            return []
        return [Observation(min(rss / limit_bytes, 1.0))]

    meter.create_observable_gauge(
        "process.memory.utilization",
        callbacks=[_observe_task_memory_utilization],
        description="Process RSS as a fraction of the container/task memory limit",
        unit="1",
    )
    logger.info("[OTel] Task memory utilization gauge registered")


def init_opentelemetry() -> bool:
    """Initialize OpenTelemetry providers (traces, metrics, logs).

    Reads configuration from environment variables:
      OTEL_EXPORTER_OTLP_ENDPOINT  — base URL of the OTel Collector HTTP endpoint
                                      (e.g. http://localhost:4318). If not set,
                                      instrumentation is skipped gracefully.
      OTEL_SERVICE_NAME             — service name label (default: document-analysis)
      OTEL_SERVICE_VERSION          — service version label (default: 1.0.0)
      ENV                           — deployment.environment label (default: dev)

    Returns True if initialized successfully, False if skipped or failed.
    """
    global \
        _initialized, \
        _tracer_provider, \
        _meter_provider, \
        _logger_provider, \
        _tracer, \
        _jobs_processed, \
        _jobs_failed, \
        _job_duration

    if _initialized:
        return True

    os.environ.setdefault("OTEL_SEMCONV_STABILITY_OPT_IN", "http")

    collector_url = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").rstrip("/")
    if not collector_url:
        logger.info("[OTel] OTEL_EXPORTER_OTLP_ENDPOINT not set — skipping instrumentation")
        return False

    try:
        from opentelemetry import metrics, trace
        from opentelemetry._logs import set_logger_provider
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.logging import LoggingInstrumentor
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        service_name = os.getenv("OTEL_SERVICE_NAME", "document-analysis")
        service_version = os.getenv("OTEL_SERVICE_VERSION", "1.0.0")
        environment = os.getenv("ENV", "dev")

        resource = Resource.create(
            {
                SERVICE_NAME: service_name,
                SERVICE_VERSION: service_version,
                "service.instance.id": socket.gethostname(),
                "deployment.environment": environment,
            }
        )

        # --- Traces ---
        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{collector_url}/v1/traces"))
        )
        trace.set_tracer_provider(tracer_provider)
        _tracer_provider = tracer_provider
        _tracer = trace.get_tracer(service_name)

        # --- Metrics ---
        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(endpoint=f"{collector_url}/v1/metrics"),
                    export_interval_millis=15_000,
                )
            ],
        )
        metrics.set_meter_provider(meter_provider)
        _meter_provider = meter_provider
        meter = metrics.get_meter(service_name)

        from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor

        SystemMetricsInstrumentor().instrument()
        logger.info("[OTel] System metrics instrumented")

        _register_task_memory_utilization_gauge(meter)

        _jobs_processed = meter.create_counter(
            "document_analysis_jobs_processed_total",
            description="Total number of document analysis jobs processed",
        )
        _jobs_failed = meter.create_counter(
            "document_analysis_jobs_failed_total",
            description="Total number of document analysis jobs that failed",
        )
        _job_duration = meter.create_histogram(
            "document_analysis_job_duration_seconds",
            description="Duration of document analysis job processing in seconds",
            unit="s",
        )

        _register_sqs_gauges(meter, environment)

        # --- Logs ---
        logger_provider = LoggerProvider(resource=resource)
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(OTLPLogExporter(endpoint=f"{collector_url}/v1/logs"))
        )
        set_logger_provider(logger_provider)
        _logger_provider = logger_provider

        # Explicitly bridge Python stdlib logging → OTel log export
        handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
        logging.getLogger().addHandler(handler)

        LoggingInstrumentor().instrument(
            set_logging_format=True, enable_log_auto_instrumentation=False
        )

        _initialized = True
        logger.info(f"[OTel] Instrumentation initialized — exporting to {collector_url}")
        return True

    except Exception as exc:
        logger.warning(f"[OTel] Initialization failed — telemetry disabled: {exc}")
        return False


def shutdown_opentelemetry() -> None:
    """Flush and shut down all OTel providers. Call during process shutdown."""
    global _initialized
    if not _initialized:
        return

    for provider_name, provider in [
        ("traces", _tracer_provider),
        ("metrics", _meter_provider),
        ("logs", _logger_provider),
    ]:
        if provider is None:
            continue
        try:
            provider.shutdown()
        except Exception:
            logger.warning(f"[OTel] {provider_name} provider shutdown failed", exc_info=True)

    _initialized = False
    logger.info("[OTel] Providers shut down")


@contextmanager
def job_span(
    message_id: str, job_type: str = "document_analysis", facility_id: str | None = None
) -> Generator:
    """Context manager that wraps a job in an OTel span and records metrics.

    Usage:
        with job_span(message_id, facility_id=event.facility_id) as span:
            result = await process_document(job)

    Records:
        - A trace span for the job duration
        - Increments jobs_processed_total on success
        - Increments jobs_failed_total on exception
        - Records job_duration_seconds histogram
    """
    import time

    if not _initialized or _tracer is None:
        yield None
        return

    start = time.perf_counter()
    span_attrs: dict = {"message.id": message_id, "job.type": job_type}
    metric_attrs: dict = {"job.type": job_type}
    if facility_id:
        span_attrs["facility.id"] = facility_id
        metric_attrs["facility.id"] = facility_id
    with _tracer.start_as_current_span(
        f"{job_type}.process",
        attributes=span_attrs,
    ) as span:
        try:
            yield span
            elapsed = time.perf_counter() - start
            if _jobs_processed:
                _jobs_processed.add(1, {**metric_attrs, "status": "success"})
            if _job_duration:
                _job_duration.record(elapsed, {**metric_attrs, "status": "success"})
        except Exception as exc:
            elapsed = time.perf_counter() - start
            if span:
                from opentelemetry.trace import StatusCode

                span.set_status(StatusCode.ERROR, str(exc))
                span.record_exception(exc)
            if _jobs_failed:
                _jobs_failed.add(1, {**metric_attrs, "error": type(exc).__name__})
            if _job_duration:
                _job_duration.record(elapsed, {**metric_attrs, "status": "error"})
            raise
