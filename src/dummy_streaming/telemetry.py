"""OpenTelemetry bootstrap helpers for dummy streaming smoke tests."""

from __future__ import annotations

import atexit
import os
from functools import lru_cache
from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Tracer

_DEFAULT_ENDPOINT = "http://otel-lgtm:4318/v1/traces"
_DEFAULT_SERVICE_NAME = "dummy-loom-streaming"


@lru_cache(maxsize=1)
def configure_tracing(service_name: str | None = None) -> None:
    """Configure a global OTEL tracer provider once.

    Args:
        service_name: Optional service name for resource attribution.
    """
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", _DEFAULT_ENDPOINT)
    resolved_service = service_name or os.environ.get("OTEL_SERVICE_NAME", _DEFAULT_SERVICE_NAME)
    provider = TracerProvider(resource=Resource.create({"service.name": resolved_service}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    atexit.register(provider.shutdown)


def get_tracer(name: str) -> Tracer:
    """Return a configured tracer for one module or flow."""
    configure_tracing()
    return trace.get_tracer(name)
