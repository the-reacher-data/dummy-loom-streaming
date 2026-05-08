
"""Error mirror flow for the streaming smoke."""

from __future__ import annotations

from typing import Any

from loom.streaming import ErrorEnvelope, FromMultiTypeTopic, IntoTopic, Process, StreamFlow
from loom.streaming.kafka import DecodeError

from smoke.models import ScrapeRequest, ScrapeResponse
from smoke.tasks.error_flow import PrintErrorEnvelopeTask


error_flow: StreamFlow[
    ErrorEnvelope[ScrapeRequest] | ErrorEnvelope[ScrapeResponse] | DecodeError,
    ErrorEnvelope[Any],
] = StreamFlow(
    name="error_flow",
    source=FromMultiTypeTopic[
        ErrorEnvelope[ScrapeRequest] | ErrorEnvelope[ScrapeResponse] | DecodeError
        ](
            name="scrape.errors",
            payloads=(
                ErrorEnvelope[ScrapeRequest],
                ErrorEnvelope[ScrapeResponse],
                DecodeError,
            ),
        ),
        process=Process(
            PrintErrorEnvelopeTask,
            IntoTopic[ErrorEnvelope[Any]](name="scrape.errors.audit"),
        ),
    )
