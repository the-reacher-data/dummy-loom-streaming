"""Error mirror flow for the streaming smoke."""

from __future__ import annotations

from typing import Any

from loom.streaming import (
    ErrorEnvelope,
    ErrorKind,
    Fork,
    FromTopic,
    IntoTopic,
    Process,
    StreamFlow,
    payload,
)

from smoke.tasks.error_flow import PrintBusinessErrorTask, PrintErrorEnvelopeTask


error_flow: StreamFlow[ErrorEnvelope[Any], ErrorEnvelope[Any]] = StreamFlow(
    name="error_flow",
    source=FromTopic[ErrorEnvelope[Any]](
        name="scrape.errors",
        payload=ErrorEnvelope[Any],
    ),
    process=Process(
        Fork.by(
            selector=payload.kind,
            branches={
                ErrorKind.BUSINESS: Process(
                    PrintBusinessErrorTask,
                    IntoTopic[ErrorEnvelope[Any]](
                        name="scrape.errors.audit",
                        payload=ErrorEnvelope[Any],
                    ),
                ),
            },
            default=Process(
                PrintErrorEnvelopeTask,
                IntoTopic[ErrorEnvelope[Any]](
                    name="scrape.errors.audit",
                    payload=ErrorEnvelope[Any],
                ),
            ),
        ),
    ),
)
