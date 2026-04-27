"""Sync smoke flow — consume responses from Kafka and fan out requests back to Redpanda."""

from __future__ import annotations

from loom.streaming.graph._flow import Process, StreamFlow
from loom.streaming.nodes._boundary import FromTopic, IntoTopic
from loom.streaming.nodes._fork import Fork, ForkRoute
from loom.streaming.nodes._shape import Drain

from dummy_streaming.models import ScrapeRequest, ScrapeResponse
from dummy_streaming.tasks.smoke_sync import (
    FanOutFollowupRequestsStep,
    PrintResponseTask,
    UppercaseRequestIdTask,
)

sync_scrape_flow = StreamFlow(
    name="sync_smoke_flow",
    source=FromTopic[ScrapeResponse](
        name="scrape.responses",
        payload=ScrapeResponse,
    ),
    process=Process(
        Fork.when(
            routes=(
                ForkRoute(
                    when=lambda msg: msg.payload.response_kind == "catalog",
                    process=Process(
                        UppercaseRequestIdTask(),
                        PrintResponseTask(),
                        FanOutFollowupRequestsStep(),
                        IntoTopic[ScrapeRequest](
                            name="scrape.requests",
                            payload=ScrapeRequest,
                        ),
                    ),
                ),
                ForkRoute(
                    when=lambda msg: msg.payload.response_kind == "summary",
                    process=Process(
                        UppercaseRequestIdTask(),
                        PrintResponseTask(),
                        Drain(),
                    ),
                ),
            ),
            default=Process(Drain()),
        ),
    ),
)
