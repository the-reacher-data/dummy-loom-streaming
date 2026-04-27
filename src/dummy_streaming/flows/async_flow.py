"""Async smoke flow — consume requests from Redpanda and publish responses to Kafka."""

from __future__ import annotations

import httpx

from loom.streaming.graph._flow import Process, StreamFlow
from loom.streaming.nodes._boundary import FromTopic, IntoTopic
from loom.streaming.nodes._with import ContextFactory, ResourceScope, WithAsync

from dummy_streaming.models import ScrapeRequest, ScrapeResponse
from dummy_streaming.tasks.smoke_async import FetchScrapeResponseTask

_async_client_factory = ContextFactory(lambda: httpx.AsyncClient())

async_scrape_flow = StreamFlow(
    name="async_smoke_flow",
    source=FromTopic[ScrapeRequest](
        name="scrape.requests",
        payload=ScrapeRequest,
    ),
    process=Process(
        WithAsync(
            step=FetchScrapeResponseTask(),
            scope=ResourceScope.WORKER,
            http=_async_client_factory,
        ),
        IntoTopic[ScrapeResponse](
            name="scrape.responses",
            payload=ScrapeResponse,
        ),
    ),
)
