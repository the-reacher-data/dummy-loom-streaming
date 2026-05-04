"""Async pod — CollectBatch + WithAsync over the requests topic."""

from __future__ import annotations

from loom.streaming import (
    CollectBatch,
    ErrorEnvelope,
    ErrorKind,
    FromTopic,
    IntoTopic,
    Process,
    ResourceScope,
    StreamFlow,
    WithAsync,
)

from dummy_streaming.httpx_factory import AsyncHttpxClientFactory
from dummy_streaming.models import ScrapeRequest, ScrapeResponse
from dummy_streaming.tasks.http_fetch import FetchRequestTask


async_scrape_flow: StreamFlow[ScrapeRequest, ScrapeResponse] = StreamFlow(
    name="async_smoke_flow",
    source=FromTopic[ScrapeRequest](
        name="scrape.requests",
        payload=ScrapeRequest,
    ),
    process=Process(
        CollectBatch(
            max_records=50,
            timeout_ms=2000,
        ),
        WithAsync(
            process=Process(
                FetchRequestTask.from_config("streaming.smoke.fetch_request"),
                IntoTopic[ScrapeResponse](
                    name="scrape.responses",
                    payload=ScrapeResponse,
                ),
            ),
            scope=ResourceScope.WORKER,
            http=AsyncHttpxClientFactory.from_config("httpx"),
            max_concurrency=50,
        ),
    ),
    errors={
        ErrorKind.WIRE: IntoTopic[ErrorEnvelope[ScrapeRequest]](
            name="scrape.errors",
            payload=ErrorEnvelope[ScrapeRequest],
        ),
        ErrorKind.ROUTING: IntoTopic[ErrorEnvelope[ScrapeRequest]](
            name="scrape.errors",
            payload=ErrorEnvelope[ScrapeRequest],
        ),
        ErrorKind.TASK: IntoTopic[ErrorEnvelope[ScrapeRequest]](
            name="scrape.errors",
            payload=ErrorEnvelope[ScrapeRequest],
        ),
        ErrorKind.BUSINESS: IntoTopic[ErrorEnvelope[ScrapeRequest]](
            name="scrape.errors",
            payload=ErrorEnvelope[ScrapeRequest],
        ),
    },
)
