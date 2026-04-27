"""Async smoke tasks for the two-pod streaming scenario."""

from __future__ import annotations

from collections.abc import Awaitable

import httpx

from loom.streaming.core._message import Message
from dummy_streaming.telemetry import get_tracer
from loom.streaming.nodes._step import RecordStep

from dummy_streaming.models import ScrapeRequest, ScrapeResponse

_API_BASE = "https://dummyjson.com"
_tracer = get_tracer("dummy_streaming.tasks.smoke_async")


class FetchCatalogCommentsTask(RecordStep[ScrapeRequest, ScrapeResponse]):
    """Fetch a post's comments and publish them as a catalog response."""

    def execute(
        self,
        message: Message[ScrapeRequest],
        **kwargs: object,
    ) -> Awaitable[ScrapeResponse]:
        client = kwargs["http"]
        if not isinstance(client, httpx.AsyncClient):
            raise TypeError(f"Expected httpx.AsyncClient, got {type(client).__name__}")
        with _tracer.start_as_current_span("fetch_catalog_comments") as span:
            span.set_attribute("scrape.request_id", message.payload.request_id)
            span.set_attribute("scrape.post_id", message.payload.post_id)
            span.set_attribute("scrape.mode", message.payload.mode)
            return self._fetch(message.payload, client)

    async def _fetch(
        self,
        request: ScrapeRequest,
        client: httpx.AsyncClient,
    ) -> ScrapeResponse:
        response = await client.get(
            f"{_API_BASE}/posts/{request.post_id}/comments",
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        item_ids = [comment["id"] for comment in data["comments"]]
        return ScrapeResponse(
            request_id=request.request_id,
            response_kind="catalog",
            post_id=request.post_id,
            item_ids=item_ids,
            title=f"{len(item_ids)} comments",
        )


class FetchScrapeResponseTask(RecordStep[ScrapeRequest, ScrapeResponse]):
    """Fetch a scrape response according to the request mode."""

    def execute(
        self,
        message: Message[ScrapeRequest],
        **kwargs: object,
    ) -> Awaitable[ScrapeResponse]:
        client = kwargs["http"]
        if not isinstance(client, httpx.AsyncClient):
            raise TypeError(f"Expected httpx.AsyncClient, got {type(client).__name__}")
        if message.payload.mode == "summary":
            return self._fetch_summary(message.payload, client)
        return self._fetch_catalog(message.payload, client)

    async def _fetch_catalog(
        self,
        request: ScrapeRequest,
        client: httpx.AsyncClient,
    ) -> ScrapeResponse:
        response = await client.get(
            f"{_API_BASE}/posts/{request.post_id}/comments",
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        item_ids = [comment["id"] for comment in data["comments"]]
        return ScrapeResponse(
            request_id=request.request_id,
            response_kind="catalog",
            post_id=request.post_id,
            item_ids=item_ids,
            title=f"{len(item_ids)} comments",
        )

    async def _fetch_summary(
        self,
        request: ScrapeRequest,
        client: httpx.AsyncClient,
    ) -> ScrapeResponse:
        response = await client.get(f"{_API_BASE}/posts/{request.post_id}", timeout=30.0)
        response.raise_for_status()
        data = response.json()
        return ScrapeResponse(
            request_id=request.request_id,
            response_kind="summary",
            post_id=request.post_id,
            item_ids=[request.post_id],
            title=data["title"],
        )


class FetchSummaryTask(RecordStep[ScrapeRequest, ScrapeResponse]):
    """Fetch a single post summary and publish it as a summary response."""

    def execute(
        self,
        message: Message[ScrapeRequest],
        **kwargs: object,
    ) -> Awaitable[ScrapeResponse]:
        client = kwargs["http"]
        if not isinstance(client, httpx.AsyncClient):
            raise TypeError(f"Expected httpx.AsyncClient, got {type(client).__name__}")
        with _tracer.start_as_current_span("fetch_catalog_comments") as span:
            span.set_attribute("scrape.request_id", message.payload.request_id)
            span.set_attribute("scrape.post_id", message.payload.post_id)
            span.set_attribute("scrape.mode", message.payload.mode)
            return self._fetch(message.payload, client)

    async def _fetch(
        self,
        request: ScrapeRequest,
        client: httpx.AsyncClient,
    ) -> ScrapeResponse:
        response = await client.get(f"{_API_BASE}/posts/{request.post_id}", timeout=30.0)
        response.raise_for_status()
        data = response.json()
        return ScrapeResponse(
            request_id=request.request_id,
            response_kind="summary",
            post_id=request.post_id,
            item_ids=[request.post_id],
            title=data["title"],
        )
