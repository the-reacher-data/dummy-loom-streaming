"""HTTP fetch task for the async pod."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping

import httpx

from loom.streaming import Message, RecordStep

from smoke.models import ScrapeRequest, ScrapeResponse

_logger = logging.getLogger(__name__)
_DEFAULT_API_BASE = "https://dummyjson.com"


class FetchRequestTask(RecordStep[ScrapeRequest, ScrapeResponse]):
    """Async HTTP fetch for a single request.

    Called once per message, but executed concurrently across all messages
    by the ``WithAsync`` adapter.

    Args:
        timeout_s: Per-request HTTP timeout in seconds.
        api_base: Base URL used to resolve relative request paths.
    """

    def __init__(self, *, timeout_s: float = 30.0, api_base: str = _DEFAULT_API_BASE) -> None:
        self.timeout_s = timeout_s
        self.api_base = api_base.rstrip("/")

    async def execute(  # type: ignore[override]
        self,
        message: Message[ScrapeRequest],
        **kwargs: object,
    ) -> ScrapeResponse:
        client = kwargs["http"]
        if not isinstance(client, httpx.AsyncClient):
            raise TypeError(f"Expected httpx.AsyncClient, got {type(client).__name__}")
        request = message.payload
        _logger.info(
            "request_received request_id=%s kind=%s url=%s method=%s",
            request.request_id,
            request.kind,
            request.url,
            request.method,
        )
        url = _resolve_request_url(self.api_base, request.url, request.params)
        _logger.info(
            "fetch_start request_id=%s kind=%s url=%s method=%s",
            request.request_id,
            request.kind,
            url,
            request.method,
        )
        t0 = time.monotonic()
        response = await client.request(request.method, url, timeout=self.timeout_s)
        elapsed_ms = (time.monotonic() - t0) * 1000
        _logger.info(
            "fetch_done request_id=%s kind=%s url=%s status_code=%s elapsed_ms=%.2f",
            request.request_id,
            request.kind,
            url,
            response.status_code,
            elapsed_ms,
        )
        return ScrapeResponse(
            request_id=request.request_id,
            kind=request.kind,
            method=request.method,
            url=url,
            status_code=response.status_code,
            elapsed_ms=round(elapsed_ms, 2),
            body=response.text,
        )


class FetchRequestSyncTask(RecordStep[ScrapeRequest, ScrapeResponse]):
    """Sync HTTP fetch for a single request."""

    def __init__(self, *, timeout_s: float = 30.0, api_base: str = _DEFAULT_API_BASE) -> None:
        self.timeout_s = timeout_s
        self.api_base = api_base.rstrip("/")

    def execute(
        self,
        message: Message[ScrapeRequest],
        **kwargs: object,
    ) -> ScrapeResponse:
        client = kwargs["http"]
        if not isinstance(client, httpx.Client):
            raise TypeError(f"Expected httpx.Client, got {type(client).__name__}")
        request = message.payload
        _logger.info(
            "request_received request_id=%s kind=%s url=%s method=%s",
            request.request_id,
            request.kind,
            request.url,
            request.method,
        )
        url = _resolve_request_url(self.api_base, request.url, request.params)
        _logger.info(
            "fetch_start request_id=%s kind=%s url=%s method=%s",
            request.request_id,
            request.kind,
            url,
            request.method,
        )
        t0 = time.monotonic()
        response = client.request(request.method, url, timeout=self.timeout_s)
        elapsed_ms = (time.monotonic() - t0) * 1000
        _logger.info(
            "fetch_done request_id=%s kind=%s url=%s status_code=%s elapsed_ms=%.2f",
            request.request_id,
            request.kind,
            url,
            response.status_code,
            elapsed_ms,
        )
        return ScrapeResponse(
            request_id=request.request_id,
            kind=request.kind,
            method=request.method,
            url=url,
            status_code=response.status_code,
            elapsed_ms=round(elapsed_ms, 2),
            body=response.text,
        )


def _resolve_request_url(api_base: str, url: str, params: Mapping[str, str]) -> str:
    """Resolve a request URL against the configured API base."""
    rendered = url.format_map(params)
    if rendered.startswith(("http://", "https://")):
        return rendered
    return f"{api_base.rstrip('/')}/{rendered.lstrip('/')}"
