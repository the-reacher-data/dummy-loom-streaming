"""Async scrape task — RecordStep whose execute returns an awaitable for WithAsync."""

from __future__ import annotations

from collections.abc import Awaitable

import httpx

from loom.streaming import Message, RecordStep

from smoke.models import PostDetail, PostId

_API_BASE = "https://jsonplaceholder.typicode.com"


class AsyncScrapeTask(RecordStep[PostId, PostDetail]):
    """Fetch one post detail via asynchronous HTTP GET.

    Expects an ``httpx.AsyncClient`` to be injected via
    ``WithAsync(..., http=factory)``. ``execute`` returns an awaitable so
    that :class:`loom.streaming.nodes.WithAsync` can drive it concurrently.
    """

    def execute(
        self,
        message: Message[PostId],
        **kwargs: object,
    ) -> Awaitable[PostDetail]:
        client = kwargs["http"]
        if not isinstance(client, httpx.AsyncClient):
            raise TypeError(f"Expected httpx.AsyncClient, got {type(client).__name__}")
        return self._fetch(message.payload.post_id, client)

    async def _fetch(self, post_id: int, client: httpx.AsyncClient) -> PostDetail:
        response = await client.get(
            f"{_API_BASE}/posts/{post_id}",
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        return PostDetail(
            user_id=data["userId"],
            id=data["id"],
            title=data["title"],
            body=data["body"],
            source="async",
        )
