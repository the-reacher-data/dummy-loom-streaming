"""Sync scrape task — RecordStep that calls JSONPlaceholder API."""

from __future__ import annotations

import httpx

from loom.streaming import Message, RecordStep

from dummy_streaming.models import PostDetail, PostId

_API_BASE = "https://jsonplaceholder.typicode.com"


class SyncScrapeTask(RecordStep[PostId, PostDetail]):
    """Fetch one post detail via synchronous HTTP GET.

    Expects an ``httpx.Client`` to be injected via ``With(..., http=factory)``.
    """

    def execute(self, message: Message[PostId], **kwargs: object) -> PostDetail:
        client = kwargs["http"]
        if not isinstance(client, httpx.Client):
            raise TypeError(f"Expected httpx.Client, got {type(client).__name__}")

        post_id = message.payload.post_id
        response = client.get(f"{_API_BASE}/posts/{post_id}", timeout=30.0)
        response.raise_for_status()
        data = response.json()
        return PostDetail(
            user_id=data["userId"],
            id=data["id"],
            title=data["title"],
            body=data["body"],
            source="sync",
        )
