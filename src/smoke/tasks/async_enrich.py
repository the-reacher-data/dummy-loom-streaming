"""Async enrich task — RecordStep whose execute returns an awaitable.

When driven by :class:`loom.streaming.nodes.WithAsync`, Bytewax will
await the returned coroutine concurrently (up to ``max_concurrency``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable

from loom.streaming import Message, RecordStep

from smoke.models import PostDetail, PostId


class AsyncEnrichTask(RecordStep[PostId, PostDetail]):
    """Asynchronous enrichment: PostId → PostDetail (awaitable)."""

    def execute(
        self,
        message: Message[PostId],
        **kwargs: object,
    ) -> Awaitable[PostDetail]:
        return self._enrich(message.payload.post_id)

    async def _enrich(self, pid: int) -> PostDetail:
        await asyncio.sleep(0.01)
        return PostDetail(
            user_id=pid,
            id=pid,
            title=f"AsyncTitle {pid}",
            body=f"AsyncBody {pid}",
            source="async",
        )
