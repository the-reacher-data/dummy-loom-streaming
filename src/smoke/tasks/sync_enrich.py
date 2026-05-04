"""Sync enrich task — simple RecordStep transformation."""

from __future__ import annotations

from loom.streaming import Message, RecordStep

from smoke.models import PostDetail, PostId


class SyncEnrichTask(RecordStep[PostId, PostDetail]):
    """Synchronous enrichment: PostId → PostDetail."""

    def execute(self, message: Message[PostId], **kwargs: object) -> PostDetail:
        pid = message.payload.post_id
        return PostDetail(
            user_id=pid,
            id=pid,
            title=f"Title {pid}",
            body=f"Body {pid}",
            source="sync",
        )
