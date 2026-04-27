"""Data models for dummy streaming flows."""

from __future__ import annotations

from loom.core.model import LoomStruct


class PostId(LoomStruct):
    """Request payload carrying a post identifier to enrich."""

    post_id: int


class PostDetail(LoomStruct):
    """Enriched post detail produced by the flow."""

    user_id: int
    id: int
    title: str
    body: str
    source: str = ""


class ScrapeRequest(LoomStruct):
    """Seed or follow-up request carried between pods."""

    request_id: str
    mode: str
    post_id: int


class ScrapeResponse(LoomStruct):
    """Response message produced by the async pod."""

    request_id: str
    response_kind: str
    post_id: int
    item_ids: list[int]
    title: str = ""
