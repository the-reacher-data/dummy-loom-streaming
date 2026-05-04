"""Data models for dummy streaming flows."""

from __future__ import annotations

import msgspec

from loom.core.model import LoomStruct


class ScrapeRequest(LoomStruct):
    """Generic HTTP request carried between pods.

    The URL may be absolute or relative. When relative, the smoke tasks
    resolve it against the configured API base.
    """

    request_id: str
    kind: str
    url: str
    params: dict[str, str] = msgspec.field(default_factory=dict)
    method: str = "GET"


class ScrapeResponse(LoomStruct):
    """Raw HTTP response metadata produced by the async pod."""

    request_id: str
    kind: str
    method: str
    url: str
    status_code: int
    elapsed_ms: float
    body: str


class ProductReview(LoomStruct):
    """One review extracted from a product response."""

    product_id: int
    product_title: str
    rating: int
    comment: str
    reviewer_name: str
