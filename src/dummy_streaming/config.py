"""Shared smoke-flow configuration for dummy streaming pods."""

from __future__ import annotations

from loom.core.model import LoomFrozenStruct


class SmokeConfig(LoomFrozenStruct, frozen=True):
    """Configuration shared by the async, sync, and seed smoke pods."""

    api_base: str = "https://dummyjson.com"
    async_timeout_s: float = 30.0
    async_max_concurrency: int = 50
    seed_products_limit: int = 5
