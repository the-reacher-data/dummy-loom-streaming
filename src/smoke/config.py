"""Shared smoke-flow configuration for dummy streaming pods."""

from __future__ import annotations

from loom.core.model import LoomFrozenStruct


class SmokeConfig(LoomFrozenStruct, frozen=True):
    """Configuration shared by the async, sync, and seed smoke pods."""

    api_base: str = "https://dummyjson.com"
    async_timeout_s: float = 5.0
    async_max_concurrency: int = 50
    async_batch_max_records: int = 50
    async_batch_timeout_ms: int = 2000
    seed_products_limit: int = 5


class HttpxConfig(LoomFrozenStruct, frozen=True):
    """Configuration for the async HTTPX client factory."""

    timeout_s: float = 30.0
    max_connections: int = 50
    max_keepalive_connections: int = 20
