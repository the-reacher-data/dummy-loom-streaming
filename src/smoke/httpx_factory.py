"""Factories for configured HTTPX clients used by dummy streaming flows."""

from __future__ import annotations

import httpx

from loom.streaming import ContextFactory

from smoke.config import HttpxConfig


class AsyncHttpxClientFactory(ContextFactory):
    """Create configured ``httpx.AsyncClient`` instances from YAML-backed config."""

    __slots__ = ("_config",)

    def __init__(self, config: HttpxConfig) -> None:
        self._config = config
        super().__init__(self._create)

    def _create(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=httpx.Timeout(self._config.timeout_s),
            limits=httpx.Limits(
                max_connections=self._config.max_connections,
                max_keepalive_connections=self._config.max_keepalive_connections,
            ),
        )


class SyncHttpxClientFactory(ContextFactory):
    """Create configured ``httpx.Client`` instances from YAML-backed config."""

    __slots__ = ("_config",)

    def __init__(self, config: HttpxConfig) -> None:
        self._config = config
        super().__init__(self._create)

    def _create(self) -> httpx.Client:
        return httpx.Client(
            timeout=httpx.Timeout(self._config.timeout_s),
            limits=httpx.Limits(
                max_connections=self._config.max_connections,
                max_keepalive_connections=self._config.max_keepalive_connections,
            ),
        )
