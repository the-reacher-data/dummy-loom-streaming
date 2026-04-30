"""Shared fixtures for dummy streaming tests."""

from __future__ import annotations

import httpx
import pytest
from omegaconf import DictConfig, OmegaConf


@pytest.fixture(scope="session")
def smoke_config() -> DictConfig:
    """Return the shared runtime config used by the dummy smoke flows."""
    return OmegaConf.create(
        {
            "kafka": {
                "producer": {
                    "brokers": ["redpanda:9092"],
                    "client_id": "test-producer",
                    "topic": "scrape.responses",
                },
                "consumer": {
                    "brokers": ["redpanda:9092"],
                    "group_id": "test-consumer",
                    "topics": ["scrape.requests"],
                },
            },
            "logging": {
                "level": "info",
            },
            "httpx": {
                "config": {
                    "timeout_s": 30.0,
                    "max_connections": 50,
                    "max_keepalive_connections": 20,
                },
            },
                "streaming": {
                    "smoke": {
                        "api_base": "https://dummyjson.com",
                        "async_timeout_s": 30.0,
                        "async_max_concurrency": 50,
                        "async_batch_max_records": 2,
                        "async_batch_timeout_ms": 500,
                        "seed_products_limit": 5,
                        "expand_product_catalog": {
                            "api_base": "https://dummyjson.com",
                        },
                        "fetch_request": {
                            "api_base": "https://dummyjson.com",
                            "timeout_s": 30.0,
                        },
                    "expand_categories": {
                        "api_base": "https://dummyjson.com",
                    },
                    "expand_category_products": {
                        "api_base": "https://dummyjson.com",
                    },
                }
            },
        }
    )


@pytest.fixture
def sync_http_transport() -> httpx.MockTransport:
    """Return a MockTransport for sync httpx clients."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/products/1":
            return httpx.Response(
                200,
                json={
                    "userId": 11,
                    "id": 1,
                    "title": "Example product",
                    "body": "Example body",
                },
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    return httpx.MockTransport(handler)


@pytest.fixture
def async_http_transport() -> httpx.MockTransport:
    """Return a MockTransport for async httpx clients."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/products/1":
            return httpx.Response(200, text='{"id": 1}')
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    return httpx.MockTransport(handler)
