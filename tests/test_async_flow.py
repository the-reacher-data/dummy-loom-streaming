"""Test the async smoke flow and its async fetch tasks."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import httpx
from omegaconf import OmegaConf

from loom.streaming.compiler import compile_flow
from loom.streaming.core._message import Message, MessageMeta

from dummy_streaming.flows.async_flow import async_scrape_flow
from dummy_streaming.models import ScrapeRequest
from dummy_streaming.tasks.smoke_async import FetchCatalogCommentsTask, FetchSummaryTask


def test_fetch_catalog_comments_task_with_mock_client() -> None:
    """FetchCatalogCommentsTask should emit a catalog response with comment ids."""
    task = FetchCatalogCommentsTask()
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "comments": [
            {"id": 11},
            {"id": 12},
            {"id": 13},
        ]
    }
    mock_response.raise_for_status.return_value = None
    mock_client = MagicMock(spec=httpx.AsyncClient)
    mock_client.get = AsyncMock(return_value=mock_response)

    msg = Message(
        payload=ScrapeRequest(request_id="seed-catalog", mode="catalog", post_id=1),
        meta=MessageMeta(message_id="test-1"),
    )
    result = asyncio.run(task.execute(msg, http=mock_client))

    assert result.request_id == "seed-catalog"
    assert result.response_kind == "catalog"
    assert result.post_id == 1
    assert result.item_ids == [11, 12, 13]
    assert result.title == "3 comments"
    mock_client.get.assert_called_once_with(
        "https://dummyjson.com/posts/1/comments",
        timeout=30.0,
    )


def test_fetch_summary_task_with_mock_client() -> None:
    """FetchSummaryTask should emit a summary response for one post."""
    task = FetchSummaryTask()
    mock_response = MagicMock()
    mock_response.json.return_value = {"title": "Example title"}
    mock_response.raise_for_status.return_value = None
    mock_client = MagicMock(spec=httpx.AsyncClient)
    mock_client.get = AsyncMock(return_value=mock_response)

    msg = Message(
        payload=ScrapeRequest(request_id="seed-summary", mode="summary", post_id=2),
        meta=MessageMeta(message_id="test-2"),
    )
    result = asyncio.run(task.execute(msg, http=mock_client))

    assert result.request_id == "seed-summary"
    assert result.response_kind == "summary"
    assert result.post_id == 2
    assert result.item_ids == [2]
    assert result.title == "Example title"
    mock_client.get.assert_called_once_with(
        "https://dummyjson.com/posts/2",
        timeout=30.0,
    )


def test_async_smoke_flow_compiles() -> None:
    """Async smoke flow should compile and request an async bridge."""
    cfg = OmegaConf.create(
        {
            "kafka": {
                "producer": {
                    "brokers": ["kafka:29092"],
                    "client_id": "test-producer",
                },
                "consumer": {
                    "brokers": ["redpanda:9092"],
                    "group_id": "test-consumer",
                    "topics": ["scrape.requests"],
                },
            }
        }
    )
    plan = compile_flow(async_scrape_flow, runtime_config=cfg)
    assert plan.name == "async_smoke_flow"
    assert len(plan.nodes) == 2
    assert plan.needs_async_bridge is True
