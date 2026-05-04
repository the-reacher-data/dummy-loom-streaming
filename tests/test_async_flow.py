"""Test the probe flow and its sync fetch tasks."""

from __future__ import annotations

import httpx
from omegaconf import DictConfig

from loom.streaming import Message, MessageMeta, compile_flow

from dummy_streaming.flows.async_flow import async_scrape_flow
from dummy_streaming.models import ScrapeRequest
from dummy_streaming.tasks.http_fetch import FetchRequestSyncTask


class TestAsyncFlow:
    """Probe flow and sync task behavior."""

    def test_fetch_request_sync_task_with_mock_transport(
        self,
        sync_http_transport: httpx.MockTransport,
    ) -> None:
        """FetchRequestSyncTask should fetch a URL and return a ScrapeResponse."""
        task = FetchRequestSyncTask()
        with httpx.Client(transport=sync_http_transport) as client:
            msg = Message(
                payload=ScrapeRequest(
                    request_id="test-1",
                    kind="product",
                    url="/products/{product_id}",
                    params={"product_id": "1"},
                ),
                meta=MessageMeta(message_id="test-1"),
            )
            result = task.execute(msg, http=client)

        assert result.request_id == "test-1"
        assert result.kind == "product"
        assert result.status_code == 200
        assert result.body == '{"userId":11,"id":1,"title":"Example product","body":"Example body"}'

    def test_async_smoke_flow_compiles(self, smoke_config: DictConfig) -> None:
        """Probe flow should compile without requesting an async bridge."""
        plan = compile_flow(async_scrape_flow, runtime_config=smoke_config)
        assert plan.name == "async_smoke_flow"
        assert len(plan.nodes) == 2
        assert plan.needs_async_bridge is True
