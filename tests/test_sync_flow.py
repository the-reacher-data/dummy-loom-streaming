"""Test the sync smoke flow and its sync response tasks."""

from __future__ import annotations

from omegaconf import DictConfig

from loom.streaming import Message, MessageMeta, compile_flow

from smoke.flows.sync_flow import sync_scrape_flow
from smoke.models import ScrapeResponse
from smoke.tasks.catalog_fanout import ExtractProductReviewsTask, PrintResponseTask


class TestSyncFlow:
    """Sync smoke flow and task behavior."""

    def test_print_response_task_passes_through(self) -> None:
        """PrintResponseTask should log the response and return the same payload."""
        task = PrintResponseTask()
        message = Message(
            payload=ScrapeResponse(
                request_id="seed-product-1",
                kind="product",
                method="GET",
                url="https://dummyjson.com/products/1",
                status_code=200,
                elapsed_ms=25.0,
                body='{"title": "Example product"}',
            ),
            meta=MessageMeta(message_id="test-2", trace_id="trace-sync-1"),
        )

        result = task.execute(message)

        assert result == message.payload

    def test_extract_reviews_task_logs_reviews(self) -> None:
        """ExtractProductReviewsTask should parse reviews from a product body."""
        task = ExtractProductReviewsTask()
        message = Message(
            payload=ScrapeResponse(
                request_id="seed-product-1",
                kind="product",
                method="GET",
                url="https://dummyjson.com/products/1",
                status_code=200,
                elapsed_ms=30.0,
                body='{"id": 1, "title": "Mascara", "reviews": [{"rating": 5, "comment": "Great!", "reviewerName": "Alice"}]}',
            ),
            meta=MessageMeta(message_id="test-2", trace_id="trace-sync-2"),
        )

        result = task.execute(message)

        assert result == message.payload

    def test_sync_smoke_flow_compiles(self, smoke_config: DictConfig) -> None:
        """Sync smoke flow should compile against Kafka config."""
        plan = compile_flow(sync_scrape_flow, runtime_config=smoke_config)
        assert plan.name == "sync_smoke_flow"
        assert len(plan.nodes) == 1
