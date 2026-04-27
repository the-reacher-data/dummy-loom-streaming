"""Test the sync smoke flow and its sync response tasks."""

from __future__ import annotations

from loom.streaming.compiler import compile_flow
from loom.streaming.core._message import Message, MessageMeta
from omegaconf import OmegaConf

from dummy_streaming.flows.sync_flow import sync_scrape_flow
from dummy_streaming.models import ScrapeRequest, ScrapeResponse
from dummy_streaming.tasks.smoke_sync import PrintResponseTask, UppercaseRequestIdTask


def test_uppercase_request_id_task_preserves_payload_shape() -> None:
    """UppercaseRequestIdTask should uppercase the request id and keep other data."""
    task = UppercaseRequestIdTask()
    message = Message(
        payload=ScrapeResponse(
            request_id="seed-catalog",
            response_kind="catalog",
            post_id=1,
            item_ids=[11, 12],
            title="2 comments",
        ),
        meta=MessageMeta(message_id="test-1"),
    )

    result = task.execute(message)

    assert result.request_id == "SEED-CATALOG"
    assert result.response_kind == "catalog"
    assert result.item_ids == [11, 12]


def test_print_response_task_passes_through(capsys) -> None:
    """PrintResponseTask should write the response and return the same payload."""
    task = PrintResponseTask()
    message = Message(
        payload=ScrapeResponse(
            request_id="seed-summary",
            response_kind="summary",
            post_id=2,
            item_ids=[2],
            title="Example title",
        ),
        meta=MessageMeta(message_id="test-2"),
    )

    result = task.execute(message)
    captured = capsys.readouterr()

    assert "response kind=summary" in captured.out
    assert result == message.payload


def test_sync_smoke_flow_compiles() -> None:
    """Sync smoke flow should compile against Kafka config."""
    cfg = OmegaConf.create(
        {
            "kafka": {
                "producer": {
                    "brokers": ["redpanda:9092"],
                    "client_id": "test-producer",
                },
                "consumer": {
                    "brokers": ["kafka:29092"],
                    "group_id": "test-consumer",
                    "topics": ["scrape.responses"],
                },
            }
        }
    )
    plan = compile_flow(sync_scrape_flow, runtime_config=cfg)
    assert plan.name == "sync_smoke_flow"
    assert len(plan.nodes) == 1
