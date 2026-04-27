"""Test fan-out behaviour in the sync smoke flow."""

from __future__ import annotations

from loom.streaming.core._message import Message, MessageMeta

from dummy_streaming.models import ScrapeResponse
from dummy_streaming.tasks.smoke_sync import FanOutFollowupRequestsStep


def test_fan_out_produces_one_request_per_item_id() -> None:
    """A catalog response should expand into one follow-up request per id."""
    step = FanOutFollowupRequestsStep()
    message = Message(
        payload=ScrapeResponse(
            request_id="seed-catalog",
            response_kind="catalog",
            post_id=1,
            item_ids=[101, 202, 303],
            title="3 comments",
        ),
        meta=MessageMeta(message_id="test-1"),
    )

    results = list(step.execute(message))

    assert len(results) == 3
    assert [item.payload.post_id for item in results] == [101, 202, 303]
    assert [item.payload.mode for item in results] == ["followup", "followup", "followup"]
    assert [item.payload.request_id for item in results] == [
        "seed-catalog-101",
        "seed-catalog-202",
        "seed-catalog-303",
    ]


def test_fan_out_ignores_non_catalog_responses() -> None:
    """Non-catalog responses should not create follow-up requests."""
    step = FanOutFollowupRequestsStep()
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

    assert list(step.execute(message)) == []
