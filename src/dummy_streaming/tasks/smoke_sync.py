"""Sync smoke tasks for the two-pod streaming scenario."""

from __future__ import annotations

from collections.abc import Iterable

from loom.streaming.core._message import Message
from dummy_streaming.telemetry import get_tracer
from loom.streaming.nodes._step import ExpandStep, RecordStep

from dummy_streaming.models import ScrapeRequest, ScrapeResponse

_tracer = get_tracer("dummy_streaming.tasks.smoke_sync")


class UppercaseRequestIdTask(RecordStep[ScrapeResponse, ScrapeResponse]):
    """Uppercase the request identifier for logging and visibility."""

    def execute(self, message: Message[ScrapeResponse], **kwargs: object) -> ScrapeResponse:
        payload = message.payload
        with _tracer.start_as_current_span("uppercase_request_id") as span:
            span.set_attribute("scrape.request_id", payload.request_id)
            span.set_attribute("scrape.response_kind", payload.response_kind)
        return ScrapeResponse(
            request_id=payload.request_id.upper(),
            response_kind=payload.response_kind,
            post_id=payload.post_id,
            item_ids=payload.item_ids,
            title=payload.title,
        )


class PrintResponseTask(RecordStep[ScrapeResponse, ScrapeResponse]):
    """Print one response and keep it flowing downstream."""

    def execute(self, message: Message[ScrapeResponse], **kwargs: object) -> ScrapeResponse:
        payload = message.payload
        with _tracer.start_as_current_span("print_response") as span:
            span.set_attribute("scrape.request_id", payload.request_id)
            span.set_attribute("scrape.response_kind", payload.response_kind)
        print(
            f"response kind={payload.response_kind} request_id={payload.request_id} "
            f"post_id={payload.post_id} item_ids={payload.item_ids} title={payload.title}",
            flush=True,
        )
        return payload


class FanOutFollowupRequestsStep(ExpandStep[ScrapeResponse, ScrapeRequest]):
    """Expand one catalog response into many follow-up request messages."""

    def execute(
        self,
        message: Message[ScrapeResponse],
        **kwargs: object,
    ) -> Iterable[Message[ScrapeRequest]]:
        payload = message.payload
        if payload.response_kind != "catalog":
            return ()
        with _tracer.start_as_current_span("fan_out_followup_requests") as span:
            span.set_attribute("scrape.request_id", payload.request_id)
            span.set_attribute("scrape.item_count", len(payload.item_ids))
        return [
            Message(
                payload=ScrapeRequest(
                    request_id=f"{payload.request_id}-{item_id}",
                    mode="followup",
                    post_id=item_id,
                ),
                meta=message.meta,
            )
            for item_id in payload.item_ids
        ]
