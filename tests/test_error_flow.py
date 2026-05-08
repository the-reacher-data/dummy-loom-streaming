
"""Test the error mirror flows and error envelope printer."""

from __future__ import annotations

from loom.core.config import load_config
from loom.streaming import ErrorEnvelope, ErrorKind, Message, MessageMeta, compile_flow
from loom.streaming.kafka import DecodeError
from loom.streaming.compiler import CompiledMultiSource

from smoke.flows.error_flow import error_flow
from smoke.models import ScrapeRequest, ScrapeResponse
from smoke.tasks.error_flow import PrintErrorEnvelopeTask


def _fqn(t: type[object]) -> str:
    return f"{t.__module__}.{t.__qualname__}"


class TestErrorFlow:
    """Error mirror flows used by the streaming smoke."""

    def test_print_error_envelope_task_passes_through(self) -> None:
        """PrintErrorEnvelopeTask should log the envelope and return it unchanged."""
        task = PrintErrorEnvelopeTask()
        envelope = ErrorEnvelope[ScrapeRequest](
            kind=ErrorKind.TASK,
            reason="demo failure",
            payload_type=_fqn(ScrapeRequest),
            original_message=Message(
                payload=ScrapeRequest(
                    request_id="seed-products-malformed",
                    kind="product",
                    url="/products/{product_id}",
                ),
                meta=MessageMeta(message_id="test-1", trace_id="trace-error-1"),
            ),
        )
        message = Message(payload=envelope, meta=MessageMeta(message_id="test-2", trace_id="trace-error-2"))

        result = task.execute(message)

        assert result == envelope

    def test_error_flow_compiles(self) -> None:
        """Error mirror flow should compile against its dedicated config."""
        plan = compile_flow(error_flow, runtime_config=load_config("config/errors.yaml"))
        assert plan.name == "error_flow"
        assert isinstance(plan.source, CompiledMultiSource)
        assert plan.source.topics == ("scrape.errors",)
        assert plan.source.decode_strategy == "record"
        assert len(plan.nodes) == 2
        assert _fqn(ScrapeRequest) in plan.source.dispatch.error
        assert _fqn(ScrapeResponse) in plan.source.dispatch.error
        assert DecodeError.loom_message_type() in plan.source.dispatch.wire
