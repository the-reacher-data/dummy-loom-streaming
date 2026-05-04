"""Test the error mirror flows and error envelope printer."""

from __future__ import annotations

from loom.core.config import load_config
from loom.streaming import ErrorEnvelope, ErrorKind, Message, MessageMeta, compile_flow

from smoke.flows.error_flow import error_flow
from smoke.models import ScrapeRequest
from smoke.tasks.error_flow import PrintErrorEnvelopeTask


class TestErrorFlow:
    """Error mirror flows used by the streaming smoke."""

    def test_print_error_envelope_task_passes_through(self) -> None:
        """PrintErrorEnvelopeTask should log the envelope and return it unchanged."""
        task = PrintErrorEnvelopeTask()
        envelope = ErrorEnvelope[ScrapeRequest](
            kind=ErrorKind.TASK,
            reason="demo failure",
            original_message=Message(
                payload=ScrapeRequest(
                    request_id="seed-products-malformed",
                    kind="product",
                    url="/products/{product_id}",
                ),
                meta=MessageMeta(message_id="test-1"),
            ),
        )
        message = Message(payload=envelope, meta=MessageMeta(message_id="test-2"))

        result = task.execute(message)

        assert result == envelope

    def test_error_flow_compiles(self) -> None:
        """Error mirror flow should compile against its dedicated config."""
        plan = compile_flow(error_flow, runtime_config=load_config("config/errors.yaml"))
        assert plan.name == "error_flow"
        assert len(plan.nodes) == 1
