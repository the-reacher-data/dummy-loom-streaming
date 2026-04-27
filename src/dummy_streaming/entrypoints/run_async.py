"""Run the async smoke flow with Bytewax StreamingRunner."""

from __future__ import annotations

import structlog

from dummy_streaming.telemetry import get_tracer
from loom.streaming.bytewax.runner import StreamingRunner

from dummy_streaming.flows.async_flow import async_scrape_flow

logger = structlog.get_logger()
_tracer = get_tracer("dummy_streaming.entrypoints.run_async")


def main() -> None:
    """Build and run the async smoke flow against Kafka."""
    logger.info("starting_async_flow")
    with _tracer.start_as_current_span("async_smoke_flow"):
        runner = StreamingRunner.from_yaml(
        async_scrape_flow,
        path="config/async_streaming.yaml",
    )
            runner.run()


if __name__ == "__main__":
    main()
