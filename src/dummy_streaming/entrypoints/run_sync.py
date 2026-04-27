"""Run the sync smoke flow with Bytewax StreamingRunner."""

from __future__ import annotations

import structlog

from dummy_streaming.telemetry import get_tracer
from loom.streaming.bytewax.runner import StreamingRunner

from dummy_streaming.flows.sync_flow import sync_scrape_flow

logger = structlog.get_logger()
_tracer = get_tracer("dummy_streaming.entrypoints.run_sync")


def main() -> None:
    """Build and run the sync smoke flow against Kafka."""
    logger.info("starting_sync_flow")
    with _tracer.start_as_current_span("sync_smoke_flow"):
        runner = StreamingRunner.from_yaml(
        sync_scrape_flow,
        path="config/sync_streaming.yaml",
    )
            runner.run()


if __name__ == "__main__":
    main()
