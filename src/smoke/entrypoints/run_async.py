"""Run the async smoke flow with Bytewax StreamingRunner."""

from __future__ import annotations

import logging

import structlog

from loom.streaming.bytewax.runner import StreamingRunner

from smoke.flows.async_flow import async_scrape_flow

logger = structlog.get_logger()


def main() -> None:
    """Build and run the async smoke flow against Kafka."""
    _setup_logging()
    logger.info("starting_async_flow")
    StreamingRunner.from_yaml(async_scrape_flow, "config/async_streaming.yaml").run()


def _setup_logging() -> None:
    """Configure structlog and set the pod log level to info."""
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    main()
