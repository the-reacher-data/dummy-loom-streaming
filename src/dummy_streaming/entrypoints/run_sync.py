"""Run the sync smoke flow with Bytewax StreamingRunner."""

from __future__ import annotations

import logging
import os

import structlog

from loom.core.config import load_config
from loom.streaming.bytewax.runner import StreamingRunner

from dummy_streaming.flows.sync_flow import build_sync_scrape_flow

logger = structlog.get_logger()


def main() -> None:
    """Build and run the sync smoke flow against Kafka."""
    _setup_logging()
    logger.info("starting_sync_flow")
    cfg = load_config("config/sync_streaming.yaml")
    runner = StreamingRunner.from_config(
        build_sync_scrape_flow(cfg),
        runtime_config=cfg,
    )
    runner.run()


def _setup_logging() -> None:
    """Configure structlog and raise loom framework logs to WARNING."""
    level = os.environ.get("LOG_LEVEL", "INFO")
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
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))
    for noisy in ("loom.streaming.flow", "loom.streaming.bytewax"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


if __name__ == "__main__":
    main()
