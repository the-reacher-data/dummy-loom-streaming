"""Seed the smoke with a single product request for wiring probes."""

from __future__ import annotations

import logging

import structlog
from omegaconf import OmegaConf

from loom.core.config import load_config, section
from loom.streaming.kafka import KafkaMessageProducer, KafkaProducerClient, MessageDescriptor
from loom.streaming.kafka import MsgspecCodec, resolve_producer_topic

from dummy_streaming.config import SmokeConfig
from dummy_streaming.models import ScrapeRequest

logger = structlog.get_logger()


_REQUEST_DESCRIPTOR = MessageDescriptor(
    message_type="dummy.streaming.scrape.request",
    message_version=1,
)


def main() -> None:
    """Produce a single product request into Redpanda."""
    settings = load_config("config/seed_streaming.yaml")
    _setup_logging(settings)
    smoke = section(settings, "streaming.smoke", SmokeConfig)
    producer_settings = _load_producer_settings(settings)
    topic = resolve_producer_topic("scrape.requests", producer_settings)

    request = ScrapeRequest(
        request_id="seed-product-1",
        kind="product",
        url=f"{smoke.api_base}/products/1",
    )

    with KafkaMessageProducer(
        raw=KafkaProducerClient(producer_settings),
        codec=MsgspecCodec(),
    ) as producer:
        logger.info(
            "seeding_request",
            request_id=request.request_id,
            kind=request.kind,
            url=request.url,
            params=request.params,
        )
        producer.send(topic=topic, payload=request, descriptor=_REQUEST_DESCRIPTOR)


def _load_producer_settings(settings: object) -> object:
    """Load the Kafka producer section with a clear error if missing."""
    producer_settings = getattr(settings, "producer", None)
    if producer_settings is None:
        raise RuntimeError("seed_streaming.yaml must define kafka.producer")
    return producer_settings


def _setup_logging(config: object) -> None:
    """Configure structlog and set the configured log level."""
    level = str(OmegaConf.select(config, "logging.level", "INFO"))
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


if __name__ == "__main__":
    main()
