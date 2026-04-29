"""Seed the two-pod streaming smoke with the initial catalog request."""

from __future__ import annotations

import structlog

from loom.core.config import load_config, section
from loom.streaming.kafka import KafkaMessageProducer, KafkaProducerClient, MessageDescriptor
from loom.streaming.kafka._config import resolve_producer_topic
from loom.streaming.kafka._codec import MsgspecCodec

from dummy_streaming.config import SmokeConfig
from dummy_streaming.models import ScrapeRequest

logger = structlog.get_logger()

_REQUEST_DESCRIPTOR = MessageDescriptor(
    message_type="dummy.streaming.scrape.request",
    message_version=1,
)


def main() -> None:
    """Produce the initial catalog request into Redpanda."""
    settings = load_config("config/seed_streaming.yaml")
    smoke = section(settings, "streaming.smoke", SmokeConfig)
    producer_settings = _load_producer_settings(settings)
    topic = resolve_producer_topic("scrape.requests", producer_settings)

    catalog_request = ScrapeRequest(
        request_id="seed-products-catalog",
        kind="products_catalog",
        url=f"{smoke.api_base}/products",
        params={"limit": str(smoke.seed_products_limit)},
    )

    with KafkaMessageProducer(
        raw=KafkaProducerClient(producer_settings),
        codec=MsgspecCodec(),
    ) as producer:
        logger.info(
            "seeding_request",
            request_id=catalog_request.request_id,
            kind=catalog_request.kind,
            url=catalog_request.url,
            params=catalog_request.params,
        )
        producer.send(topic=topic, payload=catalog_request, descriptor=_REQUEST_DESCRIPTOR)


def _load_producer_settings(settings: object) -> object:
    """Load the Kafka producer section with a clear error if missing."""
    producer_settings = getattr(settings, "producer", None)
    if producer_settings is None:
        raise RuntimeError("seed_streaming.yaml must define kafka.producer")
    return producer_settings


if __name__ == "__main__":
    main()
