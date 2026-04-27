"""Seed the two-pod streaming smoke with initial requests."""

from __future__ import annotations

import structlog

from dummy_streaming.telemetry import get_tracer
from loom.streaming.kafka import KafkaMessageProducer, KafkaProducerClient, MessageDescriptor
from loom.streaming.kafka._config import load_kafka_settings, resolve_producer_topic
from loom.streaming.kafka._codec import MsgspecCodec

from dummy_streaming.models import ScrapeRequest

logger = structlog.get_logger()
_REQUEST_DESCRIPTOR = MessageDescriptor(
    message_type="dummy.streaming.scrape.request",
    message_version=1,
)


def main() -> None:
    """Produce the initial smoke requests into Redpanda."""
    settings = load_kafka_settings("config/seed_streaming.yaml")
    producer_settings = settings.producer
    if producer_settings is None:
        raise RuntimeError("seed_streaming.yaml must define kafka.producer")
    topic = resolve_producer_topic("scrape.requests", producer_settings)

    with KafkaMessageProducer(
        raw=KafkaProducerClient(producer_settings),
        codec=MsgspecCodec(),
    ) as producer:
        for request in (
            ScrapeRequest(request_id="seed-catalog", mode="catalog", post_id=1),
            ScrapeRequest(request_id="seed-summary", mode="summary", post_id=2),
        ):
                    with _tracer.start_as_current_span("seed_request") as span:
                span.set_attribute("scrape.request_id", request.request_id)
                span.set_attribute("scrape.mode", request.mode)
                logger.info("seeding_request", request_id=request.request_id, mode=request.mode)
                producer.send(topic=topic, payload=request, descriptor=_REQUEST_DESCRIPTOR)


if __name__ == "__main__":
    main()
