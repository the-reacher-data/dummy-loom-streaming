"""Seed the streaming smoke with a selectable request set."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

import structlog

from loom.core.config import load_config, section
from loom.streaming.kafka import (
    KafkaMessageProducer,
    KafkaProducerClient,
    KafkaRecord,
    KafkaSettings,
    MessageDescriptor,
    MsgspecCodec,
    ProducerSettings,
    resolve_producer_topic,
)

from smoke.config import SmokeConfig
from smoke.models import ScrapeRequest

logger = structlog.get_logger()


_REQUEST_DESCRIPTOR = MessageDescriptor(
    message_type="dummy.streaming.scrape.request",
    message_version=1,
)


def _build_decode_error_record(topic: str) -> KafkaRecord[bytes]:
    """Build one corrupt raw record for decode-error validation."""
    return KafkaRecord[bytes](
        topic=topic,
        key=b"seed-decode-error",
        value=b"not-msgpack",
        headers={},
    )


def main(argv: Sequence[str] | None = None) -> None:
    """Produce the selected request set into Redpanda."""
    args = _parse_args(argv)
    settings = load_config("config/seed_streaming.yaml")
    _setup_logging()
    smoke = section(settings, "streaming.smoke", SmokeConfig)
    producer_settings = _load_producer_settings(settings)
    topic = resolve_producer_topic("scrape.requests", producer_settings)

    requests = _build_requests(smoke, args.mode)

    with KafkaMessageProducer(
        raw=KafkaProducerClient(producer_settings),
        codec=MsgspecCodec(),
    ) as producer:
        for request in requests:
            logger.info(
                "seeding_request",
                request_id=request.request_id,
                kind=request.kind,
                url=request.url,
                params=request.params,
            )
            producer.send(
                topic=topic,
                payload=request,
                descriptor=_REQUEST_DESCRIPTOR,
                correlation_id=f"corr-{request.request_id}",
                trace_id=request.request_id,
            )

    if args.mode in {"errors", "all"}:
        _seed_decode_error(producer_settings, topic)


def _load_producer_settings(settings: object) -> ProducerSettings:
    """Load the Kafka producer section with a clear error if missing."""
    kafka_settings = section(settings, "kafka", KafkaSettings)
    if kafka_settings.producer is None:
        raise RuntimeError("seed_streaming.yaml must define kafka.producer")
    return kafka_settings.producer


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


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("good", "errors", "all"),
        default="all",
        help="Select which seed requests to publish.",
    )
    return parser.parse_args(argv)


def _build_requests(smoke: SmokeConfig, mode: str) -> tuple[ScrapeRequest, ...]:
    """Build the selected seed requests."""
    good = (
        ScrapeRequest(
            request_id="seed-products-catalog",
            kind="products_catalog",
            url=f"{smoke.api_base}/products",
            params={"limit": str(smoke.seed_products_limit)},
        ),
    )
    errors = (
        ScrapeRequest(
            request_id="seed-products-malformed",
            kind="product",
            url="/products/{product_id}",
        ),
        ScrapeRequest(
            request_id="seed-sync-routing-error",
            kind="sync_error",
            url=f"{smoke.api_base}/products/1",
        ),
        ScrapeRequest(
            request_id="seed-products-unreachable",
            kind="product",
            url="http://127.0.0.1:65535/products/1",
        ),
    )
    if mode == "good":
        return good
    if mode == "errors":
        return errors
    return good + errors


def _seed_decode_error(producer_settings: ProducerSettings, topic: str) -> None:
    """Produce one corrupt raw record so the wire decoder emits a DecodeError."""
    with KafkaProducerClient(producer_settings) as producer:
        logger.warning(
            "seeding_decode_error",
            request_id="seed-decode-error",
            topic=topic,
        )
        producer.send(_build_decode_error_record(topic))


if __name__ == "__main__":
    main()
