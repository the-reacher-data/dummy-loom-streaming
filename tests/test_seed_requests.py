"""Tests for the seed request entrypoint."""

from __future__ import annotations

from smoke.config import SmokeConfig
from smoke.entrypoints.seed_requests import _build_decode_error_record, _build_requests


def test_build_requests_errors_mode_includes_sync_routing_error() -> None:
    smoke = SmokeConfig(api_base="https://example.test", seed_products_limit=5)

    requests = _build_requests(smoke, "errors")

    assert [request.request_id for request in requests] == [
        "seed-products-malformed",
        "seed-sync-routing-error",
        "seed-products-unreachable",
    ]
    assert [request.kind for request in requests] == ["product", "sync_error", "product"]
    assert requests[1].url == "https://example.test/products/1"


def test_build_decode_error_record_shape() -> None:
    record = _build_decode_error_record("scrape.requests")

    assert record.topic == "scrape.requests"
    assert record.key == b"seed-decode-error"
    assert record.value == b"not-msgpack"
    assert record.headers == {}


def test_build_requests_all_mode_includes_good_and_errors() -> None:
    smoke = SmokeConfig(api_base="https://example.test", seed_products_limit=5)

    requests = _build_requests(smoke, "all")

    assert len(requests) == 4
    assert requests[0].request_id == "seed-products-catalog"
    assert requests[1].request_id == "seed-products-malformed"
    assert requests[2].request_id == "seed-sync-routing-error"
    assert requests[3].request_id == "seed-products-unreachable"
