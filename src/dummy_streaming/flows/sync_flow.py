"""Sync pod — Broadcast + Fork with IntoTopic per branch (framework requirement)."""

from __future__ import annotations

from typing import Any

from loom.streaming import (
    Broadcast,
    BroadcastRoute,
    ErrorEnvelope,
    ErrorKind,
    Fork,
    FromTopic,
    IntoTopic,
    Process,
    StreamFlow,
    payload,
)

from dummy_streaming.models import ScrapeRequest, ScrapeResponse
from dummy_streaming.tasks.catalog_fanout import (
    ExpandCategoriesTask,
    ExpandCategoryProductsTask,
    ExpandProductCatalogTask,
    ExtractProductReviewsTask,
    PrintResponseTask,
)

sync_scrape_flow: StreamFlow[Any, Any] = StreamFlow(
    name="sync_smoke_flow",
    source=FromTopic[ScrapeResponse](
        name="scrape.responses",
        payload=ScrapeResponse,
    ),
    process=Process(
        Broadcast(
            BroadcastRoute(
                process=Process(PrintResponseTask),
                output=IntoTopic[ScrapeResponse](
                    name="scrape.audit",
                    payload=ScrapeResponse,
                ),
            ),
            BroadcastRoute(
                process=Process(
                    Fork.by(
                        selector=payload.kind,
                        branches={
                            "products_catalog": Process(
                                ExpandProductCatalogTask.from_config(
                                    "streaming.smoke.expand_product_catalog"
                                ),
                                IntoTopic[ScrapeRequest](
                                    name="scrape.requests",
                                    payload=ScrapeRequest,
                                ),
                            ),
                            "categories": Process(
                                ExpandCategoriesTask.from_config(
                                    "streaming.smoke.expand_categories"
                                ),
                                IntoTopic[ScrapeRequest](
                                    name="scrape.requests",
                                    payload=ScrapeRequest,
                                ),
                            ),
                            "category": Process(
                                ExpandCategoryProductsTask.from_config(
                                    "streaming.smoke.expand_category_products"
                                ),
                                IntoTopic[ScrapeRequest](
                                    name="scrape.requests",
                                    payload=ScrapeRequest,
                                ),
                            ),
                            "product": Process(
                                ExtractProductReviewsTask,
                                IntoTopic[ScrapeResponse](
                                    name="scrape.audit",
                                    payload=ScrapeResponse,
                                ),
                            ),
                        },
                    ),
                ),
                output=IntoTopic[ScrapeResponse](
                    name="scrape.audit",
                    payload=ScrapeResponse,
                ),
            ),
        ),
    ),
    errors={
        ErrorKind.WIRE: IntoTopic[ErrorEnvelope[ScrapeResponse]](
            name="scrape.errors",
            payload=ErrorEnvelope[ScrapeResponse],
        ),
        ErrorKind.ROUTING: IntoTopic[ErrorEnvelope[ScrapeResponse]](
            name="scrape.errors",
            payload=ErrorEnvelope[ScrapeResponse],
        ),
        ErrorKind.TASK: IntoTopic[ErrorEnvelope[ScrapeResponse]](
            name="scrape.errors",
            payload=ErrorEnvelope[ScrapeResponse],
        ),
        ErrorKind.BUSINESS: IntoTopic[ErrorEnvelope[ScrapeResponse]](
            name="scrape.errors",
            payload=ErrorEnvelope[ScrapeResponse],
        ),
    },
)
