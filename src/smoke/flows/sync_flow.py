"""Sync pod — Broadcast + Fork with IntoTopic per branch (framework requirement)."""

from __future__ import annotations

from typing import Any

from loom.streaming import (
    Broadcast,
    BroadcastRoute,
    ErrorEnvelope,
    ErrorKind,
    ErrorRoute,
    Fork,
    FromTopic,
    IntoTopic,
    Process,
    StreamFlow,
    payload,
)
from loom.streaming.kafka import DecodeError

from smoke.models import ScrapeRequest, ScrapeResponse
from smoke.tasks.catalog_fanout import (
    ExpandCategoriesTask,
    ExpandCategoryProductsTask,
    ExpandProductCatalogTask,
    ExtractProductReviewsTask,
    PrintResponseTask,
    TriggerSyncErrorTask,
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
                            "sync_error": Process(
                                TriggerSyncErrorTask,
                                IntoTopic[ScrapeResponse](
                                    name="scrape.audit",
                                    payload=ScrapeResponse,
                                ),
                            ),
                        },
                    ),
                ),
                output=None,
            ),
        ),
    ),
    errors=(
        ErrorRoute(
            kinds=(ErrorKind.WIRE,),
            output=IntoTopic[DecodeError](
                name="scrape.errors",
                payload=DecodeError,
            ),
        ),
        ErrorRoute(
            kinds=(ErrorKind.ROUTING, ErrorKind.TASK, ErrorKind.BUSINESS),
            output=IntoTopic[ErrorEnvelope[ScrapeResponse]](
                name="scrape.errors",
                payload=ErrorEnvelope[ScrapeResponse],
            ),
        ),
    ),
)
