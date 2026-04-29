"""Sync pod — Broadcast + Fork with IntoTopic per branch (framework requirement)."""

from __future__ import annotations

from typing import Any

from omegaconf import DictConfig, OmegaConf

from loom.core.config import section
from loom.core.expr.refs import RootRef
from loom.streaming.graph._flow import Process, StreamFlow
from loom.streaming.nodes._boundary import FromTopic, IntoTopic
from loom.streaming.nodes._broadcast import Broadcast, BroadcastRoute
from loom.streaming.nodes._fork import Fork

from dummy_streaming.config import SmokeConfig
from dummy_streaming.models import ScrapeRequest, ScrapeResponse
from dummy_streaming.tasks.smoke_sync import (
    ExpandCategoriesTask,
    ExpandCategoryProductsTask,
    ExtractProductReviewsTask,
    PrintResponseTask,
)

_payload = RootRef("payload")

_DEFAULT_FLOW_CONFIG = OmegaConf.create(
    {
        "streaming": {
            "smoke": {
                "api_base": SmokeConfig().api_base,
            }
        }
    }
)


def build_sync_scrape_flow(config: DictConfig) -> StreamFlow[Any, Any]:
    """Build the sync smoke flow from resolved config."""
    smoke = section(config, "streaming.smoke", SmokeConfig)
    return StreamFlow(
        name="sync_smoke_flow",
        source=FromTopic[ScrapeResponse](
            name="scrape.responses",
            payload=ScrapeResponse,
        ),
        process=Process(
            Broadcast(
                BroadcastRoute(
                    process=Process(PrintResponseTask()),
                    output=IntoTopic[ScrapeResponse](
                        name="scrape.audit",
                        payload=ScrapeResponse,
                    ),
                ),
                BroadcastRoute(
                    process=Process(
                        Fork.by(
                            selector=_payload.kind,
                            branches={
                                "categories": Process(
                                    ExpandCategoriesTask(api_base=smoke.api_base),
                                    IntoTopic[ScrapeRequest](
                                        name="scrape.requests",
                                        payload=ScrapeRequest,
                                    ),
                                ),
                                "category": Process(
                                    ExpandCategoryProductsTask(api_base=smoke.api_base),
                                    IntoTopic[ScrapeRequest](
                                        name="scrape.requests",
                                        payload=ScrapeRequest,
                                    ),
                                ),
                                "product": Process(
                                    ExtractProductReviewsTask(),
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
    )


sync_scrape_flow = build_sync_scrape_flow(_DEFAULT_FLOW_CONFIG)
