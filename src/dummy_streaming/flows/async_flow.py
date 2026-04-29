"""Async pod — fetch requests concurrently."""

from __future__ import annotations

import httpx
from omegaconf import DictConfig, OmegaConf

from loom.core.config import section
from loom.streaming.graph._flow import Process, StreamFlow
from loom.streaming.nodes._boundary import FromTopic, IntoTopic
from loom.streaming.nodes._with import ContextFactory, ResourceScope, WithAsync

from dummy_streaming.config import SmokeConfig
from dummy_streaming.models import ScrapeRequest, ScrapeResponse
from dummy_streaming.tasks.smoke_async import FetchRequestTask

_DEFAULT_FLOW_CONFIG = OmegaConf.create(
    {
        "streaming": {
            "smoke": {
                "api_base": SmokeConfig().api_base,
                "async_timeout_s": SmokeConfig().async_timeout_s,
                "async_max_concurrency": SmokeConfig().async_max_concurrency,
            }
        }
    }
)


def build_async_scrape_flow(config: DictConfig) -> StreamFlow[ScrapeRequest, ScrapeResponse]:
    """Build the async smoke flow from resolved config."""
    smoke = section(config, "streaming.smoke", SmokeConfig)
    http_factory = ContextFactory(lambda: httpx.AsyncClient())
    return StreamFlow(
        name="async_smoke_flow",
        source=FromTopic[ScrapeRequest](
            name="scrape.requests",
            payload=ScrapeRequest,
        ),
        process=Process(
            WithAsync(
                process=Process(
                    FetchRequestTask(
                        timeout_s=smoke.async_timeout_s,
                        api_base=smoke.api_base,
                    ),
                    IntoTopic[ScrapeResponse](
                        name="scrape.responses",
                        payload=ScrapeResponse,
                    ),
                ),
                scope=ResourceScope.WORKER,
                http=http_factory,
                max_concurrency=smoke.async_max_concurrency,
            ),
        ),
    )


async_scrape_flow = build_async_scrape_flow(_DEFAULT_FLOW_CONFIG)
