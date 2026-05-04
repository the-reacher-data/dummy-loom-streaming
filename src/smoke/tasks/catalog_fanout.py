"""Catalog fan-out and response summary tasks for the sync pod."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping

from loom.streaming import ExpandStep, Message, RecordStep

from smoke.models import ProductReview, ScrapeRequest, ScrapeResponse

_logger = logging.getLogger(__name__)
_DEFAULT_API_BASE = "https://dummyjson.com"


class PrintResponseTask(RecordStep[ScrapeResponse, ScrapeResponse]):
    """Log a concise one-line summary for every response."""

    def execute(self, message: Message[ScrapeResponse], **_kwargs: object) -> ScrapeResponse:
        p = message.payload
        _logger.info(
            "response request_id=%s kind=%s url=%s status_code=%s elapsed_ms=%s",
            p.request_id,
            p.kind,
            p.url,
            p.status_code,
            p.elapsed_ms,
        )
        return p


class ExpandCategoriesTask(ExpandStep[ScrapeResponse, ScrapeRequest]):
    """Fan-out: one categories catalog → one ScrapeRequest per category.

    Parses the JSON body from ``/products/categories`` and emits an
    individual request for each category so the async pod can fetch them
    concurrently.
    """

    def __init__(self, *, api_base: str = _DEFAULT_API_BASE) -> None:
        self.api_base = api_base.rstrip("/")

    def execute(
        self, message: Message[ScrapeResponse], **_kwargs: object
    ) -> Iterable[ScrapeRequest]:
        p = message.payload
        categories = json.loads(p.body)
        if not isinstance(categories, list):
            _logger.warning("expand_categories_invalid_body request_id=%s", p.request_id)
            return []

        _logger.info(
            "expand_categories request_id=%s category_count=%d",
            p.request_id,
            len(categories),
        )
        for cat in categories:
            slug = cat.get("slug", "")
            name = cat.get("name", "")
            req = ScrapeRequest(
                request_id=f"{p.request_id}-cat-{slug}",
                kind="category",
                url="/products/category/{slug}",
                params={"slug": slug},
            )
            _logger.info(
                "emit_category_request request_id=%s category=%s url=%s",
                req.request_id,
                name,
                _resolve_request_url(self.api_base, req.url, req.params),
            )
            yield req


class ExpandProductCatalogTask(ExpandStep[ScrapeResponse, ScrapeRequest]):
    """Fan-out: one product catalog response → one product request per product."""

    def __init__(self, *, api_base: str = _DEFAULT_API_BASE) -> None:
        self.api_base = api_base.rstrip("/")

    def execute(
        self, message: Message[ScrapeResponse], **_kwargs: object
    ) -> Iterable[ScrapeRequest]:
        p = message.payload
        data = json.loads(p.body)
        products: list[dict[str, object]] = data.get("products", [])

        _logger.info(
            "expand_product_catalog request_id=%s product_count=%d",
            p.request_id,
            len(products),
        )
        for product in products:
            pid = int(product.get("id", 0))
            title = str(product.get("title", ""))
            req = ScrapeRequest(
                request_id=f"{p.request_id}-product-{pid}",
                kind="product",
                url="/products/{product_id}",
                params={"product_id": str(pid)},
            )
            _logger.info(
                "emit_catalog_product_request request_id=%s product_title=%s url=%s",
                req.request_id,
                title,
                _resolve_request_url(self.api_base, req.url, req.params),
            )
            yield req


class ExpandCategoryProductsTask(ExpandStep[ScrapeResponse, ScrapeRequest]):
    """Fan-out: one category response → one ScrapeRequest per product.

    Parses the JSON body from ``/products/category/{slug}`` and emits an
    individual request for each product id so the async pod can fetch them
    concurrently.
    """

    def __init__(self, *, api_base: str = _DEFAULT_API_BASE) -> None:
        self.api_base = api_base.rstrip("/")

    def execute(
        self, message: Message[ScrapeResponse], **_kwargs: object
    ) -> Iterable[ScrapeRequest]:
        p = message.payload
        data = json.loads(p.body)
        products: list[dict[str, object]] = data.get("products", [])

        _logger.info(
            "expand_category_products request_id=%s product_count=%d",
            p.request_id,
            len(products),
        )
        for product in products:
            pid = int(product["id"])
            title = product.get("title", "")
            req = ScrapeRequest(
                request_id=f"{p.request_id}-product-{pid}",
                kind="product",
                url="/products/{product_id}",
                params={"product_id": str(pid)},
            )
            _logger.info(
                "emit_product_request request_id=%s product_title=%s url=%s",
                req.request_id,
                title,
                _resolve_request_url(self.api_base, req.url, req.params),
            )
            yield req


class ExtractProductReviewsTask(RecordStep[ScrapeResponse, ScrapeResponse]):
    """Parse a product response and log every review.

    The product body already contains the ``reviews`` array, so no extra
    HTTP call is needed.
    """

    def execute(self, message: Message[ScrapeResponse], **_kwargs: object) -> ScrapeResponse:
        p = message.payload
        data = json.loads(p.body)
        pid = data.get("id", 0)
        title = data.get("title", "unknown")
        reviews: list[dict[str, object]] = data.get("reviews", [])

        _logger.info(
            "extract_product_reviews request_id=%s product_id=%s product_title=%s review_count=%d",
            p.request_id,
            pid,
            title,
            len(reviews),
        )
        for review in reviews:
            pr = ProductReview(
                product_id=pid,
                product_title=title,
                rating=int(review.get("rating", 0)),
                comment=str(review.get("comment", "")),
                reviewer_name=str(review.get("reviewerName", "")),
            )
            _logger.info(
                "review product_id=%s product_title=%s rating=%d comment=%s reviewer_name=%s",
                pr.product_id,
                pr.product_title,
                pr.rating,
                pr.comment,
                pr.reviewer_name,
        )
        return p


class HandleBusinessProductTask(RecordStep[ScrapeResponse, ScrapeResponse]):
    """Handle the demo business branch for one product and pass it through."""

    def execute(self, message: Message[ScrapeResponse], **_kwargs: object) -> ScrapeResponse:
        p = message.payload
        _logger.info(
            "business_product_branch request_id=%s url=%s status_code=%s",
            p.request_id,
            p.url,
            p.status_code,
        )
        return p


class ExtractCategoryReviewsTask(RecordStep[ScrapeResponse, ScrapeResponse]):
    """Parse a category response and log every review found inside each product.

    The category body already contains the full product objects with their
    ``reviews`` arrays, so no extra HTTP call is needed.
    """

    def execute(self, message: Message[ScrapeResponse], **_kwargs: object) -> ScrapeResponse:
        p = message.payload
        data = json.loads(p.body)
        products: list[dict[str, object]] = data.get("products", [])
        category = data.get("products", [{}])[0].get("category", "unknown") if products else "unknown"

        _logger.info(
            "extract_category_reviews request_id=%s category=%s product_count=%d",
            p.request_id,
            category,
            len(products),
        )

        for product in products:
            pid = product.get("id", 0)
            title = product.get("title", "unknown")
            reviews: list[dict[str, object]] = product.get("reviews", [])
            for review in reviews:
                pr = ProductReview(
                    product_id=pid,
                    product_title=title,
                    rating=int(review.get("rating", 0)),
                    comment=str(review.get("comment", "")),
                    reviewer_name=str(review.get("reviewerName", "")),
                )
                _logger.info(
                    "review product_id=%s product_title=%s rating=%d comment=%s reviewer_name=%s",
                    pr.product_id,
                    pr.product_title,
                    pr.rating,
                    pr.comment,
                    pr.reviewer_name,
                )

        return p


def _resolve_request_url(api_base: str, url: str, params: Mapping[str, str]) -> str:
    """Resolve a request URL against the configured API base."""
    rendered = url.format_map(params)
    if rendered.startswith(("http://", "https://")):
        return rendered
    return f"{api_base.rstrip('/')}/{rendered.lstrip('/')}"
