"""Test fan-out behaviour in the sync smoke flow."""

from __future__ import annotations

from loom.streaming import Message, MessageMeta

from smoke.models import ScrapeResponse
from smoke.tasks.catalog_fanout import (
    ExpandCategoriesTask,
    ExpandCategoryProductsTask,
    ExpandProductCatalogTask,
)


class TestFanOut:
    """Fan-out tasks used by the sync smoke flow."""

    def test_expand_categories_task_emits_one_request_per_category(self) -> None:
        """A categories response should expand into one follow-up request per category."""
        task = ExpandCategoriesTask()
        message = Message(
            payload=ScrapeResponse(
                request_id="seed-categories",
                kind="categories",
                method="GET",
                url="https://dummyjson.com/products/categories",
                status_code=200,
                elapsed_ms=45.0,
                body='[{"slug": "beauty", "name": "Beauty"}, {"slug": "fragrances", "name": "Fragrances"}]',
            ),
            meta=MessageMeta(message_id="test-1"),
        )

        results = list(task.execute(message))

        assert len(results) == 2
        assert [item.request_id for item in results] == [
            "seed-categories-cat-beauty",
            "seed-categories-cat-fragrances",
        ]
        assert [item.kind for item in results] == ["category", "category"]

    def test_expand_category_products_task_emits_one_request_per_product(self) -> None:
        """A category response should expand into one follow-up request per product."""
        task = ExpandCategoryProductsTask()
        message = Message(
            payload=ScrapeResponse(
                request_id="seed-cat-beauty",
                kind="category",
                method="GET",
                url="https://dummyjson.com/products/category/beauty",
                status_code=200,
                elapsed_ms=45.0,
                body='{"products": [{"id": 1, "title": "Mascara"}, {"id": 2, "title": "Eyeshadow"}]}',
            ),
            meta=MessageMeta(message_id="test-2"),
        )

        results = list(task.execute(message))

        assert len(results) == 2
        assert [item.request_id for item in results] == [
            "seed-cat-beauty-product-1",
            "seed-cat-beauty-product-2",
        ]
        assert [item.kind for item in results] == ["product", "product"]

    def test_expand_product_catalog_task_emits_one_request_per_product_category(self) -> None:
        """A products catalog response should expand into one request per product."""
        task = ExpandProductCatalogTask()
        message = Message(
            payload=ScrapeResponse(
                request_id="seed-products-catalog",
                kind="products_catalog",
                method="GET",
                url="https://dummyjson.com/products",
                status_code=200,
                elapsed_ms=45.0,
                body='{"products": [{"id": 1, "title": "Laptop", "category": "laptops"}, {"id": 2, "title": "Phone", "category": "smartphones"}]}',
            ),
            meta=MessageMeta(message_id="test-4"),
        )

        results = list(task.execute(message))

        assert len(results) == 2
        assert [item.request_id for item in results] == [
            "seed-products-catalog-product-1",
            "seed-products-catalog-product-2",
        ]
        assert [item.kind for item in results] == ["product", "product"]

    def test_expand_ignores_empty_list(self) -> None:
        """An empty list should not emit any requests."""
        task = ExpandCategoriesTask()
        message = Message(
            payload=ScrapeResponse(
                request_id="seed-categories",
                kind="categories",
                method="GET",
                url="https://dummyjson.com/products/categories",
                status_code=200,
                elapsed_ms=30.0,
                body='[]',
            ),
            meta=MessageMeta(message_id="test-3"),
        )

        assert list(task.execute(message)) == []
