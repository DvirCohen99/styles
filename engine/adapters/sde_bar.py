"""
Sde Bar adapter — sdebar.co.il
Platform: Shopify
Strategy: Standard Shopify flow via ShopifyMixin
"""
from __future__ import annotations

import logging

from engine.adapters.base import BaseAdapter
from engine.adapters._shopify_mixin import ShopifyMixin
from engine.schemas.product import RawProductPayload
from engine.schemas.result import ParseResult
from engine.schemas.source import SourceMeta

log = logging.getLogger("engine.adapters.sde_bar")


class SdeBarAdapter(ShopifyMixin, BaseAdapter):
    SOURCE_KEY = "sde_bar"
    SOURCE_NAME = "Sde Bar"
    BASE_URL = "https://www.sdebar.co.il"
    PLATFORM_FAMILY = "shopify"

    def __init__(self):
        super().__init__(
            min_delay=2.5,
            max_delay=5.5,
            extra_headers={"Referer": "https://www.sdebar.co.il/"},
        )

    @property
    def source_meta(self) -> SourceMeta:
        return SourceMeta(
            source_key=self.SOURCE_KEY,
            source_name=self.SOURCE_NAME,
            base_url=self.BASE_URL,
            platform_family=self.PLATFORM_FAMILY,
            priority=2,
            has_sitemap=True,
            has_api=True,
        )

    def discover_category_urls(self) -> list[str]:
        return [
            f"{self.BASE_URL}/collections/all",
            f"{self.BASE_URL}/collections/women",
            f"{self.BASE_URL}/collections/new-arrivals",
            f"{self.BASE_URL}/collections/sale",
        ]

    def discover_product_urls(self, limit: int = 200) -> list[str]:
        return self._shopify_discover_product_urls(limit=limit)

    def fetch_product_page(self, url: str) -> RawProductPayload:
        return self._shopify_fetch_product_page(url)

    def extract_raw_payload(self, url: str, html: str) -> RawProductPayload:
        return self._build_raw_payload(url, html, method="dom")

    def parse_product(self, raw: RawProductPayload) -> ParseResult:
        result = self._shopify_parse_product(raw)
        if result.success and result.product and not result.product.brand:
            result.product.brand = "Sde Bar"
        return result
