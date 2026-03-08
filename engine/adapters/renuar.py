"""
Renuar adapter — renuar.co.il
Platform: Shopify (Hebrew locale)
Strategy:
  1. Sitemap discovery (/sitemap.xml → product URLs sitemap)
  2. Product fetch via /products/<handle>.json (Shopify API)
  3. Fallback: JSON-LD → DOM
"""
from __future__ import annotations

import logging
from typing import Optional

from engine.adapters.base import BaseAdapter
from engine.adapters._shopify_mixin import ShopifyMixin
from engine.schemas.product import RawProductPayload
from engine.schemas.result import ParseResult
from engine.schemas.source import SourceMeta

log = logging.getLogger("engine.adapters.renuar")


class RenuarAdapter(ShopifyMixin, BaseAdapter):
    SOURCE_KEY = "renuar"
    SOURCE_NAME = "Renuar"
    BASE_URL = "https://www.renuar.co.il"
    PLATFORM_FAMILY = "shopify"

    def __init__(self):
        super().__init__(
            min_delay=2.5,
            max_delay=5.0,
            extra_headers={
                "Referer": "https://www.renuar.co.il/",
                "Accept-Language": "he-IL,he;q=0.9",
            },
        )

    @property
    def source_meta(self) -> SourceMeta:
        return SourceMeta(
            source_key=self.SOURCE_KEY,
            source_name=self.SOURCE_NAME,
            base_url=self.BASE_URL,
            platform_family=self.PLATFORM_FAMILY,
            priority=1,
            has_sitemap=True,
            has_api=True,
        )

    def discover_category_urls(self) -> list[str]:
        return [
            f"{self.BASE_URL}/he/women",
            f"{self.BASE_URL}/he/men",
            f"{self.BASE_URL}/he/sale",
            f"{self.BASE_URL}/he/new-arrivals",
        ]

    def discover_product_urls(self, limit: int = 200) -> list[str]:
        urls = self._shopify_discover_product_urls(limit=limit)
        # Renuar uses Hebrew locale prefix — ensure /he/ is present if needed
        normalized = []
        for u in urls:
            # Keep as-is, sitemap should have canonical URLs
            normalized.append(u)
        return normalized[:limit]

    def fetch_product_page(self, url: str) -> RawProductPayload:
        return self._shopify_fetch_product_page(url)

    def extract_raw_payload(self, url: str, html: str) -> RawProductPayload:
        return self._build_raw_payload(url, html, method="dom")

    def parse_product(self, raw: RawProductPayload) -> ParseResult:
        result = self._shopify_parse_product(raw)
        # Post-process: Renuar-specific enrichments
        if result.success and result.product:
            p = result.product
            # Renuar is an Israeli brand — set brand if not set
            if not p.brand:
                p.brand = "Renuar"
            if not p.source_name:
                p.source_name = self.SOURCE_NAME
        return result
