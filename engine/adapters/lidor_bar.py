"""
Lidor Bar adapter — lidorbar.co.il
Platform: Shopify
Strategy: Standard Shopify flow via ShopifyMixin

Note: URL must be verified — could be lidor.co.il, lidorbar.co.il, or lidor-bar.co.il.
Multiple candidate URLs are tried in healthcheck.
"""
from __future__ import annotations

import logging

from engine.adapters.base import BaseAdapter
from engine.adapters._shopify_mixin import ShopifyMixin
from engine.schemas.product import RawProductPayload
from engine.schemas.result import ParseResult, HealthCheckResult
from engine.schemas.source import SourceMeta

log = logging.getLogger("engine.adapters.lidor_bar")

# Candidate base URLs — try until one works
_CANDIDATE_URLS = [
    "https://www.lidorbar.co.il",
    "https://www.lidor.co.il",
    "https://lidorbar.myshopify.com",
]


class LidorBarAdapter(ShopifyMixin, BaseAdapter):
    SOURCE_KEY = "lidor_bar"
    SOURCE_NAME = "Lidor Bar"
    BASE_URL = "https://www.lidorbar.co.il"
    PLATFORM_FAMILY = "shopify"

    def __init__(self):
        super().__init__(
            min_delay=2.5,
            max_delay=5.5,
            extra_headers={"Referer": "https://www.lidorbar.co.il/"},
        )
        self._resolved_url: str | None = None

    def _resolve_base_url(self) -> str:
        """Find the actual working URL for this brand."""
        if self._resolved_url:
            return self._resolved_url
        for url in _CANDIDATE_URLS:
            try:
                resp = self.client.head(url)
                if resp.status_code < 400:
                    self._resolved_url = url
                    self.BASE_URL = url
                    log.info(f"[LidorBar] Resolved to {url}")
                    return url
            except Exception:
                continue
        return self.BASE_URL

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
            notes="URL needs verification — multiple candidates",
        )

    def discover_category_urls(self) -> list[str]:
        base = self._resolve_base_url()
        return [
            f"{base}/collections/all",
            f"{base}/collections/women",
            f"{base}/collections/new",
        ]

    def discover_product_urls(self, limit: int = 200) -> list[str]:
        self._resolve_base_url()
        return self._shopify_discover_product_urls(limit=limit)

    def fetch_product_page(self, url: str) -> RawProductPayload:
        return self._shopify_fetch_product_page(url)

    def extract_raw_payload(self, url: str, html: str) -> RawProductPayload:
        return self._build_raw_payload(url, html, method="dom")

    def parse_product(self, raw: RawProductPayload) -> ParseResult:
        result = self._shopify_parse_product(raw)
        if result.success and result.product and not result.product.brand:
            result.product.brand = "Lidor Bar"
        return result

    def healthcheck(self) -> HealthCheckResult:
        base = self._resolve_base_url()
        result = super().healthcheck()
        if not result.reachable:
            result.warnings.append(
                f"Primary URL unreachable. Tried: {_CANDIDATE_URLS}"
            )
        return result
