"""
Base adapter contract.

Every source adapter must:
1. Inherit from BaseAdapter
2. Implement all abstract methods
3. Follow the extraction order:
   sitemap → category → JSON-LD → script payload → DOM → heuristic

The base class provides shared utilities so adapters stay lean.
"""
from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from engine.http.client import HTTPClient, ScrapingError, NotFoundError, BlockedError
from engine.schemas.product import NormalizedProduct, RawProductPayload, ProductVariant
from engine.schemas.result import ParseResult, ParseWarning, HealthCheckResult
from engine.schemas.source import SourceMeta, SourceStats
from engine.extraction.json_ld import extract_json_ld, find_product_json_ld, find_breadcrumbs_json_ld, parse_product_from_json_ld
from engine.extraction.script_payload import extract_script_payload
from engine.extraction.dom_selector import DOMExtractor
from engine.extraction.heuristic import HeuristicExtractor
from engine.normalization.price import normalize_price, normalize_price_pair
from engine.normalization.text import normalize_text, normalize_name, detect_gender, detect_material, detect_is_new_collection
from engine.normalization.variants import normalize_sizes, normalize_colors, normalize_variants
from engine.normalization.images import normalize_image_urls


log = logging.getLogger("engine.adapters.base")


class BaseAdapter(ABC):
    """
    Abstract base for all source adapters.

    Subclasses MUST implement:
        - source_meta (property or class attr)
        - discover_category_urls()
        - discover_product_urls()
        - fetch_product_page()
        - extract_raw_payload()
        - parse_product()

    Subclasses MAY override:
        - parse_variants()
        - detect_stock()
        - detect_sale()
        - normalize()
        - healthcheck()
    """

    # ── To be set by each subclass ─────────────────────────────────────────
    SOURCE_KEY: str = ""
    SOURCE_NAME: str = ""
    BASE_URL: str = ""
    PLATFORM_FAMILY: str = "unknown"    # shopify | magento | woocommerce | custom | unknown
    PARSER_VERSION: str = "1.0.0"

    def __init__(
        self,
        min_delay: float = 2.5,
        max_delay: float = 6.0,
        max_retries: int = 3,
        extra_headers: Optional[dict] = None,
    ):
        self.client = HTTPClient(
            min_delay=min_delay,
            max_delay=max_delay,
            max_retries=max_retries,
            base_headers=extra_headers or {},
        )
        self.stats = SourceStats(source_key=self.SOURCE_KEY)
        self._warnings: list[ParseWarning] = []

    # ── Abstract interface ────────────────────────────────────────────────

    @property
    @abstractmethod
    def source_meta(self) -> SourceMeta:
        """Return static metadata about this source."""
        ...

    @abstractmethod
    def discover_category_urls(self) -> list[str]:
        """Return list of category/collection page URLs."""
        ...

    @abstractmethod
    def discover_product_urls(self, limit: int = 200) -> list[str]:
        """
        Return deduplicated list of product page URLs.
        Should exhaust sitemap → category pages → pagination.
        """
        ...

    @abstractmethod
    def fetch_product_page(self, url: str) -> RawProductPayload:
        """
        Fetch a product page and return a RawProductPayload
        containing html, json_ld, script payloads.
        """
        ...

    @abstractmethod
    def extract_raw_payload(self, url: str, html: str) -> RawProductPayload:
        """Extract all raw data structures from HTML string."""
        ...

    @abstractmethod
    def parse_product(self, raw: RawProductPayload) -> ParseResult:
        """
        Parse a RawProductPayload into a NormalizedProduct.
        Must not raise — return ParseResult with success=False on failure.
        """
        ...

    # ── Provided base implementations ─────────────────────────────────────

    def parse_variants(self, raw_variants: list[dict]) -> list[ProductVariant]:
        """Parse variant list into ProductVariant objects."""
        return normalize_variants(raw_variants)

    def detect_stock(self, product_data: dict, variants: list[ProductVariant]) -> dict:
        """
        Detect stock status from product data and variants.
        Returns dict with: in_stock, out_of_stock, low_stock, stock_status
        """
        # Check variant-level stock
        if variants:
            available_count = sum(1 for v in variants if v.in_stock)
            total = len(variants)
            if available_count == 0:
                return {"in_stock": False, "out_of_stock": True, "low_stock": False, "stock_status": "out_of_stock"}
            if available_count <= 2:
                return {"in_stock": True, "out_of_stock": False, "low_stock": True, "stock_status": "low_stock"}
            return {"in_stock": True, "out_of_stock": False, "low_stock": False, "stock_status": "in_stock"}

        # Product-level availability
        avail = str(product_data.get("availability", product_data.get("available", "true"))).lower()
        if avail in ("false", "out_of_stock", "outofstock", "unavailable", "0"):
            return {"in_stock": False, "out_of_stock": True, "low_stock": False, "stock_status": "out_of_stock"}
        return {"in_stock": True, "out_of_stock": False, "low_stock": False, "stock_status": "in_stock"}

    def detect_sale(self, current_price: Optional[float], original_price: Optional[float]) -> dict:
        """Compute sale fields from price pair."""
        if current_price and original_price and original_price > current_price:
            discount_amount = round(original_price - current_price, 2)
            discount_percent = round((discount_amount / original_price) * 100, 1)
            return {
                "is_on_sale": True,
                "discount_amount": discount_amount,
                "discount_percent": discount_percent,
            }
        return {"is_on_sale": False, "discount_amount": None, "discount_percent": None}

    def normalize(self, raw: RawProductPayload, partial: dict) -> NormalizedProduct:
        """
        Final normalization step — fills in computed fields
        and returns a validated NormalizedProduct.
        """
        product_id = NormalizedProduct.make_id(raw.source_site, raw.product_url)
        now = datetime.now(timezone.utc).isoformat()

        partial.setdefault("product_id", product_id)
        partial.setdefault("source_site", raw.source_site)
        partial.setdefault("source_name", self.SOURCE_NAME)
        partial.setdefault("product_url", raw.product_url)
        partial.setdefault("scraped_at", now)
        partial.setdefault("first_seen_at", now)
        partial.setdefault("last_seen_at", now)
        partial.setdefault("parser_version", self.PARSER_VERSION)
        partial.setdefault("currency", "ILS")

        # Normalize sub-fields
        if "product_name" in partial:
            partial["product_name"] = normalize_name(partial["product_name"])
        if "image_urls" in partial:
            partial["image_urls"] = normalize_image_urls(partial["image_urls"], base_url=self.BASE_URL)
            if partial["image_urls"] and not partial.get("primary_image_url"):
                partial["primary_image_url"] = partial["image_urls"][0]
        if "sizes_available" in partial:
            partial["sizes_available"] = normalize_sizes(partial["sizes_available"])
        if "colors_available" in partial:
            partial["colors_available"] = normalize_colors(partial["colors_available"])

        # Warnings
        partial["warnings"] = [w.message for w in self._warnings]

        # Confidence based on extraction method
        method = raw.extraction_method
        confidence_map = {
            "json_ld": 0.95,
            "script": 0.9,
            "api": 0.98,
            "dom": 0.75,
            "heuristic": 0.5,
            "unknown": 0.6,
        }
        partial["extraction_confidence"] = confidence_map.get(method, 0.6)
        partial["raw_source_payload"] = {
            "method": method,
            "url": raw.product_url,
        }

        return NormalizedProduct(**partial)

    def healthcheck(self) -> HealthCheckResult:
        """
        Quick health check for this source.
        Verifies reachability and attempts to discover + parse one product.
        """
        result = HealthCheckResult(
            source_key=self.SOURCE_KEY,
            status="failed",
            checked_at=datetime.now(timezone.utc).isoformat(),
        )
        t_start = time.monotonic()

        # 1. Reachability
        try:
            resp = self.client.head(self.BASE_URL)
            result.reachable = resp.status_code < 500
        except Exception as e:
            result.error = f"Unreachable: {e}"
            result.response_time_ms = round((time.monotonic() - t_start) * 1000, 1)
            return result

        result.response_time_ms = round((time.monotonic() - t_start) * 1000, 1)

        # 2. Discovery
        try:
            urls = self.discover_product_urls(limit=3)
            result.discovery_ok = len(urls) > 0
            if not urls:
                result.warnings.append("Product discovery returned 0 URLs")
        except Exception as e:
            result.warnings.append(f"Discovery failed: {e}")
            urls = []

        # 3. Parse sample
        if urls:
            try:
                sample_url = urls[0]
                result.sample_product_url = sample_url
                parse_result = self.parse_product(self.fetch_product_page(sample_url))
                if parse_result.success and parse_result.product:
                    result.parse_ok = True
                    result.sample_product_name = parse_result.product.product_name
                else:
                    result.warnings.extend(parse_result.errors)
            except Exception as e:
                result.warnings.append(f"Parse failed: {e}")

        # Final status
        if result.reachable and result.discovery_ok and result.parse_ok:
            result.status = "ok"
        elif result.reachable and (result.discovery_ok or result.parse_ok):
            result.status = "degraded"

        return result

    def scrape_all(self, limit: int = 200) -> tuple[list[NormalizedProduct], SourceStats]:
        """
        Full scrape pipeline:
        1. Discover product URLs
        2. For each URL: fetch → extract → parse → normalize
        Returns (products, stats)
        """
        self.stats = SourceStats(source_key=self.SOURCE_KEY)

        log.info(f"[{self.SOURCE_NAME}] Starting scrape (limit={limit})")

        # Discover
        try:
            urls = self.discover_product_urls(limit=limit)
            self.stats.urls_discovered = len(urls)
            log.info(f"[{self.SOURCE_NAME}] Discovered {len(urls)} product URLs")
        except Exception as e:
            self.stats.error_messages.append(f"Discovery failed: {e}")
            self.stats.mark_finished()
            return [], self.stats

        products: list[NormalizedProduct] = []
        for i, url in enumerate(urls[:limit], 1):
            self.stats.products_attempted += 1
            self._warnings = []
            try:
                raw = self.fetch_product_page(url)
                result = self.parse_product(raw)

                if result.skipped:
                    self.stats.products_skipped += 1
                    log.debug(f"[{self.SOURCE_NAME}] {i} SKIP: {result.skip_reason}")
                    continue

                if result.success and result.product:
                    products.append(result.product)
                    self.stats.products_parsed += 1
                    self.stats.warning_count += len(result.warnings)

                    p = result.product
                    if p.is_on_sale:
                        self.stats.sale_products_count += 1
                    if p.out_of_stock:
                        self.stats.out_of_stock_count += 1
                    if p.is_new_collection:
                        self.stats.new_collection_products_count += 1

                    log.info(f"[{self.SOURCE_NAME}] {i}/{len(urls)} OK: {p.product_name[:60]}")
                else:
                    self.stats.products_failed += 1
                    log.warning(f"[{self.SOURCE_NAME}] {i} FAIL: {url} — {result.errors}")

            except NotFoundError:
                self.stats.products_skipped += 1
                log.debug(f"[{self.SOURCE_NAME}] {i} 404: {url}")
            except BlockedError as e:
                self.stats.products_failed += 1
                self.stats.error_messages.append(f"Blocked at {url}: {e}")
                log.warning(f"[{self.SOURCE_NAME}] {i} BLOCKED: {url}")
            except Exception as e:
                self.stats.products_failed += 1
                self.stats.error_messages.append(f"Error at {url}: {e}")
                log.error(f"[{self.SOURCE_NAME}] {i} ERROR ({url}): {e}")

        self.stats.total_live_products = self.stats.products_parsed
        self.stats.mark_finished()
        log.info(
            f"[{self.SOURCE_NAME}] Done: {self.stats.products_parsed} parsed, "
            f"{self.stats.products_failed} failed, {self.stats.products_skipped} skipped"
        )
        return products, self.stats

    # ── Shared helpers ────────────────────────────────────────────────────

    def _warn(self, field: str, message: str, severity: str = "warning", raw_value=None) -> None:
        self._warnings.append(ParseWarning(
            field=field, message=message, severity=severity, raw_value=raw_value
        ))

    def _fetch_html(self, url: str, extra_headers: Optional[dict] = None) -> str:
        resp = self.client.get(url, headers=extra_headers)
        return resp.text

    def _build_raw_payload(self, url: str, html: str, method: str = "unknown") -> RawProductPayload:
        json_ld = extract_json_ld(html)
        script_payloads = extract_script_payload(html)
        return RawProductPayload(
            source_site=self.SOURCE_KEY,
            product_url=url,
            html_snapshot=html[:50000],  # truncate for storage
            json_ld_data=json_ld,
            script_payload=script_payloads,
            extraction_method=method,
        )

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
