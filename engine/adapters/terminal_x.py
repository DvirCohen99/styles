"""
Terminal X adapter — terminalx.com
Platform: Magento 2 (custom theme)
Strategy:
  1. Sitemap discovery
  2. JSON-LD (Magento 2 generates Product JSON-LD)
  3. window.__INITIAL_STATE__ / Magento 2 mage-init script data
  4. DOM fallback using Magento 2 standard selectors

Note: Terminal X is a JS-heavy site. Most product data is available
in the HTML served by the server (JSON-LD + mage-init data blobs).
No Playwright needed for most products.

BLOCKER: If Terminal X uses aggressive bot detection (Cloudflare),
some requests may be blocked. The adapter handles this gracefully.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional
from urllib.parse import urljoin

from engine.adapters.base import BaseAdapter
from engine.schemas.product import NormalizedProduct, RawProductPayload, ProductVariant
from engine.schemas.result import ParseResult
from engine.schemas.source import SourceMeta
from engine.extraction.json_ld import find_product_json_ld, parse_product_from_json_ld, find_breadcrumbs_json_ld
from engine.extraction.sitemap import SitemapDiscovery
from engine.normalization.price import normalize_price_pair
from engine.normalization.text import normalize_text, detect_gender, detect_material
from engine.normalization.variants import normalize_variants, normalize_sizes, normalize_colors
from engine.normalization.images import normalize_image_urls

log = logging.getLogger("engine.adapters.terminal_x")


class TerminalXAdapter(BaseAdapter):
    SOURCE_KEY = "terminal_x"
    SOURCE_NAME = "Terminal X"
    BASE_URL = "https://www.terminalx.com"
    PLATFORM_FAMILY = "magento"

    def __init__(self):
        super().__init__(
            min_delay=3.0,
            max_delay=7.0,
            extra_headers={
                "Referer": "https://www.terminalx.com/",
                "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8",
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
            js_heavy=True,
            notes="Magento 2 — JSON-LD + mage-init data; watch for Cloudflare",
        )

    def discover_category_urls(self) -> list[str]:
        return [
            f"{self.BASE_URL}/women",
            f"{self.BASE_URL}/men",
            f"{self.BASE_URL}/kids",
            f"{self.BASE_URL}/sale",
        ]

    def discover_product_urls(self, limit: int = 200) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()

        # 1. Sitemap (Magento 2 generates product sitemaps)
        try:
            discovery = SitemapDiscovery(self.client)
            sitemap_urls = discovery.discover(
                self.BASE_URL,
                pattern=r"\.html$|/p-\d+",
                max_urls=limit,
            )
            for u in sitemap_urls:
                if ".html" in u and u not in seen:
                    seen.add(u)
                    urls.append(u)
            if urls:
                log.info(f"[TerminalX] Sitemap: {len(urls)} product URLs")
                return urls[:limit]
        except Exception as e:
            log.warning(f"[TerminalX] Sitemap failed: {e}")

        # 2. Category HTML pages
        for cat_url in self.discover_category_urls():
            if len(urls) >= limit:
                break
            try:
                page = 1
                while len(urls) < limit:
                    paged = f"{cat_url}?p={page}" if page > 1 else cat_url
                    html = self._fetch_html(paged)
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, "lxml")
                    # Magento 2 product links
                    product_links = soup.select(
                        "a.product-item-link, "
                        "a[class*='product-item'], "
                        "a[href*='.html']:not([href*='category'])"
                    )
                    found = 0
                    for a in product_links:
                        href = a.get("href", "")
                        if href and ".html" in href:
                            full = urljoin(self.BASE_URL, href.split("?")[0])
                            if full not in seen:
                                seen.add(full)
                                urls.append(full)
                                found += 1
                    if found == 0 or page > 15:
                        break
                    page += 1
            except Exception as e:
                log.warning(f"[TerminalX] Category {cat_url} failed: {e}")

        return urls[:limit]

    def fetch_product_page(self, url: str) -> RawProductPayload:
        html = self._fetch_html(url)
        raw = self._build_raw_payload(url, html, method="unknown")
        # Try to extract Magento-specific data blobs
        magento_data = self._extract_magento_data(html)
        if magento_data:
            raw.script_payload = raw.script_payload or {}
            raw.script_payload["magento_product"] = magento_data
        return raw

    def extract_raw_payload(self, url: str, html: str) -> RawProductPayload:
        raw = self._build_raw_payload(url, html, method="dom")
        magento_data = self._extract_magento_data(html)
        if magento_data:
            raw.script_payload = raw.script_payload or {}
            raw.script_payload["magento_product"] = magento_data
        return raw

    def _extract_magento_data(self, html: str) -> Optional[dict]:
        """
        Extract Magento 2 product data from:
        - data-mage-init attributes
        - window.dataLayer product pushes
        - Inline configurable product JSON
        """
        # Pattern: [data-role=swatch-options] or inline product config
        patterns = [
            r'\[data-role=swatch-options\][^>]*>(.*?)</script',
            r'JsonConfig\s*=\s*(\{.*?\})\s*;',
            r'"configurable_simple_product"\s*:\s*(\{.+?\})',
            r'ProductPrice\s*=\s*(\{.+?\})',
            # dataLayer
            r'dataLayer\.push\s*\(\s*(\{.*?"ecommerce".*?\})\s*\)',
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                raw_json = match.group(1)
                # Balance braces
                depth = 0
                end = 0
                for i, ch in enumerate(raw_json):
                    if ch == "{":
                        depth += 1
                    elif ch == "}":
                        depth -= 1
                        if depth == 0:
                            end = i + 1
                            break
                try:
                    return json.loads(raw_json[:end])
                except (json.JSONDecodeError, ValueError):
                    continue

        return None

    def parse_product(self, raw: RawProductPayload) -> ParseResult:
        # 1. JSON-LD (most reliable for Magento 2)
        if raw.json_ld_data:
            ld = find_product_json_ld(raw.json_ld_data)
            if ld:
                partial = parse_product_from_json_ld(ld)
                partial["breadcrumbs"] = find_breadcrumbs_json_ld(raw.json_ld_data)
                # Enrich with Magento script data
                if raw.script_payload and raw.script_payload.get("magento_product"):
                    self._enrich_from_magento(raw.script_payload["magento_product"], partial)
                raw.extraction_method = "json_ld"
                product = self.normalize(raw, partial)
                return ParseResult(
                    success=True, product_url=raw.product_url,
                    source_site=raw.source_site, product=product,
                    extraction_method="json_ld", confidence=0.88,
                )

        # 2. Script payload
        if raw.script_payload and raw.script_payload.get("magento_product"):
            partial = self._parse_magento_data(raw.script_payload["magento_product"])
            if partial:
                raw.extraction_method = "script"
                product = self.normalize(raw, partial)
                return ParseResult(
                    success=True, product_url=raw.product_url,
                    source_site=raw.source_site, product=product,
                    extraction_method="script", confidence=0.82,
                )

        # 3. DOM fallback
        if raw.html_snapshot:
            return self._parse_dom(raw)

        return ParseResult(
            success=False, product_url=raw.product_url,
            source_site=raw.source_site,
            errors=["TerminalX: no parseable data (possible bot detection)"],
        )

    def _enrich_from_magento(self, magento_data: dict, partial: dict) -> None:
        """Merge Magento configurable product data into partial result."""
        # Swatch / size data
        options = magento_data.get("attributes", magento_data.get("options", {}))
        if isinstance(options, dict):
            for attr_id, attr_data in options.items():
                if not isinstance(attr_data, dict):
                    continue
                code = attr_data.get("code", "")
                options_list = attr_data.get("options", [])
                if "size" in code.lower() or "מידה" in code:
                    sizes = [o.get("label", "") for o in options_list]
                    partial.setdefault("sizes_available", normalize_sizes([s for s in sizes if s]))
                elif "color" in code.lower() or "צבע" in code:
                    colors = [o.get("label", "") for o in options_list]
                    partial.setdefault("colors_available", normalize_colors([c for c in colors if c]))

    def _parse_magento_data(self, data: dict) -> Optional[dict]:
        """Parse extracted Magento JSON blob."""
        name = data.get("productName", data.get("name", ""))
        if not name:
            return None
        price_raw = data.get("price", data.get("finalPrice", {}).get("amount", 0))
        orig_raw = data.get("regularPrice", data.get("basePrice", {}).get("amount", 0))
        current_price, original_price = normalize_price_pair(price_raw, orig_raw)
        return {
            "product_name": name,
            "current_price": current_price,
            "original_price": original_price,
        }

    def _parse_dom(self, raw: RawProductPayload) -> ParseResult:
        """Magento 2 DOM selectors."""
        from engine.extraction.dom_selector import DOMExtractor
        from engine.extraction.heuristic import HeuristicExtractor
        dom = DOMExtractor(raw.html_snapshot)
        heur = HeuristicExtractor(raw.html_snapshot)

        name = dom.text(
            "h1.page-title span",
            "h1[class*='product-name']",
            "h1.product-name",
            "h1",
        ) or heur.extract_name()
        if not name:
            return ParseResult(
                success=False, product_url=raw.product_url,
                source_site=raw.source_site, errors=["TerminalX DOM: no name"],
            )

        price = dom.extract_price(
            ".price-box .special-price .price",
            ".price-box .price",
            "[data-price-type='finalPrice'] .price",
            ".product-info-price .price",
        ) or heur.extract_price()

        orig = dom.extract_price(
            ".price-box .old-price .price",
            "[data-price-type='oldPrice'] .price",
        )

        images = dom.extract_images(
            ".fotorama__img", ".gallery-placeholder img",
            ".product-image-main img", "img.photo",
        ) or heur.extract_images()
        images = normalize_image_urls(images, base_url=self.BASE_URL)

        sizes = dom.texts(
            ".swatch-option.text",
            "[data-option-type='0']",
            ".size-option",
        )
        colors = dom.texts(
            ".swatch-option.color",
            "[data-option-type='1']",
        )

        _, original_price = normalize_price_pair(price, orig)
        raw.extraction_method = "dom"
        product = self.normalize(raw, {
            "product_name": name,
            "current_price": price,
            "original_price": original_price,
            "image_urls": images,
            "sizes_available": normalize_sizes(sizes),
            "colors_available": normalize_colors(colors),
            "breadcrumbs": dom.extract_breadcrumbs(),
        })
        return ParseResult(
            success=True, product_url=raw.product_url,
            source_site=raw.source_site, product=product,
            extraction_method="dom", confidence=0.65,
        )
