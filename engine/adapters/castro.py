"""
Castro adapter — castro.com
Platform: Custom (Israeli brand)
Strategy:
  1. Sitemap discovery
  2. JSON-LD extraction (castro has Product JSON-LD)
  3. Script payload (castro embeds product data in __INITIAL_STATE__ or similar)
  4. DOM fallback

Note: Castro's internal API endpoints are not officially documented.
Approach: HTML + JSON-LD is most reliable.
"""
from __future__ import annotations

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
from engine.normalization.text import normalize_text, detect_gender
from engine.normalization.variants import normalize_variants, normalize_sizes, normalize_colors
from engine.normalization.images import normalize_image_urls

log = logging.getLogger("engine.adapters.castro")


class CastroAdapter(BaseAdapter):
    SOURCE_KEY = "castro"
    SOURCE_NAME = "Castro"
    BASE_URL = "https://www.castro.com"
    PLATFORM_FAMILY = "custom"

    CATEGORY_SLUGS = ["women", "men", "kids", "sale"]

    def __init__(self):
        super().__init__(
            min_delay=2.5,
            max_delay=6.0,
            extra_headers={"Referer": "https://www.castro.com/"},
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
        )

    def discover_category_urls(self) -> list[str]:
        return [f"{self.BASE_URL}/he/{slug}" for slug in self.CATEGORY_SLUGS]

    def discover_product_urls(self, limit: int = 200) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()

        # 1. Sitemap
        try:
            discovery = SitemapDiscovery(self.client)
            sitemap_urls = discovery.discover(
                self.BASE_URL,
                pattern=r"/he/product/|/product/",
                max_urls=limit,
            )
            for u in sitemap_urls:
                if u not in seen:
                    seen.add(u)
                    urls.append(u)
            if urls:
                log.info(f"[Castro] Sitemap: {len(urls)} product URLs")
                return urls[:limit]
        except Exception as e:
            log.warning(f"[Castro] Sitemap failed: {e}")

        # 2. Category HTML pages
        for slug in self.CATEGORY_SLUGS:
            if len(urls) >= limit:
                break
            try:
                page = 1
                while len(urls) < limit:
                    cat_url = f"{self.BASE_URL}/he/{slug}?page={page}"
                    html = self._fetch_html(cat_url)
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, "lxml")
                    found = 0
                    for a in soup.select("a[href*='/product/'], a[href*='/he/product/']"):
                        href = a.get("href", "")
                        if href:
                            full = urljoin(self.BASE_URL, href.split("?")[0])
                            if full not in seen:
                                seen.add(full)
                                urls.append(full)
                                found += 1
                    if found == 0 or page > 10:
                        break
                    page += 1
            except Exception as e:
                log.warning(f"[Castro] Category {slug} failed: {e}")

        return urls[:limit]

    def fetch_product_page(self, url: str) -> RawProductPayload:
        html = self._fetch_html(url)
        return self._build_raw_payload(url, html, method="unknown")

    def extract_raw_payload(self, url: str, html: str) -> RawProductPayload:
        return self._build_raw_payload(url, html, method="dom")

    def parse_product(self, raw: RawProductPayload) -> ParseResult:
        # 1. JSON-LD (Castro has Product schema)
        if raw.json_ld_data:
            ld = find_product_json_ld(raw.json_ld_data)
            if ld:
                partial = parse_product_from_json_ld(ld)
                partial["breadcrumbs"] = find_breadcrumbs_json_ld(raw.json_ld_data)
                partial["brand"] = partial.get("brand") or "Castro"
                # Enrich from script payload
                self._enrich_from_script(raw, partial)
                raw.extraction_method = "json_ld"
                product = self.normalize(raw, partial)
                return ParseResult(
                    success=True, product_url=raw.product_url,
                    source_site=raw.source_site, product=product,
                    extraction_method="json_ld", confidence=0.88,
                )

        # 2. Script payload
        if raw.script_payload:
            partial = self._parse_script_payload(raw)
            if partial:
                raw.extraction_method = "script"
                product = self.normalize(raw, partial)
                return ParseResult(
                    success=True, product_url=raw.product_url,
                    source_site=raw.source_site, product=product,
                    extraction_method="script", confidence=0.85,
                )

        # 3. DOM fallback
        if raw.html_snapshot:
            return self._parse_dom(raw)

        return ParseResult(
            success=False, product_url=raw.product_url,
            source_site=raw.source_site,
            errors=["Castro: no JSON-LD, no script payload, no HTML"],
        )

    def _enrich_from_script(self, raw: RawProductPayload, partial: dict) -> None:
        """Try to add variant / size data from script payload."""
        if not raw.script_payload:
            return
        payload = raw.script_payload

        # Look for product data in various patterns
        for key in ["initial_state", "preloaded_state", "product_data", "pdp_data"]:
            data = payload.get(key)
            if not isinstance(data, dict):
                continue
            # Navigate to product
            product = data.get("product", data.get("currentProduct", data.get("pdp", {})))
            if not isinstance(product, dict):
                continue

            variants_raw = product.get("variants", product.get("skus", []))
            if variants_raw:
                variants = normalize_variants(variants_raw)
                partial["color_variant_objects"] = variants

                sizes: list[str] = []
                colors: list[str] = []
                for v in variants:
                    if v.size and v.size not in sizes:
                        sizes.append(v.size)
                    if v.color and v.color not in colors:
                        colors.append(v.color)
                partial.setdefault("sizes_available", normalize_sizes(sizes))
                partial.setdefault("colors_available", normalize_colors(colors))
            break

    def _parse_script_payload(self, raw: RawProductPayload) -> Optional[dict]:
        """Try to extract product from inline script payloads."""
        from engine.extraction.script_payload import deep_get
        payload = raw.script_payload or {}

        for key in ["initial_state", "next_data", "pdp_data", "product_data"]:
            data = payload.get(key)
            if not isinstance(data, dict):
                continue

            # Try various paths
            product = (
                deep_get(data, "product")
                or deep_get(data, "props", "pageProps", "product")
                or deep_get(data, "pdp", "product")
                or deep_get(data, "currentProduct")
            )
            if product and isinstance(product, dict) and product.get("name"):
                name = product.get("name", product.get("title", ""))
                price_raw = product.get("price", product.get("salePrice", 0))
                orig_raw = product.get("originalPrice", product.get("compareAtPrice", 0))
                current_price, original_price = normalize_price_pair(price_raw, orig_raw)
                images_raw = product.get("images", product.get("media", []))
                images = []
                for img in images_raw:
                    src = img.get("src", img.get("url", img if isinstance(img, str) else ""))
                    if src:
                        images.append(str(src))
                images = normalize_image_urls(images, base_url=self.BASE_URL)
                return {
                    "product_name": name,
                    "current_price": current_price,
                    "original_price": original_price,
                    "image_urls": images,
                    "brand": "Castro",
                }
        return None

    def _parse_dom(self, raw: RawProductPayload) -> ParseResult:
        from engine.extraction.dom_selector import DOMExtractor
        from engine.extraction.heuristic import HeuristicExtractor
        dom = DOMExtractor(raw.html_snapshot)
        heur = HeuristicExtractor(raw.html_snapshot)

        name = dom.text(
            "h1.product__name", "h1[class*='product']",
            ".product-title h1", "h1",
        ) or heur.extract_name()

        if not name:
            return ParseResult(
                success=False, product_url=raw.product_url,
                source_site=raw.source_site,
                errors=["Castro DOM: no product name"],
            )

        price = dom.extract_price(
            ".product-price__sale", ".price-sale",
            "[class*='current-price']", "[class*='price']",
        ) or heur.extract_price()

        orig_price = dom.extract_price(".price-original", "[class*='was-price']", "[class*='old-price']")
        images = dom.extract_images(
            ".product-gallery img", ".product-image img",
            ".swiper-slide img", "img[data-src*='castro']",
        ) or heur.extract_images()
        images = normalize_image_urls(images, base_url=self.BASE_URL)

        sizes = dom.texts("[class*='size-option']", "[data-size]", ".size-selector__item")
        colors = dom.attrs("[class*='color-swatch'], [data-color]", "title") or \
                 dom.texts("[class*='color-name']")

        _, original_price = normalize_price_pair(price, orig_price)

        raw.extraction_method = "dom"
        product = self.normalize(raw, {
            "product_name": name,
            "current_price": price,
            "original_price": original_price,
            "image_urls": images,
            "sizes_available": normalize_sizes(sizes),
            "colors_available": normalize_colors(colors),
            "brand": "Castro",
            "breadcrumbs": dom.extract_breadcrumbs(),
        })
        return ParseResult(
            success=True, product_url=raw.product_url,
            source_site=raw.source_site, product=product,
            extraction_method="dom", confidence=0.65,
        )
