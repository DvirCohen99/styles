"""
Shopify platform mixin.

Provides Shopify-specific helpers shared across all Shopify-based adapters:
- /products/<handle>.json product fetching
- /collections/<slug>.json + pagination
- Sitemap-based product URL discovery (/sitemap.xml -> product_urls sitemap)
- Shopify variant parsing
"""
from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urljoin

from engine.schemas.product import NormalizedProduct, RawProductPayload, ProductVariant
from engine.schemas.result import ParseResult, ParseWarning
from engine.extraction.sitemap import SitemapDiscovery
from engine.normalization.price import normalize_price_pair
from engine.normalization.text import normalize_text, detect_gender, detect_material, detect_is_new_collection
from engine.normalization.variants import normalize_variants, normalize_sizes, normalize_colors
from engine.normalization.images import normalize_image_urls

log = logging.getLogger("engine.adapters.shopify")


class ShopifyMixin:
    """
    Mixin for Shopify-based adapters.
    Assumes self.client, self.BASE_URL, self.SOURCE_KEY, self.SOURCE_NAME are set.
    """

    def _shopify_discover_product_urls(self, limit: int = 500) -> list[str]:
        """
        Discover product URLs via:
        1. Sitemap XML (fastest, most complete)
        2. /collections/all.json pagination (fallback)
        """
        urls: list[str] = []
        seen: set[str] = set()

        # 1. Sitemap approach
        try:
            discovery = SitemapDiscovery(self.client)
            sitemap_urls = discovery.discover(
                self.BASE_URL,
                pattern=r"/products/",
                max_urls=limit,
            )
            for u in sitemap_urls:
                if u not in seen:
                    seen.add(u)
                    urls.append(u)
            if urls:
                log.info(f"[{self.SOURCE_NAME}] Sitemap found {len(urls)} product URLs")
                return urls[:limit]
        except Exception as e:
            log.warning(f"[{self.SOURCE_NAME}] Sitemap discovery failed: {e}")

        # 2. /collections/all.json fallback
        try:
            page = 1
            per_page = 250
            while len(urls) < limit:
                api_url = f"{self.BASE_URL}/collections/all/products.json"
                resp = self.client.get_json(api_url, params={"limit": per_page, "page": page})
                products = resp.get("products", [])
                if not products:
                    break
                for p in products:
                    handle = p.get("handle", "")
                    if handle:
                        product_url = f"{self.BASE_URL}/products/{handle}"
                        if product_url not in seen:
                            seen.add(product_url)
                            urls.append(product_url)
                if len(products) < per_page:
                    break
                page += 1
            log.info(f"[{self.SOURCE_NAME}] /collections/all found {len(urls)} products")
        except Exception as e:
            log.warning(f"[{self.SOURCE_NAME}] /collections/all fallback failed: {e}")

        return urls[:limit]

    def _shopify_fetch_product_json(self, product_url: str) -> Optional[dict]:
        """Fetch Shopify product JSON from <url>.json endpoint."""
        json_url = product_url.split("?")[0].rstrip("/") + ".json"
        try:
            resp = self.client.get_json(json_url)
            return resp.get("product") or resp
        except Exception as e:
            log.debug(f"[{self.SOURCE_NAME}] .json fetch failed for {product_url}: {e}")
            return None

    def _shopify_parse_product_json(self, data: dict, url: str) -> Optional[dict]:
        """Parse Shopify product JSON into normalized field dict."""
        if not data or not data.get("title"):
            return None

        title = data.get("title", "")
        description_html = data.get("body_html", "")
        from bs4 import BeautifulSoup
        description = normalize_text(BeautifulSoup(description_html or "", "lxml").get_text()) if description_html else ""

        variants_raw = data.get("variants", [])
        variants = normalize_variants(variants_raw)

        # Price from first variant
        first_var = variants_raw[0] if variants_raw else {}
        current_price, original_price = normalize_price_pair(
            first_var.get("price"),
            first_var.get("compare_at_price"),
            shopify_cents=False,
        )

        # Images
        image_list = data.get("images", [])
        image_urls = [
            img["src"].split("?")[0] for img in image_list if img.get("src")
        ]
        image_urls = normalize_image_urls(image_urls, base_url=self.BASE_URL)

        # Options
        sizes: list[str] = []
        colors: list[str] = []
        for opt in data.get("options", []):
            name = (opt.get("name") or "").lower()
            values = opt.get("values", [])
            if any(k in name for k in ["size", "מידה", "גודל"]):
                sizes.extend(values)
            elif any(k in name for k in ["color", "colour", "צבע", "colore"]):
                colors.extend(values)

        tags = data.get("tags", [])
        tags = tags if isinstance(tags, list) else [t.strip() for t in str(tags).split(",")]

        category = data.get("product_type", "") or ""
        vendor = data.get("vendor", "") or ""
        handle = data.get("handle", "")

        fabric_type, composition = detect_material(description)
        is_new = detect_is_new_collection(title + " " + " ".join(tags))
        gender = detect_gender(title, category)

        # Stock: check if any variant is available
        in_stock = any(
            v.get("available", True) for v in variants_raw
        ) if variants_raw else True

        return {
            "product_name": title,
            "original_product_title": title,
            "short_description": description[:300],
            "original_description": description,
            "current_price": current_price,
            "original_price": original_price,
            "image_urls": image_urls,
            "colors_available": normalize_colors(colors),
            "sizes_available": normalize_sizes(sizes),
            "color_variant_objects": variants,
            "category": category or None,
            "brand": vendor or None,
            "gender_target": gender,
            "is_new_collection": is_new,
            "fabric_type": fabric_type or None,
            "composition": composition or None,
            "source_product_reference": str(data.get("id", "")),
            "in_stock": in_stock,
            "out_of_stock": not in_stock,
        }

    def _shopify_fetch_product_page(self, url: str) -> RawProductPayload:
        """
        Fetch a Shopify product page.
        Tries .json endpoint first, falls back to HTML.
        """
        # Try JSON first
        json_data = self._shopify_fetch_product_json(url)
        if json_data:
            return RawProductPayload(
                source_site=self.SOURCE_KEY,
                product_url=url,
                script_payload={"shopify_product": json_data},
                extraction_method="api",
            )

        # HTML fallback
        html = self._fetch_html(url)
        return self._build_raw_payload(url, html, method="dom")

    def _shopify_parse_product(self, raw: RawProductPayload) -> ParseResult:
        """Generic Shopify product parse."""
        warnings: list[ParseWarning] = []
        self._warnings = []

        # Try API payload first
        shopify_data = None
        if raw.script_payload:
            shopify_data = (
                raw.script_payload.get("shopify_product")
                or raw.script_payload.get("shopify_analytics", {}).get("meta", {}).get("product")
            )

        if shopify_data:
            partial = self._shopify_parse_product_json(shopify_data, raw.product_url)
            if partial:
                raw.extraction_method = "api"
                product = self.normalize(raw, partial)
                return ParseResult(
                    success=True,
                    product_url=raw.product_url,
                    source_site=raw.source_site,
                    product=product,
                    warnings=self._warnings,
                    extraction_method="api",
                    confidence=0.98,
                )

        # JSON-LD fallback
        if raw.json_ld_data:
            from engine.extraction.json_ld import find_product_json_ld, parse_product_from_json_ld, find_breadcrumbs_json_ld
            product_ld = find_product_json_ld(raw.json_ld_data)
            if product_ld:
                partial = parse_product_from_json_ld(product_ld)
                partial["breadcrumbs"] = find_breadcrumbs_json_ld(raw.json_ld_data)
                raw.extraction_method = "json_ld"
                product = self.normalize(raw, partial)
                return ParseResult(
                    success=True,
                    product_url=raw.product_url,
                    source_site=raw.source_site,
                    product=product,
                    warnings=self._warnings,
                    extraction_method="json_ld",
                    confidence=0.9,
                )

        # DOM fallback
        if raw.html_snapshot:
            from engine.extraction.dom_selector import DOMExtractor
            dom = DOMExtractor(raw.html_snapshot)
            name = dom.text("h1.product__title", "h1[class*='product']", "h1")
            if not name:
                return ParseResult(
                    success=False,
                    product_url=raw.product_url,
                    source_site=raw.source_site,
                    errors=["Could not extract product name from DOM"],
                )
            images = dom.extract_images(
                ".product__media img", ".product-single__photo img", ".product-gallery img", "img"
            )
            price = dom.extract_price(
                "[class*='price--sale']", ".price__regular .price-item",
                "[class*='current-price']", "[class*='price']"
            )
            partial = {
                "product_name": name,
                "image_urls": images,
                "current_price": price,
                "breadcrumbs": dom.extract_breadcrumbs(),
            }
            raw.extraction_method = "dom"
            product = self.normalize(raw, partial)
            return ParseResult(
                success=True,
                product_url=raw.product_url,
                source_site=raw.source_site,
                product=product,
                warnings=self._warnings,
                extraction_method="dom",
                confidence=0.7,
            )

        return ParseResult(
            success=False,
            product_url=raw.product_url,
            source_site=raw.source_site,
            errors=["No parseable data found (no JSON, no JSON-LD, no HTML)"],
        )
