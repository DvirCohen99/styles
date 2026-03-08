"""
Zara Israel adapter — zara.com/il
Platform: Custom (Inditex) with internal REST API
Strategy:
  1. Use Zara's catalog API to enumerate categories and products
  2. Fetch product detail via API endpoint
  3. No browser needed — pure API approach

API base: https://www.zara.com/itxrest/3/catalog/store/{STORE_ID}/
"""
from __future__ import annotations

import logging
import re
from typing import Optional

from engine.adapters.base import BaseAdapter
from engine.schemas.product import NormalizedProduct, RawProductPayload, ProductVariant
from engine.schemas.result import ParseResult, ParseWarning
from engine.schemas.source import SourceMeta
from engine.normalization.price import normalize_price_pair
from engine.normalization.text import normalize_text, detect_gender, detect_material
from engine.normalization.variants import normalize_colors, normalize_sizes
from engine.normalization.images import normalize_image_urls

log = logging.getLogger("engine.adapters.zara")

STORE_ID = "11719"   # Zara IL store
LANG_ID = "2"        # Hebrew
API_BASE = f"https://www.zara.com/itxrest/3/catalog/store/{STORE_ID}"

# Zara IL category IDs (sourced from API)
ZARA_CATEGORY_IDS = {
    "woman": "2524048",
    "man": "2524049",
    "girl": "2524050",
    "boy": "2524051",
}


class ZaraAdapter(BaseAdapter):
    SOURCE_KEY = "zara"
    SOURCE_NAME = "Zara Israel"
    BASE_URL = "https://www.zara.com"
    PLATFORM_FAMILY = "custom"

    def __init__(self):
        super().__init__(
            min_delay=2.0,
            max_delay=5.0,
            extra_headers={
                "Referer": "https://www.zara.com/il/",
                "Origin": "https://www.zara.com",
                "x-requested-with": "XMLHttpRequest",
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
            has_api=True,
            js_heavy=True,
        )

    def discover_category_urls(self) -> list[str]:
        return [
            "https://www.zara.com/il/en/woman-new-in-l1180.html",
            "https://www.zara.com/il/en/man-new-in-l837.html",
            "https://www.zara.com/il/en/girl-new-in-l1388.html",
        ]

    def discover_product_urls(self, limit: int = 200) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        per_cat = max(10, limit // len(ZARA_CATEGORY_IDS))

        for cat_name, cat_id in ZARA_CATEGORY_IDS.items():
            if len(urls) >= limit:
                break
            try:
                products = self._fetch_category_products(cat_id, per_cat)
                for p in products:
                    product_url = self._build_product_url(p)
                    if product_url and product_url not in seen:
                        seen.add(product_url)
                        urls.append(product_url)
            except Exception as e:
                log.warning(f"[Zara] Category {cat_name} ({cat_id}) failed: {e}")

        log.info(f"[Zara] Discovered {len(urls)} product URLs")
        return urls[:limit]

    def _fetch_category_products(self, category_id: str, limit: int) -> list[dict]:
        """Fetch product list for a category via Zara API."""
        endpoint = f"{API_BASE}/category/{category_id}/product"
        params = {"languageId": LANG_ID, "ajax": "true", "offset": 0, "limit": limit}
        data = self.client.get_json(endpoint, params=params)

        products = []
        for group in data.get("productGroups", []):
            for elem in group.get("elements", []):
                components = elem.get("commercialComponents", [])
                for comp in components:
                    if comp.get("id") and comp.get("name"):
                        products.append(comp)
                        if len(products) >= limit:
                            return products
        return products

    def _build_product_url(self, product_data: dict) -> Optional[str]:
        """Build product page URL from API data."""
        seo_keyword = product_data.get("seo", {}).get("keyword", "")
        product_id = product_data.get("id", "")
        if seo_keyword and product_id:
            return f"https://www.zara.com/il/en/{seo_keyword}-p{product_id}.html"
        return None

    def fetch_product_page(self, url: str) -> RawProductPayload:
        """Fetch via Zara product detail API."""
        product_id = self._extract_product_id(url)
        if not product_id:
            # Fall back to HTML
            html = self._fetch_html(url)
            return self._build_raw_payload(url, html, method="dom")

        try:
            endpoint = f"{API_BASE}/product/detail"
            data = self.client.get_json(endpoint, params={"languageId": LANG_ID, "productId": product_id})
            return RawProductPayload(
                source_site=self.SOURCE_KEY,
                product_url=url,
                script_payload={"zara_product": data},
                extraction_method="api",
            )
        except Exception as e:
            log.warning(f"[Zara] API fetch failed for {url}: {e}")
            html = self._fetch_html(url)
            return self._build_raw_payload(url, html, method="dom")

    def extract_raw_payload(self, url: str, html: str) -> RawProductPayload:
        return self._build_raw_payload(url, html, method="dom")

    def _extract_product_id(self, url: str) -> Optional[str]:
        """Extract Zara product ID from URL: ...-p12345678.html"""
        match = re.search(r"-p(\d+)\.html", url)
        return match.group(1) if match else None

    def parse_product(self, raw: RawProductPayload) -> ParseResult:
        # Try API payload
        zara_data = None
        if raw.script_payload:
            zara_data = raw.script_payload.get("zara_product")

        if zara_data:
            partial = self._parse_zara_api(zara_data, raw.product_url)
            if partial:
                raw.extraction_method = "api"
                product = self.normalize(raw, partial)
                return ParseResult(
                    success=True,
                    product_url=raw.product_url,
                    source_site=raw.source_site,
                    product=product,
                    extraction_method="api",
                    confidence=0.97,
                )

        # JSON-LD fallback
        if raw.json_ld_data:
            from engine.extraction.json_ld import find_product_json_ld, parse_product_from_json_ld
            ld = find_product_json_ld(raw.json_ld_data)
            if ld:
                partial = parse_product_from_json_ld(ld)
                partial["brand"] = "Zara"
                raw.extraction_method = "json_ld"
                product = self.normalize(raw, partial)
                return ParseResult(
                    success=True, product_url=raw.product_url,
                    source_site=raw.source_site, product=product,
                    extraction_method="json_ld", confidence=0.85,
                )

        # DOM fallback
        if raw.html_snapshot:
            return self._parse_dom(raw)

        return ParseResult(
            success=False, product_url=raw.product_url,
            source_site=raw.source_site,
            errors=["No parseable data — Zara may require session cookies"],
        )

    def _parse_zara_api(self, data: dict, url: str) -> Optional[dict]:
        """Parse Zara product API response."""
        name = data.get("name", "")
        if not name:
            return None

        description = normalize_text(data.get("description", ""))
        section = data.get("sectionName", "")
        family = data.get("familyName", "")

        # Price (Zara returns in cents as int)
        raw_price = data.get("price", 0)
        raw_orig = data.get("oldPrice", data.get("originalPrice", 0))
        current_price = raw_price / 100 if isinstance(raw_price, int) and raw_price else None
        original_price = raw_orig / 100 if isinstance(raw_orig, int) and raw_orig else None
        if original_price and current_price and original_price <= current_price:
            original_price = None

        # Colors and images
        colors: list[str] = []
        sizes: list[str] = []
        images: list[str] = []
        variants: list[ProductVariant] = []

        detail = data.get("detail", {})
        for color_entry in detail.get("colors", []):
            color_name = color_entry.get("name", "")
            if color_name:
                colors.append(color_name)

            # Images per color
            for media in color_entry.get("xmedia", []):
                path = media.get("path", "")
                img_name = media.get("name", "")
                timestamp = media.get("timestamp", "")
                if path and img_name:
                    img_url = f"https://static.zara.net/photos/{path}/{img_name}/w/750/{img_name}.jpg"
                    if timestamp:
                        img_url += f"?ts={timestamp}"
                    if img_url not in images:
                        images.append(img_url)

            # Sizes per color
            for size_entry in color_entry.get("sizes", []):
                label = size_entry.get("name", "")
                if label and label not in sizes:
                    sizes.append(label)
                stock_avail = size_entry.get("availability", "in_stock")
                avail = stock_avail not in ("out_of_stock", "0", "unavailable")
                variants.append(ProductVariant(
                    variant_id=str(size_entry.get("id", "")),
                    sku=str(size_entry.get("sku", "")),
                    color=color_name or None,
                    size=label or None,
                    price=current_price,
                    in_stock=avail,
                ))

        images = normalize_image_urls(images, base_url=self.BASE_URL)
        gender = detect_gender(name, section)
        fabric_type, composition = detect_material(description)

        return {
            "product_name": name,
            "original_product_title": name,
            "short_description": description[:300],
            "original_description": description,
            "current_price": current_price,
            "original_price": original_price,
            "image_urls": images,
            "colors_available": normalize_colors(colors),
            "sizes_available": normalize_sizes(sizes),
            "color_variant_objects": variants,
            "category": section or None,
            "subcategory": family or None,
            "brand": "Zara",
            "gender_target": gender,
            "fabric_type": fabric_type or None,
            "composition": composition or None,
            "source_product_reference": str(data.get("productId", data.get("id", ""))),
        }

    def _parse_dom(self, raw: RawProductPayload) -> ParseResult:
        """Last-resort DOM parse for Zara."""
        from engine.extraction.dom_selector import DOMExtractor
        from engine.extraction.heuristic import HeuristicExtractor
        dom = DOMExtractor(raw.html_snapshot)
        heuristic = HeuristicExtractor(raw.html_snapshot)

        name = dom.text(
            "h1.product-detail-info__header-name",
            "h1[class*='product-name']",
            "h1",
        ) or heuristic.extract_name()

        if not name:
            return ParseResult(
                success=False, product_url=raw.product_url,
                source_site=raw.source_site,
                errors=["DOM parse: no product name found"],
            )

        price = dom.extract_price(
            ".price__amount-current",
            "[class*='price--on-sale']",
            "[class*='price']",
        ) or heuristic.extract_price()

        images = dom.extract_images(
            ".media-image img", ".product-detail-images img", "img[data-src]"
        ) or heuristic.extract_images()
        images = normalize_image_urls(images, base_url=self.BASE_URL)

        raw.extraction_method = "dom"
        product = self.normalize(raw, {
            "product_name": name,
            "current_price": price,
            "image_urls": images,
            "brand": "Zara",
            "breadcrumbs": dom.extract_breadcrumbs(),
        })
        return ParseResult(
            success=True, product_url=raw.product_url,
            source_site=raw.source_site, product=product,
            extraction_method="dom", confidence=0.6,
        )
