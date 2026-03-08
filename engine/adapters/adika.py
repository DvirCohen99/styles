"""
Adika adapter — adika.co.il
Platform: Custom (Next.js / React)
Strategy:
  1. Sitemap discovery
  2. window.__NEXT_DATA__ extraction (primary — contains full product data)
  3. JSON-LD extraction
  4. DOM fallback

Adika serves a Next.js app with product data in __NEXT_DATA__.
The server-side renders JSON payload in a <script id="__NEXT_DATA__"> tag.
This is reliable and doesn't require JavaScript execution.
"""
from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urljoin

from engine.adapters.base import BaseAdapter
from engine.schemas.product import NormalizedProduct, RawProductPayload, ProductVariant
from engine.schemas.result import ParseResult
from engine.schemas.source import SourceMeta
from engine.extraction.json_ld import find_product_json_ld, parse_product_from_json_ld, find_breadcrumbs_json_ld
from engine.extraction.script_payload import find_next_product
from engine.extraction.sitemap import SitemapDiscovery
from engine.normalization.price import normalize_price_pair
from engine.normalization.text import normalize_text, detect_gender, detect_material, detect_is_new_collection
from engine.normalization.variants import normalize_variants, normalize_sizes, normalize_colors
from engine.normalization.images import normalize_image_urls

log = logging.getLogger("engine.adapters.adika")


class AdikaAdapter(BaseAdapter):
    SOURCE_KEY = "adika"
    SOURCE_NAME = "Adika"
    BASE_URL = "https://www.adika.co.il"
    PLATFORM_FAMILY = "custom"

    def __init__(self):
        super().__init__(
            min_delay=2.5,
            max_delay=6.0,
            extra_headers={
                "Referer": "https://www.adika.co.il/",
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
            js_heavy=True,
            notes="Next.js app — __NEXT_DATA__ contains full product JSON",
        )

    def discover_category_urls(self) -> list[str]:
        return [
            f"{self.BASE_URL}/category/women",
            f"{self.BASE_URL}/category/new",
            f"{self.BASE_URL}/category/sale",
            f"{self.BASE_URL}/category/accessories",
        ]

    def discover_product_urls(self, limit: int = 200) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()

        # 1. Sitemap
        try:
            discovery = SitemapDiscovery(self.client)
            sitemap_urls = discovery.discover(
                self.BASE_URL,
                pattern=r"/product/|/item/|/p/",
                max_urls=limit,
            )
            for u in sitemap_urls:
                if u not in seen:
                    seen.add(u)
                    urls.append(u)
            if urls:
                log.info(f"[Adika] Sitemap: {len(urls)} product URLs")
                return urls[:limit]
        except Exception as e:
            log.warning(f"[Adika] Sitemap failed: {e}")

        # 2. Category pages — try Next.js data endpoint
        for cat_url in self.discover_category_urls():
            if len(urls) >= limit:
                break
            # Adika may have a JSON API at _next/data/<build_id>/...
            # but build IDs change so we scrape category HTML
            try:
                page = 1
                while len(urls) < limit:
                    paged_url = f"{cat_url}?page={page}" if page > 1 else cat_url
                    html = self._fetch_html(paged_url)
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, "lxml")

                    # Next.js rendered product links
                    links = soup.select(
                        "a[href*='/product/'], a[href*='/item/'], "
                        "a[class*='product-card'], a[class*='ProductCard']"
                    )
                    # Also check for product IDs in data attributes
                    found = 0
                    for a in links:
                        href = a.get("href", "")
                        if href:
                            full = urljoin(self.BASE_URL, href.split("?")[0])
                            if full not in seen:
                                seen.add(full)
                                urls.append(full)
                                found += 1

                    # Check Next.js __NEXT_DATA__ for product list
                    if found == 0:
                        from engine.extraction.script_payload import extract_script_payload
                        payloads = extract_script_payload(html)
                        nd = payloads.get("next_data", {})
                        page_props = nd.get("props", {}).get("pageProps", {})
                        products = (
                            page_props.get("products")
                            or page_props.get("items")
                            or page_props.get("category", {}).get("products")
                        )
                        if isinstance(products, list):
                            for item in products:
                                href = item.get("url", item.get("slug", ""))
                                if href:
                                    full = urljoin(self.BASE_URL, href)
                                    if full not in seen:
                                        seen.add(full)
                                        urls.append(full)
                                        found += 1

                    if found == 0 or page > 15:
                        break
                    page += 1
            except Exception as e:
                log.warning(f"[Adika] Category {cat_url} failed: {e}")

        return urls[:limit]

    def fetch_product_page(self, url: str) -> RawProductPayload:
        html = self._fetch_html(url)
        return self._build_raw_payload(url, html, method="unknown")

    def extract_raw_payload(self, url: str, html: str) -> RawProductPayload:
        return self._build_raw_payload(url, html, method="dom")

    def parse_product(self, raw: RawProductPayload) -> ParseResult:
        # 1. __NEXT_DATA__ (most reliable for Adika)
        if raw.script_payload:
            product_data = find_next_product(raw.script_payload)
            if product_data:
                partial = self._parse_next_data_product(product_data)
                if partial:
                    raw.extraction_method = "script"
                    product = self.normalize(raw, partial)
                    return ParseResult(
                        success=True, product_url=raw.product_url,
                        source_site=raw.source_site, product=product,
                        extraction_method="script", confidence=0.92,
                    )

        # 2. JSON-LD
        if raw.json_ld_data:
            ld = find_product_json_ld(raw.json_ld_data)
            if ld:
                partial = parse_product_from_json_ld(ld)
                partial["breadcrumbs"] = find_breadcrumbs_json_ld(raw.json_ld_data)
                partial["brand"] = partial.get("brand") or "Adika"
                raw.extraction_method = "json_ld"
                product = self.normalize(raw, partial)
                return ParseResult(
                    success=True, product_url=raw.product_url,
                    source_site=raw.source_site, product=product,
                    extraction_method="json_ld", confidence=0.85,
                )

        # 3. DOM fallback
        if raw.html_snapshot:
            return self._parse_dom(raw)

        return ParseResult(
            success=False, product_url=raw.product_url,
            source_site=raw.source_site,
            errors=["Adika: no __NEXT_DATA__, no JSON-LD, no HTML"],
        )

    def _parse_next_data_product(self, data: dict) -> Optional[dict]:
        """Parse product data from Next.js pageProps."""
        name = (
            data.get("name")
            or data.get("title")
            or data.get("productName")
            or ""
        )
        if not name:
            return None

        description = normalize_text(
            data.get("description", data.get("longDescription", ""))
        )
        price_raw = data.get("price", data.get("salePrice", data.get("currentPrice")))
        orig_raw = data.get("originalPrice", data.get("compareAtPrice", data.get("basePrice")))
        current_price, original_price = normalize_price_pair(price_raw, orig_raw)

        # Images
        images: list[str] = []
        for img_field in ["images", "media", "gallery", "photos"]:
            img_data = data.get(img_field, [])
            if isinstance(img_data, list):
                for img in img_data:
                    if isinstance(img, str):
                        images.append(img)
                    elif isinstance(img, dict):
                        src = img.get("url", img.get("src", img.get("image", "")))
                        if src:
                            images.append(src)
                if images:
                    break
        # Single image field
        if not images and data.get("image"):
            images = [data["image"]]
        images = normalize_image_urls(images, base_url=self.BASE_URL)

        # Variants / sizes / colors
        variants_raw = data.get("variants", data.get("skus", data.get("options", [])))
        sizes: list[str] = []
        colors: list[str] = []
        variants: list[ProductVariant] = []

        if isinstance(variants_raw, list) and variants_raw and isinstance(variants_raw[0], dict):
            variants = normalize_variants(variants_raw)
            for v in variants:
                if v.size and v.size not in sizes:
                    sizes.append(v.size)
                if v.color and v.color not in colors:
                    colors.append(v.color)

        # Fallback: sizes/colors as flat lists
        if not sizes:
            sizes = data.get("sizes", data.get("availableSizes", []))
        if not colors:
            colors = data.get("colors", data.get("availableColors", []))

        category = data.get("category", data.get("categoryName", data.get("department", "")))
        if isinstance(category, dict):
            category = category.get("name", "")

        fabric_type, composition = detect_material(description)
        is_new = detect_is_new_collection(name + " " + str(category))
        gender = detect_gender(name, str(category))

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
            "category": str(category) if category else None,
            "brand": data.get("brand", "Adika") or "Adika",
            "gender_target": gender,
            "is_new_collection": is_new,
            "fabric_type": fabric_type or None,
            "composition": composition or None,
            "source_product_reference": str(data.get("id", data.get("productId", ""))),
            "sku_if_available": str(data.get("sku", "") or ""),
        }

    def _parse_dom(self, raw: RawProductPayload) -> ParseResult:
        from engine.extraction.dom_selector import DOMExtractor
        from engine.extraction.heuristic import HeuristicExtractor
        dom = DOMExtractor(raw.html_snapshot)
        heur = HeuristicExtractor(raw.html_snapshot)

        name = dom.text(
            "h1[class*='product']", "h1[class*='ProductTitle']",
            "h1[class*='title']", "h1",
        ) or heur.extract_name()
        if not name:
            return ParseResult(
                success=False, product_url=raw.product_url,
                source_site=raw.source_site, errors=["Adika DOM: no product name"],
            )

        price = dom.extract_price(
            "[class*='sale-price']", "[class*='SalePrice']",
            "[class*='current-price']", "[class*='price']",
        ) or heur.extract_price()
        orig = dom.extract_price("[class*='original-price']", "[class*='OldPrice']", "[class*='was']")

        images = dom.extract_images(
            "[class*='product-image'] img", "[class*='ProductImage'] img",
            "[class*='gallery'] img", "img[data-src]",
        ) or heur.extract_images()
        images = normalize_image_urls(images, base_url=self.BASE_URL)

        sizes = dom.texts("[class*='size-option']", "[data-size]", "[class*='SizeButton']")
        colors = dom.texts("[class*='color-option']", "[data-color]", "[class*='ColorSwatch'] span")

        _, original_price = normalize_price_pair(price, orig)
        raw.extraction_method = "dom"
        product = self.normalize(raw, {
            "product_name": name,
            "current_price": price,
            "original_price": original_price,
            "image_urls": images,
            "sizes_available": normalize_sizes(sizes),
            "colors_available": normalize_colors(colors),
            "brand": "Adika",
            "breadcrumbs": dom.extract_breadcrumbs(),
        })
        return ParseResult(
            success=True, product_url=raw.product_url,
            source_site=raw.source_site, product=product,
            extraction_method="dom", confidence=0.62,
        )
