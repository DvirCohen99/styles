"""
CStyle adapter — cstyle.co.il
Platform: WooCommerce
Strategy:
  1. WooCommerce REST API (/wp-json/wc/v3/products) if accessible
  2. Sitemap discovery
  3. JSON-LD (WooCommerce generates Product JSON-LD)
  4. DOM fallback

Note: WooCommerce public API requires no auth for read-only product data
on stores that enable it. If disabled, fall back to HTML scraping.
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
from engine.normalization.text import normalize_text, detect_gender, detect_material
from engine.normalization.variants import normalize_variants, normalize_sizes, normalize_colors
from engine.normalization.images import normalize_image_urls

log = logging.getLogger("engine.adapters.cstyle")


class CStyleAdapter(BaseAdapter):
    SOURCE_KEY = "cstyle"
    SOURCE_NAME = "CStyle"
    BASE_URL = "https://www.cstyle.co.il"
    PLATFORM_FAMILY = "woocommerce"

    WC_API_BASE = "https://www.cstyle.co.il/wp-json/wc/v3"

    def __init__(self):
        super().__init__(
            min_delay=2.5,
            max_delay=5.0,
            extra_headers={"Referer": "https://www.cstyle.co.il/"},
        )
        self._wc_api_available: Optional[bool] = None

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
            notes="WooCommerce — try public API first, then JSON-LD + DOM",
        )

    def _check_wc_api(self) -> bool:
        """Test if WooCommerce REST API is accessible."""
        if self._wc_api_available is not None:
            return self._wc_api_available
        try:
            resp = self.client.head(f"{self.WC_API_BASE}/products?per_page=1")
            self._wc_api_available = resp.status_code < 400
        except Exception:
            self._wc_api_available = False
        return self._wc_api_available

    def discover_category_urls(self) -> list[str]:
        return [
            f"{self.BASE_URL}/shop/",
            f"{self.BASE_URL}/product-category/women/",
            f"{self.BASE_URL}/product-category/sale/",
        ]

    def discover_product_urls(self, limit: int = 200) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()

        # 1. WooCommerce API
        if self._check_wc_api():
            try:
                page = 1
                per_page = 100
                while len(urls) < limit:
                    api_url = f"{self.WC_API_BASE}/products"
                    data = self.client.get_json(api_url, params={"per_page": per_page, "page": page, "status": "publish"})
                    if not isinstance(data, list) or not data:
                        break
                    for item in data:
                        purl = item.get("permalink", "")
                        if purl and purl not in seen:
                            seen.add(purl)
                            urls.append(purl)
                    if len(data) < per_page:
                        break
                    page += 1
                log.info(f"[CStyle] WC API: {len(urls)} products")
                if urls:
                    return urls[:limit]
            except Exception as e:
                log.warning(f"[CStyle] WC API failed: {e}")

        # 2. Sitemap
        try:
            discovery = SitemapDiscovery(self.client)
            sitemap_urls = discovery.discover(
                self.BASE_URL,
                pattern=r"/product/",
                max_urls=limit,
            )
            for u in sitemap_urls:
                if u not in seen:
                    seen.add(u)
                    urls.append(u)
            if urls:
                return urls[:limit]
        except Exception as e:
            log.warning(f"[CStyle] Sitemap failed: {e}")

        # 3. HTML category pages
        try:
            page = 1
            while len(urls) < limit:
                shop_url = f"{self.BASE_URL}/shop/page/{page}/" if page > 1 else f"{self.BASE_URL}/shop/"
                html = self._fetch_html(shop_url)
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, "lxml")
                product_links = soup.select("a.woocommerce-loop-product__link, h2.woocommerce-loop-product__title a, a[href*='/product/']")
                found = 0
                for a in product_links:
                    href = a.get("href", "")
                    if href and href not in seen:
                        full = urljoin(self.BASE_URL, href)
                        seen.add(full)
                        urls.append(full)
                        found += 1
                if found == 0 or page > 20:
                    break
                page += 1
        except Exception as e:
            log.warning(f"[CStyle] HTML shop page failed: {e}")

        return urls[:limit]

    def fetch_product_page(self, url: str) -> RawProductPayload:
        # Try WC API by extracting product slug
        slug = self._extract_wc_slug(url)
        if slug and self._check_wc_api():
            try:
                data = self.client.get_json(f"{self.WC_API_BASE}/products", params={"slug": slug})
                if isinstance(data, list) and data:
                    return RawProductPayload(
                        source_site=self.SOURCE_KEY,
                        product_url=url,
                        script_payload={"wc_product": data[0]},
                        extraction_method="api",
                    )
            except Exception:
                pass

        html = self._fetch_html(url)
        return self._build_raw_payload(url, html, method="unknown")

    def extract_raw_payload(self, url: str, html: str) -> RawProductPayload:
        return self._build_raw_payload(url, html, method="dom")

    def parse_product(self, raw: RawProductPayload) -> ParseResult:
        # 1. WooCommerce API data
        if raw.script_payload and raw.script_payload.get("wc_product"):
            partial = self._parse_wc_api_product(raw.script_payload["wc_product"])
            if partial:
                raw.extraction_method = "api"
                product = self.normalize(raw, partial)
                return ParseResult(
                    success=True, product_url=raw.product_url,
                    source_site=raw.source_site, product=product,
                    extraction_method="api", confidence=0.95,
                )

        # 2. JSON-LD
        if raw.json_ld_data:
            ld = find_product_json_ld(raw.json_ld_data)
            if ld:
                partial = parse_product_from_json_ld(ld)
                partial["breadcrumbs"] = find_breadcrumbs_json_ld(raw.json_ld_data)
                # JSON-LD alone misses sizes, colors, category — enrich from HTML
                self._enrich_from_dom(raw, partial)
                partial.setdefault("brand", "CStyle")
                raw.extraction_method = "json_ld"
                product = self.normalize(raw, partial)
                return ParseResult(
                    success=True, product_url=raw.product_url,
                    source_site=raw.source_site, product=product,
                    extraction_method="json_ld", confidence=0.92,
                )

        # 3. DOM fallback (WooCommerce standard selectors)
        if raw.html_snapshot:
            return self._parse_wc_dom(raw)

        return ParseResult(
            success=False, product_url=raw.product_url,
            source_site=raw.source_site,
            errors=["CStyle: no parseable data"],
        )

    def _parse_wc_api_product(self, data: dict) -> Optional[dict]:
        """Parse WooCommerce REST API product."""
        name = data.get("name", "")
        if not name:
            return None

        from bs4 import BeautifulSoup
        desc_html = data.get("description", data.get("short_description", ""))
        description = normalize_text(BeautifulSoup(desc_html, "lxml").get_text()) if desc_html else ""

        current_price, original_price = normalize_price_pair(
            data.get("sale_price") or data.get("price"),
            data.get("regular_price"),
        )

        images = []
        for img in data.get("images", []):
            src = img.get("src", "")
            if src:
                images.append(src)
        images = normalize_image_urls(images, base_url=self.BASE_URL)

        # Attributes (WooCommerce stores sizes/colors in attributes)
        sizes: list[str] = []
        colors: list[str] = []
        for attr in data.get("attributes", []):
            attr_name = (attr.get("name") or "").lower()
            attr_vals = attr.get("options", [])
            if any(k in attr_name for k in ["size", "מידה"]):
                sizes.extend(attr_vals)
            elif any(k in attr_name for k in ["color", "צבע", "colour"]):
                colors.extend(attr_vals)

        categories = [c.get("name", "") for c in data.get("categories", [])]
        category = categories[0] if categories else ""

        return {
            "product_name": name,
            "short_description": description[:300],
            "original_description": description,
            "current_price": current_price,
            "original_price": original_price,
            "image_urls": images,
            "sizes_available": normalize_sizes(sizes),
            "colors_available": normalize_colors(colors),
            "category": category or None,
            "source_product_reference": str(data.get("id", "")),
            "in_stock": data.get("in_stock", True),
            "out_of_stock": not data.get("in_stock", True),
        }

    def _enrich_from_dom(self, raw: RawProductPayload, partial: dict) -> None:
        """Supplement JSON-LD (or WC API) data with DOM-extracted sizes, colors, category."""
        if not raw.html_snapshot:
            return
        from engine.extraction.dom_selector import DOMExtractor
        dom = DOMExtractor(raw.html_snapshot)

        # Sizes from WooCommerce size variation select
        if not partial.get("sizes_available"):
            sizes = dom.texts(
                "select[name*='pa_size'] option:not([value=''])",
                "select[name*='size'] option:not([value=''])",
                ".variations [data-attribute*='size'] li",
            )
            if sizes:
                partial["sizes_available"] = normalize_sizes(sizes)

        # Colors from WooCommerce colour variation select
        if not partial.get("colors_available"):
            colors = dom.texts(
                "select[name*='pa_colour'] option:not([value=''])",
                "select[name*='colour'] option:not([value=''])",
                "select[name*='color'] option:not([value=''])",
            )
            if colors:
                partial["colors_available"] = normalize_colors(colors)

        # Category from breadcrumbs (second-to-last crumb is category)
        if not partial.get("category"):
            crumbs = partial.get("breadcrumbs") or []
            if len(crumbs) >= 2:
                partial["category"] = crumbs[-2]
            elif not crumbs:
                # Try DOM breadcrumbs
                dom_crumbs = dom.extract_breadcrumbs()
                if len(dom_crumbs) >= 2:
                    partial["category"] = dom_crumbs[-2]
                    if not partial.get("breadcrumbs"):
                        partial["breadcrumbs"] = dom_crumbs

        # Original price from DOM if not in JSON-LD
        if not partial.get("original_price"):
            orig = dom.extract_price(".price del .woocommerce-Price-amount")
            if orig:
                partial["original_price"] = orig

        # Short description from WooCommerce dedicated field
        if not partial.get("short_description"):
            short = dom.text(".woocommerce-product-details__short-description")
            if short:
                partial["short_description"] = normalize_text(short)[:300]

    def _parse_wc_dom(self, raw: RawProductPayload) -> ParseResult:
        """WooCommerce standard DOM selectors."""
        from engine.extraction.dom_selector import DOMExtractor
        from engine.extraction.heuristic import HeuristicExtractor
        dom = DOMExtractor(raw.html_snapshot)
        heur = HeuristicExtractor(raw.html_snapshot)

        name = dom.text(
            "h1.product_title", "h1.product-title", "h1",
        ) or heur.extract_name()
        if not name:
            return ParseResult(
                success=False, product_url=raw.product_url,
                source_site=raw.source_site, errors=["CStyle DOM: no product name"],
            )

        # WooCommerce price selectors
        price = dom.extract_price(
            ".price ins .woocommerce-Price-amount",
            ".price .woocommerce-Price-amount",
            ".price",
        ) or heur.extract_price()
        orig_price = dom.extract_price(".price del .woocommerce-Price-amount")

        images = dom.extract_images(
            ".woocommerce-product-gallery__image img",
            ".product-gallery img",
            "img[data-src*='cstyle']",
        ) or heur.extract_images()
        images = normalize_image_urls(images, base_url=self.BASE_URL)

        # WooCommerce variation selectors
        sizes = dom.texts(
            "select[name*='size'] option:not([value=''])",
            ".variations [data-attribute*='size'] span",
        )
        colors = dom.texts(
            "select[name*='color'] option:not([value=''])",
            ".variations [data-attribute*='colour'] span",
        )
        breadcrumbs = dom.extract_breadcrumbs()

        _, original_price = normalize_price_pair(price, orig_price)
        raw.extraction_method = "dom"
        product = self.normalize(raw, {
            "product_name": name,
            "current_price": price,
            "original_price": original_price,
            "image_urls": images,
            "sizes_available": normalize_sizes(sizes),
            "colors_available": normalize_colors(colors),
            "breadcrumbs": breadcrumbs,
        })
        return ParseResult(
            success=True, product_url=raw.product_url,
            source_site=raw.source_site, product=product,
            extraction_method="dom", confidence=0.7,
        )

    @staticmethod
    def _extract_wc_slug(url: str) -> Optional[str]:
        """Extract WooCommerce product slug from URL."""
        match = re.search(r"/product/([^/?#]+)/?", url)
        return match.group(1) if match else None
