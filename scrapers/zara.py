"""
Zara Israel scraper.
Uses Zara's internal REST API (reverse engineered from browser network tab).
No headless browser needed!
"""
from __future__ import annotations
from typing import Optional
import json
import re

import config
from scrapers.base import BaseScraper, RawProduct
from utils.logger import get_logger

log = get_logger("zara")

# Zara IL category IDs (from their API)
ZARA_CATEGORIES = {
    "woman": {"id": "2524048", "name": "אישה"},
    "man":   {"id": "2524049", "name": "גבר"},
    "girl":  {"id": "2524050", "name": "ילדה"},
    "boy":   {"id": "2524051", "name": "ילד"},
    "kids":  {"id": "2524052", "name": "ילדים"},
}


class ZaraScraper(BaseScraper):
    SITE_KEY = "zara"
    SITE_NAME = "Zara Israel"
    BASE_URL = "https://www.zara.com"
    API_BASE = "https://www.zara.com/itxrest/3/catalog/store"
    STORE_ID = "11719"   # Zara IL store ID
    LANG_ID = "2"        # Hebrew

    def __init__(self):
        super().__init__()
        # Zara needs specific headers
        self._session.headers.update({
            "x-requested-with": "XMLHttpRequest",
            "Referer": "https://www.zara.com/il/",
            "Origin": "https://www.zara.com",
        })

    def _api_get(self, endpoint: str, params: dict = None) -> dict:
        url = f"{self.API_BASE}/{self.STORE_ID}/{endpoint}"
        resp = self._get(url, params=params or {})
        return resp.json()

    def get_product_urls(self, limit: int = config.MAX_PRODUCTS_PER_SITE) -> list[str]:
        """Collect product URLs from Zara's API categories."""
        urls: list[str] = []
        per_category = max(1, limit // len(ZARA_CATEGORIES))

        for cat_key, cat_info in ZARA_CATEGORIES.items():
            if len(urls) >= limit:
                break
            try:
                products = self._fetch_category_products(cat_info["id"], per_category)
                for p in products:
                    seo_url = p.get("seo", {}).get("keyword", "")
                    pid = p.get("id", "")
                    if seo_url:
                        urls.append(f"https://www.zara.com/il/he/{seo_url}-p{pid}.html")
                    if len(urls) >= limit:
                        break
            except Exception as e:
                log.warning(f"Zara category {cat_key} error: {e}")

        return urls[:limit]

    def _fetch_category_products(self, category_id: str, limit: int) -> list[dict]:
        """Fetch products from a Zara category via API."""
        params = {
            "languageId": self.LANG_ID,
            "ids": category_id,
            "transportQuery": "true",
            "ajax": "true",
        }
        data = self._api_get(
            f"category/{category_id}/product",
            params={"languageId": self.LANG_ID, "ajax": "true"}
        )
        products = []
        for group in data.get("productGroups", []):
            for elem in group.get("elements", []):
                p = elem.get("commercialComponents", [{}])[0] if elem.get("commercialComponents") else {}
                if p.get("id"):
                    products.append(p)
                if len(products) >= limit:
                    break
        return products

    def scrape_product(self, url: str) -> Optional[RawProduct]:
        """
        Scrape Zara product via API — extract product ID from URL,
        then fetch full product data from Zara's product API.
        """
        # Extract product ID from URL pattern: ...-p{ID}.html
        match = re.search(r"-p(\d+)\.html", url)
        if not match:
            return None
        product_id = match.group(1)

        try:
            data = self._fetch_product_detail(product_id)
            return self._parse_product(data, url)
        except Exception as e:
            log.error(f"Zara product error ({url}): {e}")
            return None

    def _fetch_product_detail(self, product_id: str) -> dict:
        params = {
            "languageId": self.LANG_ID,
            "productId": product_id,
        }
        return self._api_get(f"product/detail", params=params)

    def _parse_product(self, data: dict, url: str) -> Optional[RawProduct]:
        name = data.get("name", "")
        if not name:
            return None

        # Price (in cents)
        price_data = data.get("price", 0)
        price = price_data / 100 if isinstance(price_data, int) else self._clean_price(str(price_data))

        orig_price_data = data.get("originalPrice", 0)
        original_price = orig_price_data / 100 if orig_price_data else None

        # Description
        desc = self._clean_text(data.get("description", ""))

        # Images
        images = []
        for media in data.get("detail", {}).get("colors", [{}])[0].get("xmedia", []):
            path = media.get("path", "")
            name_img = media.get("name", "")
            if path and name_img:
                img_url = f"https://static.zara.net/photos/{path}/{name_img}/w/750/{name_img}.jpg"
                images.append(img_url)
            if len(images) >= 6:
                break

        # Colors
        colors = [
            c.get("name", "") for c in data.get("detail", {}).get("colors", [])
            if c.get("name")
        ]

        # Sizes
        sizes = []
        for color_data in data.get("detail", {}).get("colors", []):
            for size in color_data.get("sizes", []):
                label = size.get("name", "")
                if label and label not in sizes:
                    sizes.append(label)

        # Category
        section = data.get("sectionName", "אופנה")
        family = data.get("familyName", "")

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price,
            original_price=original_price if original_price and original_price > (price or 0) else None,
            description_short=desc[:500],
            images=images,
            colors_available=colors,
            sizes_available=sizes,
            category=section,
            sub_category=family,
        )
