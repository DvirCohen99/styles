"""
Shein IL (il.shein.com) scraper.
Uses Shein's category JSON endpoints + product detail API.
Note: Shein is JS-heavy but has accessible API endpoints.
"""
from __future__ import annotations
from typing import Optional
import json
import re
import hashlib
import time

import config
from scrapers.base import BaseScraper, RawProduct
from utils.logger import get_logger

log = get_logger("shein")


class SheinScraper(BaseScraper):
    SITE_KEY = "shein"
    SITE_NAME = "Shein IL"
    BASE_URL = "https://il.shein.com"
    # Shein uses a different API domain
    API_BASE = "https://il.shein.com/api"

    # Category IDs for Shein IL
    CATEGORIES = [
        {"key": "Women's Clothing", "id": "1727", "name": "נשים"},
        {"key": "Men's Clothing",   "id": "2030", "name": "גברים"},
        {"key": "Kids",             "id": "2064", "name": "ילדים"},
    ]

    def __init__(self):
        super().__init__()
        self._session.headers.update({
            "Referer": "https://il.shein.com/",
            "Accept": "application/json",
            "sec-ch-ua": '"Not A;Brand";v="99", "Chromium";v="120"',
            "sec-ch-ua-mobile": "?1",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
        })

    def get_product_urls(self, limit: int = config.MAX_PRODUCTS_PER_SITE) -> list[str]:
        urls: list[str] = []
        per_cat = max(1, limit // len(self.CATEGORIES))

        for cat in self.CATEGORIES:
            if len(urls) >= limit:
                break
            try:
                products = self._fetch_category(cat["id"], per_cat)
                for p in products:
                    goods_id = p.get("goods_id", p.get("goodsId", ""))
                    goods_sn = p.get("goods_sn", p.get("goodsSn", ""))
                    url_name = p.get("goods_url_name", "product")
                    if goods_id:
                        url = f"{self.BASE_URL}/{url_name}-p-{goods_id}-cat-{cat['id']}.html"
                        urls.append(url)
                    if len(urls) >= limit:
                        break
            except Exception as e:
                log.warning(f"Shein category {cat['key']} error: {e}")

        return urls[:limit]

    def _fetch_category(self, cat_id: str, limit: int) -> list[dict]:
        params = {
            "cat_id": cat_id,
            "limit": min(limit, 100),
            "page": 1,
            "sort": "10",  # trending
            "currency": "ILS",
            "country": "IL",
            "lang": "he",
        }
        resp = self._get(f"{self.API_BASE}/productList/v2", params=params)
        data = resp.json()
        return data.get("info", {}).get("products", data.get("products", []))

    def scrape_product(self, url: str) -> Optional[RawProduct]:
        # Extract goods_id from URL
        match = re.search(r"-p-(\d+)", url)
        if not match:
            return None
        goods_id = match.group(1)

        try:
            data = self._fetch_product_detail(goods_id)
            return self._parse_product(data, url)
        except Exception as e:
            log.error(f"Shein product error ({url}): {e}")
            return None

    def _fetch_product_detail(self, goods_id: str) -> dict:
        params = {
            "goods_id": goods_id,
            "currency": "ILS",
            "country": "IL",
            "lang": "he",
        }
        resp = self._get(f"{self.API_BASE}/productInfo/v3", params=params)
        return resp.json().get("info", resp.json())

    def _parse_product(self, data: dict, url: str) -> Optional[RawProduct]:
        name = data.get("goods_name", data.get("goodsName", ""))
        if not name:
            return None

        # Price
        price_info = data.get("salePrice", data.get("retailPrice", {}))
        if isinstance(price_info, dict):
            price = self._clean_price(str(price_info.get("amount", "")))
        else:
            price = self._clean_price(str(price_info))

        orig_info = data.get("retailPrice", data.get("originalPrice", {}))
        if isinstance(orig_info, dict):
            orig = self._clean_price(str(orig_info.get("amount", "")))
        else:
            orig = None

        # Images
        images = []
        for img in data.get("detail_image", data.get("goods_imgs", {}).get("main_image", [{}]) if isinstance(data.get("goods_imgs"), dict) else [])[:6]:
            src = img.get("origin_image", img.get("medium_image", img.get("src", ""))) if isinstance(img, dict) else str(img)
            if src:
                images.append(src)

        # Fallback single image
        if not images and data.get("goods_img"):
            images = [data["goods_img"]]

        # Sizes
        sizes = []
        colors = []
        for attr in data.get("attrValueList", data.get("sku_list", [])):
            for prop in attr.get("attrList", []) if isinstance(attr, dict) else []:
                if "size" in prop.get("attr_name", "").lower():
                    val = prop.get("attr_value_name", "")
                    if val and val not in sizes:
                        sizes.append(val)
                elif "color" in prop.get("attr_name", "").lower():
                    val = prop.get("attr_value_name", "")
                    if val and val not in colors:
                        colors.append(val)

        desc = self._clean_text(data.get("goods_desc", ""))
        cat = data.get("cat_name", "אופנה")

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price,
            original_price=orig if orig and orig > (price or 0) else None,
            description_short=desc[:500],
            images=images[:6],
            sizes_available=sizes,
            colors_available=colors,
            category=str(cat),
        )
