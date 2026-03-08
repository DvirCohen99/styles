"""
Fox Fashion (fox.co.il) scraper.
Uses Fox's internal API endpoints.
"""
from __future__ import annotations
from typing import Optional
import json
import re
from urllib.parse import urljoin

import config
from scrapers.base import BaseScraper, RawProduct
from utils.logger import get_logger

log = get_logger("fox")


class FoxScraper(BaseScraper):
    SITE_KEY = "fox"
    SITE_NAME = "Fox Fashion"
    BASE_URL = "https://www.fox.co.il"

    CATEGORIES = {
        "women": "נשים",
        "men": "גברים",
        "kids": "ילדים",
        "sale": "מבצעים",
    }

    def __init__(self):
        super().__init__()
        self._session.headers.update({
            "Referer": "https://www.fox.co.il/",
            "Accept": "application/json, text/plain, */*",
        })

    def get_product_urls(self, limit: int = config.MAX_PRODUCTS_PER_SITE) -> list[str]:
        urls: set[str] = set()
        per_cat = max(1, limit // len(self.CATEGORIES))

        for cat_slug, cat_name in self.CATEGORIES.items():
            if len(urls) >= limit:
                break
            try:
                # Try Fox API
                api_url = f"{self.BASE_URL}/api/catalog/v1/categories/{cat_slug}/products"
                resp = self._get(api_url, params={"pageSize": per_cat, "pageNumber": 1})
                data = resp.json()

                for item in data.get("products", data.get("items", data.get("data", []))):
                    item_url = item.get("url", item.get("productUrl", ""))
                    if not item_url and item.get("slug"):
                        item_url = f"/product/{item['slug']}"
                    if item_url:
                        urls.add(urljoin(self.BASE_URL, item_url))
                    if len(urls) >= limit:
                        break

            except Exception:
                # HTML fallback
                try:
                    page = 1
                    while len(urls) < limit:
                        soup = self._soup(f"{self.BASE_URL}/{cat_slug}?page={page}")
                        links = soup.select("a[href*='/product/'], a[href*='fox.co.il/p']")
                        if not links:
                            links = soup.select("a.product-link, a[class*='product']")
                        if not links:
                            break
                        for a in links:
                            href = a.get("href", "")
                            if href:
                                urls.add(urljoin(self.BASE_URL, href.split("?")[0]))
                        page += 1
                        if page > 5:
                            break
                except Exception as e2:
                    log.warning(f"Fox {cat_slug} HTML fallback error: {e2}")

        return list(urls)[:limit]

    def scrape_product(self, url: str) -> Optional[RawProduct]:
        # Try API by extracting product slug from URL
        slug_match = re.search(r"/product/([^/?#]+)", url)
        if slug_match:
            slug = slug_match.group(1)
            try:
                resp = self._get(f"{self.BASE_URL}/api/catalog/v1/products/{slug}")
                return self._parse_api(resp.json(), url)
            except Exception:
                pass

        return self._parse_html(url)

    def _parse_api(self, data: dict, url: str) -> Optional[RawProduct]:
        name = data.get("name", data.get("title", ""))
        if not name:
            return None

        price = self._clean_price(str(data.get("price", data.get("currentPrice", ""))))
        orig = self._clean_price(str(data.get("originalPrice", data.get("regularPrice", ""))))

        images = []
        for img in data.get("images", data.get("media", []))[:6]:
            src = img.get("url", img.get("src", "")) if isinstance(img, dict) else str(img)
            if src:
                images.append(src)

        sizes = [
            s.get("label", s.get("value", ""))
            for s in data.get("sizes", data.get("sizeOptions", []))
            if isinstance(s, dict)
        ]
        colors = [
            c.get("name", c.get("label", ""))
            for c in data.get("colors", data.get("colorOptions", []))
            if isinstance(c, dict)
        ]

        desc = self._clean_text(data.get("description", ""))
        category = data.get("categoryName", data.get("department", "אופנה"))

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price,
            original_price=orig if orig and orig > (price or 0) else None,
            description_short=desc[:500],
            images=images,
            sizes_available=[s for s in sizes if s],
            colors_available=[c for c in colors if c],
            category=str(category),
        )

    def _parse_html(self, url: str) -> Optional[RawProduct]:
        soup = self._soup(url)

        name_el = soup.select_one("h1")
        if not name_el:
            return None
        name = self._clean_text(name_el.get_text())

        price_el = soup.select_one("[class*='current-price'], [class*='sale-price'], [class*='price']")
        orig_el = soup.select_one("[class*='old-price'], [class*='was-price'], s[class*='price']")
        price = self._clean_price(price_el.get_text() if price_el else "")
        orig = self._clean_price(orig_el.get_text() if orig_el else "")

        images = [
            img.get("src", "").split("?")[0]
            for img in soup.select("img[src*='fox']")
            if img.get("src")
        ][:6]

        desc_el = soup.select_one("[class*='description']")
        desc = self._clean_text(desc_el.get_text() if desc_el else "")

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price,
            original_price=orig if orig and orig > (price or 0) else None,
            description_short=desc[:500],
            images=images,
            category="אופנה",
        )
