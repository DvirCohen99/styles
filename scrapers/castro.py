"""
Castro (castro.com) scraper.
Uses Castro's internal GraphQL/REST API.
"""
from __future__ import annotations
from typing import Optional
import json
import re
from urllib.parse import urljoin

import config
from scrapers.base import BaseScraper, RawProduct
from utils.logger import get_logger

log = get_logger("castro")


class CastroScraper(BaseScraper):
    SITE_KEY = "castro"
    SITE_NAME = "Castro"
    BASE_URL = "https://www.castro.com"

    CATEGORY_SLUGS = ["women", "men", "kids", "sale"]

    def __init__(self):
        super().__init__()
        self._session.headers.update({
            "Referer": "https://www.castro.com/",
            "x-requested-with": "XMLHttpRequest",
        })

    def get_product_urls(self, limit: int = config.MAX_PRODUCTS_PER_SITE) -> list[str]:
        urls: set[str] = set()
        per_cat = max(1, limit // len(self.CATEGORY_SLUGS))

        for slug in self.CATEGORY_SLUGS:
            if len(urls) >= limit:
                break
            try:
                # Castro has a JSON endpoint for category products
                api_url = f"{self.BASE_URL}/api/v1/category/{slug}/products"
                resp = self._get(api_url, params={"limit": per_cat, "offset": 0})
                data = resp.json()
                for item in data.get("items", data.get("products", [])):
                    slug_product = item.get("slug", item.get("url_key", ""))
                    if slug_product:
                        urls.add(f"{self.BASE_URL}/he/product/{slug_product}")
                    if len(urls) >= limit:
                        break
            except Exception:
                # HTML fallback for category pages
                try:
                    self._scrape_category_html(
                        f"{self.BASE_URL}/he/{slug}", urls, per_cat
                    )
                except Exception as e2:
                    log.warning(f"Castro {slug} fallback error: {e2}")

        return list(urls)[:limit]

    def _scrape_category_html(self, cat_url: str, urls: set, limit: int) -> None:
        page = 1
        while len(urls) < limit:
            soup = self._soup(f"{cat_url}?page={page}")
            links = soup.select("a[href*='/product/'], a[href*='/he/product/']")
            if not links:
                break
            for a in links:
                href = a.get("href", "")
                if href:
                    urls.add(urljoin(self.BASE_URL, href.split("?")[0]))
            page += 1
            if page > 5:
                break

    def scrape_product(self, url: str) -> Optional[RawProduct]:
        # Extract slug from URL
        match = re.search(r"/product/([^/?#]+)", url)
        if not match:
            return self._scrape_html(url)

        slug = match.group(1)
        try:
            resp = self._get(f"{self.BASE_URL}/api/v1/products/{slug}")
            return self._parse_api_response(resp.json(), url)
        except Exception:
            return self._scrape_html(url)

    def _parse_api_response(self, data: dict, url: str) -> Optional[RawProduct]:
        name = data.get("name", data.get("title", ""))
        if not name:
            return None

        price = self._clean_price(str(data.get("price", data.get("special_price", ""))))
        orig = self._clean_price(str(data.get("original_price", data.get("price", ""))))

        images = []
        for img in data.get("images", data.get("media_gallery", []))[:6]:
            src = img.get("url", img.get("src", img if isinstance(img, str) else ""))
            if src:
                images.append(src)

        sizes = [
            opt.get("label", opt.get("value", ""))
            for opt in data.get("sizes", data.get("size_options", []))
        ]
        colors = [
            opt.get("label", opt.get("value", ""))
            for opt in data.get("colors", data.get("color_options", []))
        ]

        desc = self._clean_text(data.get("description", data.get("short_description", "")))
        category = data.get("category", data.get("department", "אופנה"))

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price,
            original_price=orig if orig and orig > (price or 0) else None,
            description_short=desc[:500],
            images=images,
            colors_available=[c for c in colors if c],
            sizes_available=[s for s in sizes if s],
            category=str(category),
        )

    def _scrape_html(self, url: str) -> Optional[RawProduct]:
        soup = self._soup(url)

        name_el = soup.select_one("h1[class*='product'], h1[class*='title'], h1")
        if not name_el:
            return None
        name = self._clean_text(name_el.get_text())

        price_el = soup.select_one("[class*='price']:not([class*='was']):not([class*='old'])")
        orig_el = soup.select_one("[class*='was-price'], [class*='old-price'], [class*='compare']")
        price = self._clean_price(price_el.get_text() if price_el else "")
        original_price = self._clean_price(orig_el.get_text() if orig_el else "")

        images = [
            img.get("src", "").split("?")[0]
            for img in soup.select("img[src*='castro']")
            if img.get("src") and not img.get("src", "").endswith(".svg")
        ][:6]

        desc_el = soup.select_one("[class*='description'], [class*='details']")
        desc = self._clean_text(desc_el.get_text() if desc_el else "")

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price,
            original_price=original_price if original_price and original_price > (price or 0) else None,
            description_short=desc[:500],
            images=images,
            category="אופנה",
        )
