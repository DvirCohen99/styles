"""
Lidor (lidor.co.il) scraper.
Standard Israeli e-commerce — BS4 scraping.
"""
from __future__ import annotations
from typing import Optional
from urllib.parse import urljoin
import re

import config
from scrapers.base import BaseScraper, RawProduct
from utils.logger import get_logger

log = get_logger("lidor")


class LidorScraper(BaseScraper):
    SITE_KEY = "lidor"
    SITE_NAME = "Lidor"
    BASE_URL = "https://www.lidor.co.il"

    CATEGORY_URLS = [
        "https://www.lidor.co.il/women",
        "https://www.lidor.co.il/men",
        "https://www.lidor.co.il/sale",
    ]

    def get_product_urls(self, limit: int = config.MAX_PRODUCTS_PER_SITE) -> list[str]:
        urls: set[str] = set()
        for cat_url in self.CATEGORY_URLS:
            if len(urls) >= limit:
                break
            try:
                page = 1
                while len(urls) < limit:
                    soup = self._soup(f"{cat_url}?page={page}")
                    links = soup.select("a[href*='/product/'], a[class*='product-link'], a[class*='product-thumb']")
                    if not links:
                        # Generic product links
                        links = [
                            a for a in soup.select("a[href]")
                            if re.search(r"/p-\d+|/product/", a.get("href", ""))
                        ]
                    if not links:
                        break
                    for a in links:
                        href = a.get("href", "")
                        if href:
                            urls.add(urljoin(self.BASE_URL, href.split("?")[0]))
                    page += 1
                    if page > 5:
                        break
            except Exception as e:
                log.warning(f"Lidor {cat_url}: {e}")
        return list(urls)[:limit]

    def scrape_product(self, url: str) -> Optional[RawProduct]:
        soup = self._soup(url)

        name_el = soup.select_one("h1.product-title, h1[class*='product'], h1")
        if not name_el:
            return None
        name = self._clean_text(name_el.get_text())

        # Prices
        price_el = soup.select_one("[class*='price-sale'], [class*='current-price'], [class*='price']:not([class*='compare'])")
        orig_el = soup.select_one("[class*='compare-price'], [class*='old-price'], [class*='was-price']")
        price = self._clean_price(price_el.get_text() if price_el else "")
        orig = self._clean_price(orig_el.get_text() if orig_el else "")

        # Images
        images = []
        for img in soup.select("img[src*='lidor'], img[data-src*='lidor'], .product-images img"):
            src = img.get("data-src") or img.get("src", "")
            if src and src not in images:
                images.append(src.split("?")[0])
            if len(images) >= 6:
                break

        # Sizes
        sizes = [
            self._clean_text(el.get_text())
            for el in soup.select("[class*='size-option'], [data-size], option[class*='size']")
            if self._clean_text(el.get_text()) not in ("", "בחר מידה")
        ]

        # Colors
        colors = [
            self._clean_text(el.get("title", el.get_text()))
            for el in soup.select("[class*='color-swatch'], [data-color]")
            if self._clean_text(el.get("title", el.get_text()))
        ]

        desc_el = soup.select_one("[class*='description'], [class*='product-detail']")
        desc = self._clean_text(desc_el.get_text() if desc_el else "")

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price,
            original_price=orig if orig and orig > (price or 0) else None,
            description_short=desc[:500],
            images=images,
            sizes_available=sizes,
            colors_available=colors,
            category="אופנה",
        )
