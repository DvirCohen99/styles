"""
Renoir (renoir.co.il) scraper.
Static HTML — straightforward BS4 scraping.
"""
from __future__ import annotations
from typing import Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import re

import config
from scrapers.base import BaseScraper, RawProduct
from utils.logger import get_logger

log = get_logger("renoir")


class RenoirScraper(BaseScraper):
    SITE_KEY = "renoir"
    SITE_NAME = "Renoir"
    BASE_URL = "https://www.renoir.co.il"

    CATEGORY_URLS = [
        "https://www.renoir.co.il/collections/women",
        "https://www.renoir.co.il/collections/men",
        "https://www.renoir.co.il/collections/all",
    ]

    def get_product_urls(self, limit: int = config.MAX_PRODUCTS_PER_SITE) -> list[str]:
        urls: set[str] = set()
        for cat_url in self.CATEGORY_URLS:
            if len(urls) >= limit:
                break
            try:
                page = 1
                while len(urls) < limit:
                    paged = f"{cat_url}?page={page}"
                    soup = self._soup(paged)
                    links = soup.select("a[href*='/products/']")
                    if not links:
                        break
                    for a in links:
                        href = a.get("href", "")
                        if "/products/" in href:
                            full = urljoin(self.BASE_URL, href.split("?")[0])
                            urls.add(full)
                    page += 1
                    if page > 10:
                        break
            except Exception as e:
                log.warning(f"Renoir category error ({cat_url}): {e}")
        return list(urls)[:limit]

    def scrape_product(self, url: str) -> Optional[RawProduct]:
        soup = self._soup(url)

        # Product name
        name_el = soup.select_one("h1.product-title, h1.product__title, h1")
        if not name_el:
            return None
        name = self._clean_text(name_el.get_text())

        # Prices
        price_el = soup.select_one(".price .money, [class*='price'] .money, .product-price")
        orig_el = soup.select_one(".compare-at-price .money, s .money, .was-price")
        price = self._clean_price(price_el.get_text() if price_el else "")
        original_price = self._clean_price(orig_el.get_text() if orig_el else "")

        # Description
        desc_el = soup.select_one(".product-description, .product__description, [class*='description']")
        description = self._clean_text(desc_el.get_text() if desc_el else "")

        # Images
        images = []
        for img in soup.select("img[src*='cdn.shopify'], img[data-src*='cdn.shopify']"):
            src = img.get("data-src") or img.get("src", "")
            if src and not src.endswith(".svg"):
                if src.startswith("//"):
                    src = "https:" + src
                clean = re.sub(r"_\d+x\d+", "", src).split("?")[0]
                if clean not in images:
                    images.append(clean)
            if len(images) >= 6:
                break

        # Sizes
        sizes = []
        for opt in soup.select("[data-option='size'] option, [name='Size'] option, .size-option"):
            s = self._clean_text(opt.get_text())
            if s and s.lower() not in ("בחר מידה", "select size", ""):
                sizes.append(s)

        # Colors
        colors = []
        for opt in soup.select("[data-option='color'] option, [name='Color'] option, .color-option"):
            c = self._clean_text(opt.get_text())
            if c and c.lower() not in ("בחר צבע", "select color", ""):
                colors.append(c)

        # Category from URL/breadcrumb
        category = "אופנה"
        breadcrumb = soup.select_one("nav.breadcrumb, .breadcrumb")
        if breadcrumb:
            crumbs = [b.get_text(strip=True) for b in breadcrumb.select("a")]
            if len(crumbs) >= 2:
                category = crumbs[1]

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price,
            original_price=original_price if original_price and original_price > (price or 0) else None,
            description_short=description[:500],
            images=images[:6],
            colors_available=colors,
            sizes_available=sizes,
            category=category,
        )
