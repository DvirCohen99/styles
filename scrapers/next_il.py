"""
Next IL (next.co.il) scraper.
Static + light JS — BeautifulSoup with JSON-LD parsing.
"""
from __future__ import annotations
from typing import Optional
from urllib.parse import urljoin
import json
import re

import config
from scrapers.base import BaseScraper, RawProduct
from utils.logger import get_logger

log = get_logger("next_il")


class NextILScraper(BaseScraper):
    SITE_KEY = "next"
    SITE_NAME = "Next IL"
    BASE_URL = "https://www.next.co.il"

    CATEGORY_URLS = [
        "https://www.next.co.il/he/shop/womens",
        "https://www.next.co.il/he/shop/mens",
        "https://www.next.co.il/he/shop/kids",
        "https://www.next.co.il/he/shop/sale",
    ]

    def get_product_urls(self, limit: int = config.MAX_PRODUCTS_PER_SITE) -> list[str]:
        urls: set[str] = set()
        per_cat = max(1, limit // len(self.CATEGORY_URLS))
        for cat_url in self.CATEGORY_URLS:
            if len(urls) >= limit:
                break
            try:
                page = 1
                while len(urls) < limit:
                    soup = self._soup(f"{cat_url}?page={page}")
                    links = soup.select("a[href*='/he/style/']")
                    if not links:
                        # Also try generic product links
                        links = soup.select("a.product-thumb, a[href*='/product/']")
                    if not links:
                        break
                    for a in links:
                        href = a.get("href", "")
                        if href:
                            urls.add(urljoin(self.BASE_URL, href.split("?")[0]))
                    page += 1
                    if page > 6:
                        break
            except Exception as e:
                log.warning(f"Next IL {cat_url}: {e}")
        return list(urls)[:limit]

    def scrape_product(self, url: str) -> Optional[RawProduct]:
        soup = self._soup(url)

        # Try JSON-LD structured data (most reliable)
        for script in soup.select("script[type='application/ld+json']"):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    data = next((d for d in data if d.get("@type") == "Product"), {})
                if data.get("@type") == "Product":
                    return self._parse_json_ld(data, url, soup)
            except Exception:
                continue

        # HTML fallback
        return self._parse_html(soup, url)

    def _parse_json_ld(self, data: dict, url: str, soup) -> Optional[RawProduct]:
        name = data.get("name", "")
        if not name:
            return None

        offers = data.get("offers", {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        price = self._clean_price(str(offers.get("price", "")))
        original_price = self._clean_price(str(offers.get("highPrice", "")))

        images = data.get("image", [])
        if isinstance(images, str):
            images = [images]
        images = images[:6]

        description = self._clean_text(data.get("description", ""))

        # Variants for sizes/colors
        sizes, colors = [], []
        for script in soup.select("script"):
            if script.string and "variants" in (script.string or ""):
                match = re.search(r'"variants"\s*:\s*(\[.*?\])', script.string, re.DOTALL)
                if match:
                    try:
                        variants = json.loads(match.group(1))
                        for v in variants:
                            for opt in v.get("options", []):
                                if re.match(r'\d+|XS|S|M|L|XL', opt):
                                    if opt not in sizes:
                                        sizes.append(opt)
                    except Exception:
                        pass
                break

        category = data.get("category", "אופנה")

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price,
            original_price=original_price if original_price and original_price > (price or 0) else None,
            description_short=description[:500],
            images=images,
            sizes_available=sizes,
            colors_available=colors,
            category=str(category),
        )

    def _parse_html(self, soup, url: str) -> Optional[RawProduct]:
        name_el = soup.select_one("h1")
        if not name_el:
            return None
        name = self._clean_text(name_el.get_text())

        price_els = soup.select("[class*='price']")
        price = None
        original_price = None
        for el in price_els:
            p = self._clean_price(el.get_text())
            if p:
                if price is None:
                    price = p
                elif p > price:
                    original_price = p
                    break

        images = [
            img.get("src", "").split("?")[0]
            for img in soup.select("img[src*='next']")
            if img.get("src")
        ][:6]

        desc_el = soup.select_one("[class*='description'], [class*='detail']")
        desc = self._clean_text(desc_el.get_text() if desc_el else "")

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price,
            original_price=original_price,
            description_short=desc[:500],
            images=images,
            category="אופנה",
        )
