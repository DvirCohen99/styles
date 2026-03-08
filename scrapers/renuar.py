"""
Renuar (renuar.co.il) scraper.
Shopify-based — similar pattern to Renoir but with different selectors.
"""
from __future__ import annotations
from typing import Optional
from urllib.parse import urljoin
import json
import re

import config
from scrapers.base import BaseScraper, RawProduct
from utils.logger import get_logger

log = get_logger("renuar")


class RenuarScraper(BaseScraper):
    SITE_KEY = "renuar"
    SITE_NAME = "Renuar"
    BASE_URL = "https://www.renuar.co.il"

    CATEGORY_URLS = [
        "https://www.renuar.co.il/he/women",
        "https://www.renuar.co.il/he/men",
        "https://www.renuar.co.il/he/sale",
    ]

    def get_product_urls(self, limit: int = config.MAX_PRODUCTS_PER_SITE) -> list[str]:
        urls: set[str] = set()
        for cat_url in self.CATEGORY_URLS:
            if len(urls) >= limit:
                break
            try:
                # Try JSON catalog endpoint first (faster)
                json_url = cat_url.rstrip("/") + "/index.json"
                resp = self._get(json_url)
                data = resp.json()
                for p in data.get("products", []):
                    handle = p.get("handle", "")
                    if handle:
                        urls.add(f"{self.BASE_URL}/he/products/{handle}")
                    if len(urls) >= limit:
                        break
            except Exception:
                # Fallback to HTML parsing
                try:
                    page = 1
                    while len(urls) < limit:
                        soup = self._soup(f"{cat_url}?page={page}")
                        links = soup.select("a[href*='/products/'], a[href*='/he/products/']")
                        if not links:
                            break
                        for a in links:
                            href = a.get("href", "")
                            if "/products/" in href:
                                full = urljoin(self.BASE_URL, href.split("?")[0])
                                urls.add(full)
                        page += 1
                        if page > 8:
                            break
                except Exception as e2:
                    log.warning(f"Renuar category fallback error: {e2}")
        return list(urls)[:limit]

    def scrape_product(self, url: str) -> Optional[RawProduct]:
        # Try JSON endpoint (much faster + more reliable)
        try:
            json_url = url.rstrip("/") + ".json"
            resp = self._get(json_url)
            return self._parse_shopify_json(resp.json().get("product", {}), url)
        except Exception:
            pass

        # HTML fallback
        try:
            soup = self._soup(url)
            return self._parse_html(soup, url)
        except Exception as e:
            log.error(f"Renuar scrape failed ({url}): {e}")
            return None

    def _parse_shopify_json(self, data: dict, url: str) -> Optional[RawProduct]:
        if not data:
            return None
        name = data.get("title", "")
        if not name:
            return None

        variants = data.get("variants", [{}])
        first_var = variants[0] if variants else {}
        price = self._clean_price(str(first_var.get("price", ""))) or 0
        compare = self._clean_price(str(first_var.get("compare_at_price", "")))
        # Shopify prices are in cents for some locales
        if price and price > 1000:
            price /= 100
            if compare:
                compare /= 100

        images = [
            img["src"].split("?")[0]
            for img in data.get("images", [])[:6]
            if img.get("src")
        ]

        sizes = []
        colors = []
        for opt in data.get("options", []):
            opt_name = opt.get("name", "").lower()
            if "size" in opt_name or "מידה" in opt_name:
                sizes = opt.get("values", [])
            elif "color" in opt_name or "צבע" in opt_name:
                colors = opt.get("values", [])

        desc_html = data.get("body_html", "")
        from bs4 import BeautifulSoup
        desc = self._clean_text(BeautifulSoup(desc_html, "lxml").get_text()) if desc_html else ""

        category = data.get("product_type", "אופנה") or "אופנה"
        tags = data.get("tags", [])

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price if price else None,
            original_price=compare if compare and compare > (price or 0) else None,
            description_short=desc[:500],
            images=images,
            colors_available=colors,
            sizes_available=sizes,
            category=category,
            extra={"shopify_tags": tags},
        )

    def _parse_html(self, soup, url: str) -> Optional[RawProduct]:
        name_el = soup.select_one("h1")
        if not name_el:
            return None
        name = self._clean_text(name_el.get_text())

        price_el = soup.select_one("[class*='price']")
        price = self._clean_price(price_el.get_text() if price_el else "")

        desc_el = soup.select_one("[class*='description']")
        description = self._clean_text(desc_el.get_text() if desc_el else "")

        images = [
            img.get("src", "").split("?")[0]
            for img in soup.select("img[src*='renuar']")
            if img.get("src")
        ][:6]

        return RawProduct(
            site=self.SITE_KEY,
            name=name,
            original_url=url,
            price=price,
            description_short=description[:500],
            images=images,
            category="אופנה",
        )
