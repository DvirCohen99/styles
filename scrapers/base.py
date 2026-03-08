"""
Base scraper class.
All site-specific scrapers inherit from this.
"""
from __future__ import annotations

import hashlib
import re
import time
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import config
from utils.logger import get_logger
from utils.rate_limiter import RateLimiter

log = get_logger("base_scraper")
_ua = UserAgent()


@dataclass
class RawProduct:
    """Raw product data before AI enrichment."""
    site: str
    name: str
    original_url: str
    price: Optional[float] = None
    original_price: Optional[float] = None
    description_short: str = ""
    images: list[str] = field(default_factory=list)
    colors_available: list[str] = field(default_factory=list)
    sizes_available: list[str] = field(default_factory=list)
    category: str = ""
    sub_category: str = ""
    currency: str = "ILS"
    extra: dict = field(default_factory=dict)

    @property
    def product_id(self) -> str:
        key = f"{self.site}:{self.original_url}"
        return hashlib.md5(key.encode()).hexdigest()

    @property
    def discount_percentage(self) -> Optional[float]:
        if self.price and self.original_price and self.original_price > self.price:
            return round((1 - self.price / self.original_price) * 100, 1)
        return None

    @property
    def is_on_sale(self) -> bool:
        return bool(self.discount_percentage and self.discount_percentage > 0)


class BaseScraper(ABC):
    """Abstract base for all site scrapers."""

    SITE_KEY: str = ""          # e.g. "renoir"
    SITE_NAME: str = ""         # e.g. "Renoir"
    BASE_URL: str = ""

    def __init__(self):
        self.rate_limiter = RateLimiter(
            min_delay=config.REQUEST_DELAY_MIN,
            max_delay=config.REQUEST_DELAY_MAX,
        )
        self._session = self._build_session()
        self._domain = urlparse(self.BASE_URL).netloc

    def _build_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({
            "User-Agent": _ua.random,
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "DNT": "1",
        })
        s.max_redirects = 5
        return s

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    )
    def _get(self, url: str, **kwargs) -> requests.Response:
        """Rate-limited GET with retries."""
        self.rate_limiter.wait(self._domain)
        # Rotate user agent per request
        self._session.headers["User-Agent"] = _ua.random
        resp = self._session.get(url, timeout=20, **kwargs)
        resp.raise_for_status()
        return resp

    def _soup(self, url: str, **kwargs) -> BeautifulSoup:
        resp = self._get(url, **kwargs)
        return BeautifulSoup(resp.text, "lxml")

    @staticmethod
    def _clean_price(text: str) -> Optional[float]:
        if not text:
            return None
        digits = re.sub(r"[^\d.]", "", text.replace(",", "."))
        try:
            return float(digits)
        except ValueError:
            return None

    @staticmethod
    def _clean_text(text: str) -> str:
        return re.sub(r"\s+", " ", text or "").strip()

    @abstractmethod
    def get_product_urls(self, limit: int = config.MAX_PRODUCTS_PER_SITE) -> list[str]:
        """Return list of product page URLs to scrape."""
        ...

    @abstractmethod
    def scrape_product(self, url: str) -> Optional[RawProduct]:
        """Scrape one product page. Returns None on failure."""
        ...

    def scrape_all(self, limit: int = config.MAX_PRODUCTS_PER_SITE) -> list[RawProduct]:
        """Main entry: collect URLs then scrape each one."""
        log.info(f"[{self.SITE_NAME}] Collecting product URLs...")
        try:
            urls = self.get_product_urls(limit)
        except Exception as e:
            log.error(f"[{self.SITE_NAME}] Failed to collect URLs: {e}")
            return []

        log.info(f"[{self.SITE_NAME}] Found {len(urls)} URLs — scraping up to {limit}")
        products: list[RawProduct] = []
        for i, url in enumerate(urls[:limit], 1):
            try:
                product = self.scrape_product(url)
                if product:
                    products.append(product)
                    log.info(f"[{self.SITE_NAME}] {i}/{min(len(urls), limit)} ✓ {product.name[:50]}")
                else:
                    log.warning(f"[{self.SITE_NAME}] {i} skipped: {url}")
            except Exception as e:
                log.error(f"[{self.SITE_NAME}] {i} error ({url}): {e}")

        log.info(f"[{self.SITE_NAME}] Done — {len(products)} products scraped")
        return products
