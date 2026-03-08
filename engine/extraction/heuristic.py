"""
Heuristic / last-resort extraction layer.

When structured extraction fails, these heuristics try to recover
partial data from any HTML page.
"""
from __future__ import annotations

import logging
import re
from typing import Optional

from bs4 import BeautifulSoup

log = logging.getLogger("engine.extraction.heuristic")

# Heuristic patterns for product name
NAME_PATTERNS = [
    r'<h1[^>]*>\s*(.+?)\s*</h1>',
    r'"name"\s*:\s*"([^"]{3,100})"',
    r'<title>\s*([^|–-]+)',
    r'og:title.*?content="([^"]+)"',
]

# Price patterns (ILS)
PRICE_PATTERNS = [
    r'₪\s*([\d,]+(?:\.\d{1,2})?)',
    r'([\d,]+(?:\.\d{1,2})?)\s*₪',
    r'"price"\s*:\s*"?([\d.]+)"?',
    r'data-price="([\d.]+)"',
    r'class="[^"]*price[^"]*"[^>]*>\s*(?:₪\s*)?([\d,]+)',
]

# Image patterns
IMAGE_PATTERNS = [
    r'og:image.*?content="([^"]+)"',
    r'"image"\s*:\s*"(https?://[^"]+\.(?:jpg|jpeg|png|webp))',
    r'data-src="(https?://[^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"',
]


class HeuristicExtractor:
    """
    Last-resort field recovery via regex patterns.
    Confidence will be low — caller should mark accordingly.
    """

    def __init__(self, html: str):
        self.html = html
        self.soup = BeautifulSoup(html, "lxml")

    def extract_name(self) -> Optional[str]:
        """Try to find product name from various heuristic sources."""
        # Prefer title tag content (strip site suffix)
        title_el = self.soup.find("title")
        if title_el:
            title = title_el.get_text()
            # Strip " | SiteName" suffix
            title = re.split(r"\s*[\|–|-]\s*", title)[0].strip()
            if 3 < len(title) < 120:
                return title

        # h1
        h1 = self.soup.find("h1")
        if h1:
            text = re.sub(r"\s+", " ", h1.get_text()).strip()
            if 3 < len(text) < 120:
                return text

        # og:title
        og = self.soup.find("meta", property="og:title")
        if og and og.get("content"):
            content = og["content"].split("|")[0].strip()
            if content:
                return content

        return None

    def extract_price(self) -> Optional[float]:
        """Try to extract a price using regex patterns."""
        for pattern in PRICE_PATTERNS:
            matches = re.findall(pattern, self.html, re.IGNORECASE)
            for m in matches:
                cleaned = m.replace(",", "").strip()
                try:
                    val = float(cleaned)
                    if 5 < val < 100000:  # Sanity range for ILS prices
                        return val
                except ValueError:
                    continue
        return None

    def extract_images(self) -> list[str]:
        """Extract image URLs from meta and data attributes."""
        images: list[str] = []
        seen: set[str] = set()

        # og:image
        for meta in self.soup.find_all("meta", property="og:image"):
            url = meta.get("content", "")
            if url and url not in seen:
                seen.add(url)
                images.append(url)

        # product images by data attributes
        for img in self.soup.find_all("img"):
            url = (
                img.get("data-src")
                or img.get("data-lazy-src")
                or img.get("data-original")
                or img.get("src", "")
            )
            if not url:
                continue
            url = url.split("?")[0]
            if url in seen:
                continue
            if not re.search(r"\.(jpg|jpeg|png|webp|avif)", url, re.I):
                continue
            # Skip tiny images
            width = img.get("width", "")
            if width and str(width).isdigit() and int(width) < 100:
                continue
            seen.add(url)
            images.append(url)

        return images[:10]

    def extract_description(self) -> Optional[str]:
        """Extract description from meta or first meaningful paragraph."""
        # og:description / meta description
        for sel in [
            ("meta", {"property": "og:description"}),
            ("meta", {"name": "description"}),
        ]:
            el = self.soup.find(*sel)
            if el and el.get("content"):
                return el["content"][:1000]

        # First substantial paragraph
        for p in self.soup.find_all("p"):
            text = re.sub(r"\s+", " ", p.get_text()).strip()
            if len(text) > 40:
                return text[:500]

        return None

    def extract_currency(self) -> str:
        """Detect currency from page."""
        if "₪" in self.html or "ILS" in self.html:
            return "ILS"
        if "$" in self.html:
            return "USD"
        if "€" in self.html:
            return "EUR"
        return "ILS"  # Default for Israeli sites
