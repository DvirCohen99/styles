"""
DOM selector-based extraction as a fallback layer.

Uses CSS selectors against BeautifulSoup parsed HTML.
Supports multiple selector candidates per field (first match wins).
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from bs4 import BeautifulSoup, Tag

log = logging.getLogger("engine.extraction.dom")


class DOMExtractor:
    """
    Generic DOM extractor.
    Accepts a selector map: field_name -> list_of_css_selectors
    """

    def __init__(self, html: str):
        self.soup = BeautifulSoup(html, "lxml")
        self.html = html

    def text(self, *selectors: str, default: str = "") -> str:
        """Return clean text from first matching selector."""
        for sel in selectors:
            try:
                el = self.soup.select_one(sel)
                if el:
                    return self._clean(el.get_text())
            except Exception:
                continue
        return default

    def attr(self, selector: str, attribute: str, default: str = "") -> str:
        """Return attribute value from first matching element."""
        try:
            el = self.soup.select_one(selector)
            if el:
                return el.get(attribute, default) or default
        except Exception:
            pass
        return default

    def texts(self, *selectors: str) -> list[str]:
        """Return list of text from all matching elements."""
        for sel in selectors:
            try:
                elements = self.soup.select(sel)
                if elements:
                    return [self._clean(el.get_text()) for el in elements if self._clean(el.get_text())]
            except Exception:
                continue
        return []

    def attrs(self, selector: str, attribute: str) -> list[str]:
        """Return list of attribute values from all matching elements."""
        try:
            elements = self.soup.select(selector)
            return [el.get(attribute, "") for el in elements if el.get(attribute)]
        except Exception:
            return []

    def exists(self, selector: str) -> bool:
        try:
            return self.soup.select_one(selector) is not None
        except Exception:
            return False

    def extract_price(self, *selectors: str) -> Optional[float]:
        """Extract price value from first matching selector."""
        for sel in selectors:
            try:
                el = self.soup.select_one(sel)
                if el:
                    raw = el.get_text()
                    price = self._parse_price(raw)
                    if price is not None and price > 0:
                        return price
            except Exception:
                continue
        return None

    def extract_images(self, *selectors: str, attr: str = "src") -> list[str]:
        """Extract image URLs, trying src, data-src, data-lazy-src."""
        images: list[str] = []
        seen: set[str] = set()

        for sel in selectors:
            try:
                for img in self.soup.select(sel):
                    url = (
                        img.get("data-src")
                        or img.get("data-lazy-src")
                        or img.get("data-original")
                        or img.get(attr, "")
                        or ""
                    )
                    url = url.split("?")[0].strip()
                    if url and url not in seen and self._is_product_image(url):
                        seen.add(url)
                        images.append(url)
            except Exception:
                continue

        return images

    def extract_meta(self, property_name: str = "", name: str = "") -> str:
        """Extract content from a <meta> tag."""
        if property_name:
            el = self.soup.find("meta", property=property_name)
        elif name:
            el = self.soup.find("meta", attrs={"name": name})
        else:
            return ""
        return (el.get("content", "") or "") if el else ""

    def extract_canonical(self) -> str:
        """Extract canonical URL."""
        el = self.soup.find("link", rel="canonical")
        return (el.get("href", "") or "") if el else ""

    def extract_breadcrumbs(self) -> list[str]:
        """Generic breadcrumb extraction."""
        # Try schema-org microdata
        crumbs = []
        for el in self.soup.select("[itemtype*='BreadcrumbList'] [itemprop='name']"):
            text = self._clean(el.get_text())
            if text:
                crumbs.append(text)
        if crumbs:
            return crumbs

        # Aria label
        nav = self.soup.find("nav", attrs={"aria-label": re.compile("breadcrumb", re.I)})
        if nav:
            for a in nav.select("a, [class*='crumb'] span"):
                text = self._clean(a.get_text())
                if text:
                    crumbs.append(text)
            return crumbs

        # Class-based
        for sel in [
            "[class*='breadcrumb'] a",
            "[class*='breadcrumb'] li",
            "[class*='Breadcrumb'] a",
            ".breadcrumbs a",
            ".breadcrumb-item",
        ]:
            elements = self.soup.select(sel)
            if elements:
                return [self._clean(el.get_text()) for el in elements if self._clean(el.get_text())]

        return []

    @staticmethod
    def _clean(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "")).strip()

    @staticmethod
    def _parse_price(text: str) -> Optional[float]:
        if not text:
            return None
        # Extract digits and decimal point
        cleaned = re.sub(r"[^\d.,]", "", text.replace("\xa0", ""))
        # Handle comma as thousands separator vs decimal separator
        if "," in cleaned and "." in cleaned:
            # e.g. "1,234.56"
            cleaned = cleaned.replace(",", "")
        elif "," in cleaned:
            # Could be thousands or decimal — use heuristic
            parts = cleaned.split(",")
            if len(parts) == 2 and len(parts[1]) <= 2:
                # Treat as decimal: "12,90" -> 12.90
                cleaned = cleaned.replace(",", ".")
            else:
                cleaned = cleaned.replace(",", "")
        try:
            return float(cleaned)
        except ValueError:
            return None

    @staticmethod
    def _is_product_image(url: str) -> bool:
        url_lower = url.lower()
        # Skip icons, logos, tracking pixels
        skip = ["icon", "logo", "pixel", "spinner", "placeholder", "blank", "1x1", "spacer"]
        if any(s in url_lower for s in skip):
            return False
        # Must look like an image
        return bool(re.search(r"\.(jpg|jpeg|png|webp|avif|gif)($|\?|/)", url_lower, re.I))
