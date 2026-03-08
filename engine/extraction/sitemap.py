"""
Sitemap and robots.txt discovery for product URL collection.

Supports:
- robots.txt sitemap directives
- XML sitemaps (flat and index)
- Sitemap URL filtering by pattern
- Gzipped sitemaps
"""
from __future__ import annotations

import gzip
import io
import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

log = logging.getLogger("engine.extraction.sitemap")

NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class SitemapDiscovery:
    """
    Discover product URLs from sitemaps.
    Usage:
        discovery = SitemapDiscovery(client)
        urls = discovery.discover("https://www.site.com", pattern=r"/products/")
    """

    def __init__(self, client):
        self.client = client

    def discover(
        self,
        base_url: str,
        pattern: Optional[str] = None,
        max_urls: int = 5000,
    ) -> list[str]:
        """
        Full discovery flow:
        1. Check robots.txt for Sitemap directives
        2. Try /sitemap.xml and /sitemap_index.xml
        3. Parse and filter URLs
        """
        sitemap_urls = self._find_sitemap_urls(base_url)

        all_product_urls: list[str] = []
        seen: set[str] = set()

        for sitemap_url in sitemap_urls:
            try:
                urls = self._parse_sitemap(sitemap_url, max_depth=3)
                for url in urls:
                    if url in seen:
                        continue
                    if pattern and not re.search(pattern, url):
                        continue
                    seen.add(url)
                    all_product_urls.append(url)
                    if len(all_product_urls) >= max_urls:
                        break
            except Exception as e:
                log.warning(f"Sitemap parse error ({sitemap_url}): {e}")

            if len(all_product_urls) >= max_urls:
                break

        log.info(f"Sitemap discovery: {len(all_product_urls)} URLs from {base_url}")
        return all_product_urls

    def _find_sitemap_urls(self, base_url: str) -> list[str]:
        """Find sitemap URLs from robots.txt and well-known locations."""
        found: list[str] = []

        # 1. robots.txt
        robots_url = urljoin(base_url, "/robots.txt")
        try:
            resp = self.client.get(robots_url)
            for line in resp.text.splitlines():
                line = line.strip()
                if line.lower().startswith("sitemap:"):
                    sm_url = line.split(":", 1)[1].strip()
                    if sm_url:
                        found.append(sm_url)
        except Exception as e:
            log.debug(f"robots.txt not accessible at {robots_url}: {e}")

        # 2. Well-known sitemap locations
        candidates = [
            urljoin(base_url, "/sitemap.xml"),
            urljoin(base_url, "/sitemap_index.xml"),
            urljoin(base_url, "/sitemaps/sitemap.xml"),
            urljoin(base_url, "/sitemap/sitemap.xml"),
        ]
        for url in candidates:
            if url not in found:
                found.append(url)

        return found

    def _parse_sitemap(self, url: str, max_depth: int = 3) -> list[str]:
        """
        Parse a sitemap (flat or index).
        Recursively follows sitemap index entries up to max_depth.
        """
        try:
            resp = self.client.get(url)
        except Exception as e:
            log.debug(f"Could not fetch sitemap {url}: {e}")
            return []

        content = resp.content
        # Handle gzipped sitemaps
        if url.endswith(".gz") or resp.headers.get("content-type", "").find("gzip") >= 0:
            try:
                content = gzip.decompress(content)
            except Exception:
                pass

        try:
            root = ET.fromstring(content)
        except ET.ParseError as e:
            log.warning(f"XML parse error for {url}: {e}")
            return []

        tag = root.tag.lower()

        # Sitemap index
        if "sitemapindex" in tag:
            if max_depth <= 0:
                return []
            urls: list[str] = []
            for sitemap_el in root.findall(".//sm:loc", NS) or root.findall(".//{*}loc"):
                child_url = (sitemap_el.text or "").strip()
                if child_url:
                    urls.extend(self._parse_sitemap(child_url, max_depth - 1))
            return urls

        # Regular sitemap
        urls = []
        for loc_el in root.findall(".//sm:loc", NS) or root.findall(".//{*}loc"):
            url_text = (loc_el.text or "").strip()
            if url_text:
                urls.append(url_text)
        return urls
