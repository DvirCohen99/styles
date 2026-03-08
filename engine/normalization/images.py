"""
Image URL normalization and deduplication.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse, urljoin


def normalize_image_urls(
    urls: list[str],
    base_url: str = "",
    max_count: int = 10,
    prefer_large: bool = True,
) -> list[str]:
    """
    Clean, deduplicate, and filter image URLs.
    Strips query strings, upgrades to HTTPS, resolves relative URLs.
    """
    seen: set[str] = set()
    result: list[str] = []

    for url in urls:
        if not url:
            continue
        url = str(url).strip()

        # Resolve relative URLs
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/") and base_url:
            url = urljoin(base_url, url)
        elif not url.startswith("http"):
            if base_url:
                url = urljoin(base_url, url)
            else:
                continue

        # Upgrade to HTTPS
        url = url.replace("http://", "https://", 1)

        # Strip query string (keep path)
        url = url.split("?")[0]

        # Skip non-image URLs
        if not re.search(r"\.(jpg|jpeg|png|webp|avif|gif)($|/)", url, re.I):
            # Maybe it's a clean CDN URL without extension — keep if it looks like an image path
            if not re.search(r"/image|/photo|/product|/media", url, re.I):
                continue

        # Skip tracking pixels and icons
        if re.search(r"1x1|pixel|icon|logo|spinner|placeholder|blank|favicon", url, re.I):
            continue

        # Deduplicate
        key = _canonical_image_key(url)
        if key in seen:
            continue
        seen.add(key)

        # Upgrade to larger variant if possible
        if prefer_large:
            url = _upgrade_to_large(url)

        result.append(url)
        if len(result) >= max_count:
            break

    return result


def _canonical_image_key(url: str) -> str:
    """Normalize URL for deduplication purposes."""
    # Remove size suffixes: _300x300, _small, etc.
    key = re.sub(r"_\d+x\d+", "", url)
    key = re.sub(r"_(thumb|small|medium|large|grande|master)", "", key, flags=re.I)
    return key.lower()


def _upgrade_to_large(url: str) -> str:
    """
    Try to get a larger image version.
    Handles Shopify CDN and common CDN patterns.
    """
    # Shopify: replace _small, _compact, _medium with nothing (original size)
    shopify_pattern = r"(_(?:pico|icon|thumb|small|compact|medium|large|grande|1024x1024|2048x2048|master))(\.\w+$)"
    if re.search(r"cdn\.shopify\.com", url, re.I):
        url = re.sub(r"_(?:pico|icon|thumb|small|compact|medium)\.", ".", url)

    # Zara CDN: swap w/width parameter
    if "static.zara.net" in url:
        url = re.sub(r"/w/\d+/", "/w/750/", url)

    return url
