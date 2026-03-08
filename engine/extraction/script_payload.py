"""
Inline script / hydration payload extraction.

Handles common patterns:
- window.__INITIAL_STATE__ = {...}
- window.__NEXT_DATA__ = {...}
- window.Shopify = {...}
- var pdpData = {...}
- application/json script tags
- __NUXT__ / __PRELOADED_STATE__
- REDUX state blobs
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

from bs4 import BeautifulSoup

log = logging.getLogger("engine.extraction.script_payload")


# Patterns to search for in script text
HYDRATION_PATTERNS = [
    # Next.js
    (r"window\.__NEXT_DATA__\s*=\s*(\{.+?\})\s*;?\s*$", "next_data"),
    # Generic initial state
    (r"window\.__INITIAL_STATE__\s*=\s*(\{.+)", "initial_state"),
    (r"window\.__PRELOADED_STATE__\s*=\s*(\{.+)", "preloaded_state"),
    # Nuxt
    (r"window\.__NUXT__\s*=\s*(\{.+)", "nuxt"),
    # Product data
    (r"var\s+pdpData\s*=\s*(\{.+)", "pdp_data"),
    (r"var\s+productData\s*=\s*(\{.+)", "product_data"),
    (r"window\.productData\s*=\s*(\{.+)", "product_data"),
    # Shopify
    (r"window\.ShopifyAnalytics\s*=\s*(\{.+)", "shopify_analytics"),
    (r'"product"\s*:\s*(\{.+?"id"\s*:.+?"title"\s*:.+?"variants"\s*:\s*\[)', "shopify_product"),
    # Redux / Vuex stores
    (r"window\.__REDUX_STATE__\s*=\s*(\{.+)", "redux_state"),
    # Generic data blobs
    (r'data-initial-props\s*=\s*"([^"]+)"', "data_props_attr"),
]


def _try_extract_json(text: str, pattern: str) -> Optional[dict | list]:
    """Try to extract and parse a JSON object from a regex match."""
    match = re.search(pattern, text, re.DOTALL | re.MULTILINE)
    if not match:
        return None
    raw = match.group(1)
    # Find balanced closing brace/bracket
    raw = _balance_json(raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Repair trailing commas
        repaired = re.sub(r",\s*([}\]])", r"\1", raw)
        try:
            return json.loads(repaired)
        except json.JSONDecodeError:
            return None


def _balance_json(raw: str) -> str:
    """Trim a string to its first balanced JSON object/array."""
    if not raw:
        return raw
    start_char = raw[0]
    end_char = "}" if start_char == "{" else "]"
    depth = 0
    in_string = False
    escaped = False
    for i, ch in enumerate(raw):
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == '"' and not escaped:
            in_string = not in_string
        if in_string:
            continue
        if ch == start_char:
            depth += 1
        elif ch == end_char:
            depth -= 1
            if depth == 0:
                return raw[:i + 1]
    return raw


def extract_script_payload(html: str) -> dict[str, Any]:
    """
    Scan all <script> tags and known window variable patterns.
    Returns a dict mapping payload_name -> parsed_data.
    """
    soup = BeautifulSoup(html, "lxml")
    payloads: dict[str, Any] = {}

    for script in soup.find_all("script"):
        # Handle application/json type
        if script.get("type") == "application/json":
            raw = script.get_text(strip=True)
            try:
                data = json.loads(raw)
                script_id = script.get("id", "")
                # Normalise well-known IDs to consistent keys
                if script_id == "__NEXT_DATA__":
                    payloads["next_data"] = data
                else:
                    key = script_id or f"app_json_{len(payloads)}"
                    payloads[key] = data
                continue
            except json.JSONDecodeError:
                pass

        script_text = script.get_text()
        if not script_text or len(script_text) < 20:
            continue

        for pattern, name in HYDRATION_PATTERNS:
            if name in payloads:
                continue
            result = _try_extract_json(script_text, pattern)
            if result is not None:
                payloads[name] = result
                log.debug(f"Extracted payload: {name}")

    return payloads


def find_shopify_product(payloads: dict, html: str = "") -> Optional[dict]:
    """
    Try to extract a Shopify product JSON from:
    1. window.ShopifyAnalytics.meta.product
    2. A /products/<handle>.json fetch result
    3. Inline __st=... meta tags
    """
    # Via analytics object
    analytics = payloads.get("shopify_analytics", {})
    if analytics:
        product = analytics.get("meta", {}).get("product")
        if product:
            return product

    # Direct product field in any payload
    for val in payloads.values():
        if isinstance(val, dict) and "variants" in val and "title" in val:
            return val

    return None


def find_next_product(payloads: dict) -> Optional[dict]:
    """
    Extract product data from a Next.js page payload.
    Traverses common prop paths: props.pageProps.product / .data.product
    """
    next_data = payloads.get("next_data", {})
    if not next_data:
        return None

    page_props = next_data.get("props", {}).get("pageProps", {})
    # Common paths
    candidates = [
        page_props.get("product"),
        page_props.get("data", {}).get("product") if isinstance(page_props.get("data"), dict) else None,
        page_props.get("initialProduct"),
        page_props.get("item"),
    ]
    for c in candidates:
        if isinstance(c, dict) and c:
            return c
    return None


def deep_get(data: Any, *keys: str) -> Any:
    """Safely navigate nested dicts/lists."""
    current = data
    for key in keys:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and key.isdigit():
            idx = int(key)
            current = current[idx] if idx < len(current) else None
        else:
            return None
    return current
