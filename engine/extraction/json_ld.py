"""
JSON-LD extraction from HTML pages.

Handles:
- Multiple JSON-LD blocks
- Nested @graph structures
- Malformed JSON (best-effort repair)
- Product, BreadcrumbList, Offer schemas
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

from bs4 import BeautifulSoup

log = logging.getLogger("engine.extraction.json_ld")


def _try_parse_json(raw: str) -> Optional[dict | list]:
    """Parse JSON, trying light repair if initial parse fails."""
    raw = raw.strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Light repair: remove trailing commas
        repaired = re.sub(r",\s*([}\]])", r"\1", raw)
        try:
            return json.loads(repaired)
        except json.JSONDecodeError:
            return None


def extract_json_ld(html: str) -> list[dict]:
    """
    Extract all JSON-LD blocks from HTML.
    Returns a flat list of @type objects (unwraps @graph).
    """
    soup = BeautifulSoup(html, "lxml")
    results: list[dict] = []

    for tag in soup.find_all("script", type="application/ld+json"):
        raw = tag.get_text(strip=True)
        parsed = _try_parse_json(raw)
        if parsed is None:
            log.debug("JSON-LD block failed to parse")
            continue

        # Normalise to list
        items: list[dict] = parsed if isinstance(parsed, list) else [parsed]

        for item in items:
            if not isinstance(item, dict):
                continue
            # Unwrap @graph
            if "@graph" in item:
                for node in item["@graph"]:
                    if isinstance(node, dict):
                        results.append(node)
            else:
                results.append(item)

    return results


def find_product_json_ld(json_ld_blocks: list[dict]) -> Optional[dict]:
    """Find the Product block from a list of JSON-LD objects."""
    for block in json_ld_blocks:
        t = block.get("@type", "")
        types = [t] if isinstance(t, str) else t
        if "Product" in types:
            return block
    return None


def find_breadcrumbs_json_ld(json_ld_blocks: list[dict]) -> list[str]:
    """Extract breadcrumb labels from BreadcrumbList JSON-LD."""
    for block in json_ld_blocks:
        t = block.get("@type", "")
        types = [t] if isinstance(t, str) else t
        if "BreadcrumbList" in types:
            items = block.get("itemListElement", [])
            return [
                item.get("name", item.get("item", {}).get("name", ""))
                for item in items
                if isinstance(item, dict)
            ]
    return []


def parse_product_from_json_ld(block: dict) -> dict[str, Any]:
    """
    Parse a Product JSON-LD block into a flat normalized dict.
    Returns partial dict — caller fills in missing fields.
    """
    result: dict[str, Any] = {}

    result["product_name"] = block.get("name", "")
    result["original_description"] = block.get("description", "")
    result["brand"] = _extract_brand(block.get("brand"))
    result["sku_if_available"] = block.get("sku", block.get("mpn", ""))
    result["source_product_reference"] = block.get("@id", block.get("productID", ""))

    # Images
    image_field = block.get("image", [])
    if isinstance(image_field, str):
        result["image_urls"] = [image_field]
    elif isinstance(image_field, list):
        result["image_urls"] = [
            img if isinstance(img, str) else img.get("url", img.get("contentUrl", ""))
            for img in image_field
            if img
        ]
    elif isinstance(image_field, dict):
        result["image_urls"] = [image_field.get("url", image_field.get("contentUrl", ""))]

    # Offers
    offers = block.get("offers", block.get("offer"))
    if offers:
        offers_list = offers if isinstance(offers, list) else [offers]
        first = offers_list[0] if offers_list else {}
        result["current_price"] = _parse_price(first.get("price"))
        result["currency"] = first.get("priceCurrency", "ILS")
        availability = first.get("availability", "")
        if "InStock" in availability:
            result["in_stock"] = True
            result["out_of_stock"] = False
        elif "OutOfStock" in availability:
            result["in_stock"] = False
            result["out_of_stock"] = True

        # Price range if multiple offers
        if len(offers_list) > 1:
            prices = [
                _parse_price(o.get("price"))
                for o in offers_list
                if o.get("price")
            ]
            prices = [p for p in prices if p is not None]
            if prices:
                result["current_price"] = min(prices)

    # Color / size properties
    for prop in block.get("additionalProperty", []):
        if not isinstance(prop, dict):
            continue
        pname = prop.get("name", "").lower()
        pvalue = prop.get("value", "")
        if "color" in pname or "colour" in pname:
            result.setdefault("colors_available", [])
            if pvalue:
                result["colors_available"].append(str(pvalue))
        elif "size" in pname or "מידה" in pname:
            result.setdefault("sizes_available", [])
            if pvalue:
                result["sizes_available"].append(str(pvalue))
        elif "material" in pname or "חומר" in pname:
            result["material_info"] = str(pvalue)

    return result


def _extract_brand(brand_field: Any) -> Optional[str]:
    if not brand_field:
        return None
    if isinstance(brand_field, str):
        return brand_field
    if isinstance(brand_field, dict):
        return brand_field.get("name", "")
    return None


def _parse_price(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None
