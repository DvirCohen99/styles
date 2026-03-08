"""
Variant normalization — sizes, colors, ProductVariant objects.
"""
from __future__ import annotations

import re
from typing import Optional

from engine.schemas.product import ProductVariant


# Size normalization maps
SIZE_MAP: dict[str, str] = {
    # International
    "xxs": "XXS", "xs": "XS", "s": "S", "m": "M", "l": "L",
    "xl": "XL", "xxl": "XXL", "2xl": "XXL", "3xl": "XXXL",
    "xxxl": "XXXL", "4xl": "4XL",
    # Hebrew
    "קטן מאוד": "XS", "קטן": "S", "בינוני": "M",
    "גדול": "L", "גדול מאוד": "XL",
    # Numeric to alpha
    "32": "XXS", "34": "XS", "36": "S", "38": "M",
    "40": "L", "42": "XL", "44": "XXL", "46": "XXXL",
    # One size
    "one size": "ONE SIZE", "os": "ONE SIZE", "one": "ONE SIZE",
    "מידה אחת": "ONE SIZE",
}


def normalize_sizes(sizes: list[str]) -> list[str]:
    """
    Normalize size labels to standard forms.
    Filters empty/invalid sizes.
    """
    if not sizes:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for s in sizes:
        clean = re.sub(r"\s+", " ", str(s)).strip()
        if not clean or clean.lower() in ("בחר מידה", "select size", "--", "size", "מידה"):
            continue
        normalized = SIZE_MAP.get(clean.lower(), clean.upper())
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def normalize_colors(colors: list[str]) -> list[str]:
    """Clean and deduplicate color names."""
    if not colors:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for c in colors:
        clean = re.sub(r"\s+", " ", str(c)).strip()
        if not clean or clean.lower() in ("select color", "בחר צבע", "--"):
            continue
        # Capitalize first letter
        clean = clean[0].upper() + clean[1:] if clean else clean
        if clean not in seen:
            seen.add(clean)
            result.append(clean)
    return result


def normalize_variants(raw_variants: list[dict]) -> list[ProductVariant]:
    """
    Convert a list of raw variant dicts (from Shopify / API etc.)
    into ProductVariant objects.

    Tries to extract: id, sku, color, size, price, compare_at_price, available
    """
    variants: list[ProductVariant] = []

    for raw in raw_variants:
        if not isinstance(raw, dict):
            continue

        # Extract color and size from named fields first
        color = _pick_field(raw, ["color", "colour", "צבע"])
        size = _pick_field(raw, ["size", "מידה", "גודל"])

        # If not found in named fields, use option1/option2 with heuristics
        if not color and not size:
            opt1 = str(raw.get("option1", "") or "").strip()
            opt2 = str(raw.get("option2", "") or "").strip()
            opt3 = str(raw.get("option3", "") or "").strip()
            # Assign based on whether value looks like a size
            for opt in [opt1, opt2, opt3]:
                if not opt:
                    continue
                if _looks_like_size(opt) and not size:
                    size = opt
                elif not color:
                    color = opt
        elif not size:
            # We have color; try option fields for size
            for opt_key in ["option1", "option2", "option3"]:
                opt = str(raw.get(opt_key, "") or "").strip()
                if opt and _looks_like_size(opt):
                    size = opt
                    break
        elif not color:
            # We have size; try option fields for color
            for opt_key in ["option1", "option2", "option3"]:
                opt = str(raw.get(opt_key, "") or "").strip()
                if opt and not _looks_like_size(opt):
                    color = opt
                    break

        price_raw = raw.get("price", raw.get("sellPrice", raw.get("salePrice")))
        orig_raw = raw.get("compare_at_price", raw.get("compareAtPrice", raw.get("originalPrice")))

        from engine.normalization.price import normalize_price
        price = normalize_price(price_raw)
        orig = normalize_price(orig_raw)
        if orig and price and orig <= price:
            orig = None

        available_raw = raw.get("available", raw.get("in_stock", raw.get("inStock", True)))
        if isinstance(available_raw, str):
            available = available_raw.lower() not in ("false", "0", "out_of_stock", "unavailable")
        else:
            available = bool(available_raw)

        variant = ProductVariant(
            variant_id=str(raw.get("id", raw.get("variantId", "")) or ""),
            sku=str(raw.get("sku", "") or ""),
            color=color,
            size=size,
            size_normalized=SIZE_MAP.get((size or "").lower(), size),
            price=price,
            original_price=orig,
            in_stock=available,
            barcode=str(raw.get("barcode", "") or ""),
            image_url=raw.get("featured_image", {}).get("src") if isinstance(raw.get("featured_image"), dict) else None,
        )
        variants.append(variant)

    return variants


def _pick_field(d: dict, keys: list[str]) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if v and str(v).strip() and str(v).strip().lower() not in ("null", "none", ""):
            return str(v).strip()
    return None


def _looks_like_size(s: str) -> bool:
    """Heuristic: does this string look like a size?"""
    size_patterns = [
        r"^(XXS|XS|S|M|L|XL|XXL|XXXL|2XL|3XL|4XL)$",
        r"^\d{2,3}$",  # Numeric sizes: 38, 40, etc.
        r"^(one\s*size|os)$",
        r"^\d{1,2}/\d{1,2}$",  # "32/34"
        r"^[0-9]+(Y|M|W)$",   # Kids sizes
    ]
    s_clean = s.strip().upper()
    return any(re.match(p, s_clean, re.I) for p in size_patterns)
