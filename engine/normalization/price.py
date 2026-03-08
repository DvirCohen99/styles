"""
Price normalization utilities.
"""
from __future__ import annotations

import re
from typing import Optional


def normalize_price(value) -> Optional[float]:
    """
    Convert any price representation to a float in the base currency unit.

    Handles:
    - Shopify prices in cents (12900 -> 129.0)
    - String prices with currency symbols ("₪129.00" -> 129.0)
    - Comma-separated thousands ("1,299" -> 1299.0)
    - Decimal comma ("129,90" -> 129.90)
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        val = float(value)
        # Shopify returns prices in cents (integer > reasonable max)
        # Threshold: prices > 10000 in ILS are extremely rare, so likely cents
        if val > 10000 and isinstance(value, int):
            val /= 100
        return round(val, 2) if val > 0 else None

    text = str(value).strip()
    # Remove currency symbols and whitespace
    cleaned = re.sub(r"[^\d.,]", "", text.replace("\xa0", "").replace(" ", ""))

    if not cleaned:
        return None

    # Comma handling
    if "," in cleaned and "." in cleaned:
        # e.g. "1,234.56" -> remove thousand sep
        cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        parts = cleaned.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            # Decimal comma: "129,90"
            cleaned = cleaned.replace(",", ".")
        else:
            # Thousand separator
            cleaned = cleaned.replace(",", "")

    try:
        val = float(cleaned)
        return round(val, 2) if val > 0 else None
    except ValueError:
        return None


def normalize_price_pair(
    current,
    original,
    shopify_cents: bool = False,
) -> tuple[Optional[float], Optional[float]]:
    """
    Normalize a (current_price, original_price) pair.
    Ensures original > current (otherwise original is discarded).
    """
    curr = normalize_price(current)
    orig = normalize_price(original)

    if shopify_cents:
        # Already handled by normalize_price int detection, but force it
        if isinstance(current, int) and current > 1000:
            curr = round(current / 100, 2)
        if isinstance(original, int) and original > 1000:
            orig = round(original / 100, 2)

    # Sanity check: original must be greater than current
    if curr and orig and orig <= curr:
        orig = None

    return curr, orig
