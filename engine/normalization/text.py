"""
Text normalization utilities.
"""
from __future__ import annotations

import re
import html


def normalize_text(text: str, max_len: int = 0) -> str:
    """Clean and normalize any text string."""
    if not text:
        return ""
    # Unescape HTML entities
    text = html.unescape(text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # Remove zero-width chars
    text = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", text)
    if max_len and len(text) > max_len:
        text = text[:max_len].rstrip()
    return text


def normalize_name(name: str) -> str:
    """Normalize a product name — capitalize, trim etc."""
    if not name:
        return ""
    name = normalize_text(name, max_len=300)
    # Remove duplicate whitespace again after unescape
    name = re.sub(r"\s{2,}", " ", name).strip()
    return name


def extract_bullet_points(description: str) -> list[str]:
    """Try to split a description into bullet points."""
    if not description:
        return []
    # Split on newlines, bullets, dashes
    parts = re.split(r"[\n\r]+|[•·\-–—]\s+", description)
    bullets = []
    for part in parts:
        part = normalize_text(part)
        if len(part) > 5:
            bullets.append(part)
    return bullets[:10]


def detect_gender(text: str, category: str = "") -> str:
    """
    Detect gender target from product name/category.
    Returns: women | men | kids | unisex
    """
    combined = (text + " " + category).lower()

    # Hebrew — check kids first (most specific), then women, then men
    if re.search(r"ילדים|ילדה|ילד[^י]|פעוט|תינוק|בנות|ילדות", combined):
        return "kids"
    if re.search(r"נשים|אישה|בנות", combined):
        return "women"
    if re.search(r"גברים|איש|בנים", combined):
        return "men"

    # English
    if re.search(r"\b(women|woman|ladies|girls|female)\b", combined):
        return "women"
    if re.search(r"\b(men|man|boys|male|guy)\b", combined):
        return "men"
    if re.search(r"\b(kids|children|baby|toddler|infant)\b", combined):
        return "kids"

    return "unisex"


def detect_material(description: str) -> tuple[str, str]:
    """
    Extract material and composition from a description string.
    Returns (fabric_type, composition_text).
    """
    if not description:
        return "", ""

    desc_lower = description.lower()

    # Look for percentage patterns: "100% Cotton", "80% פוליאסטר"
    comp_match = re.findall(r"\d{1,3}%\s*[\w\s]+", description, re.IGNORECASE)
    composition = ", ".join(comp_match[:4]) if comp_match else ""

    # Identify dominant fabric
    fabric = ""
    fabric_keywords = {
        "cotton": ["cotton", "כותנה", "קוטון"],
        "polyester": ["polyester", "פוליאסטר"],
        "viscose": ["viscose", "viscosa", "ויסקוזה"],
        "linen": ["linen", "פשתן"],
        "wool": ["wool", "צמר"],
        "denim": ["denim", "ג'ינס", "jeans"],
        "leather": ["leather", "עור"],
        "silk": ["silk", "משי"],
        "nylon": ["nylon", "ניילון"],
        "spandex": ["spandex", "elastane", "ספנדקס", "אלסטן"],
    }
    for fabric_name, keywords in fabric_keywords.items():
        if any(kw in desc_lower for kw in keywords):
            fabric = fabric_name
            break

    return fabric, composition


def detect_is_new_collection(text: str) -> bool:
    """Detect if a product belongs to a new collection."""
    combined = text.lower()
    patterns = [
        r"new\s*(in|arrival|collection|season)",
        r"חדש|קולקציה\s*חדשה",
        r"\bnew\b",
        r"ss2[0-9]|fw2[0-9]|aw2[0-9]",  # Season codes
    ]
    return any(re.search(p, combined) for p in patterns)
