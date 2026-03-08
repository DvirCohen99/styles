"""
Gemini AI processor.
Enriches raw scraped products with:
  - description_ai_expanded (200-400 words, Hebrew, marketing tone)
  - tags (10-20 smart Hebrew+English tags)
  - AI-detected colors and sizes refinement
  - Category correction

Free tier: gemini-2.0-flash — 1500 req/day, 15 RPM
We batch 5 products per API call to conserve quota.
"""
from __future__ import annotations

import json
import re
import time
from typing import Optional
from dataclasses import dataclass, field, asdict

import google.generativeai as genai

import config
from scrapers.base import RawProduct
from utils.logger import get_logger
from utils.rate_limiter import GeminiRateLimiter

log = get_logger("ai_processor")


@dataclass
class EnrichedProduct:
    """Final product ready for Firestore."""
    # Identity
    product_id: str
    site: str
    name: str
    original_url: str

    # Dates
    scrape_date: str
    first_seen_date: str

    # Descriptions
    description_short: str
    description_ai_expanded: str

    # AI enrichment
    tags: list[str] = field(default_factory=list)

    # Variants
    colors_available: list[str] = field(default_factory=list)
    sizes_available: list[str] = field(default_factory=list)

    # Pricing
    price: Optional[float] = None
    original_price: Optional[float] = None
    discount_percentage: Optional[float] = None
    is_on_sale: bool = False
    currency: str = "ILS"

    # Media
    images: list[str] = field(default_factory=list)

    # Classification
    category: str = ""
    sub_category: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        # Remove None values for cleaner Firestore docs
        return {k: v for k, v in d.items() if v is not None}


# ── Prompt templates ──────────────────────────────────────
SYSTEM_PROMPT = """אתה מומחה שיווק אופנה ישראלי.
תפקידך: לקבל מידע גולמי על מוצרי אופנה וליצור תוכן שיווקי מושך בעברית.

חוקים:
1. description_ai_expanded: 200-400 מילה, סגנון שיווקי ומשכנע, בעברית, תאר את המוצר, חומרים, סגנון, אירועים מתאימים
2. tags: 10-20 תגיות, מחצית עברית מחצית אנגלית, רלוונטיות למוצר
3. category: אחת מ: חולצות, מכנסיים, שמלות, חצאיות, מעילים, ג'קטים, נעליים, אביזרים, תיקים, ספורט, לינגרי, בגדי ים, פיג'מות, אחר
4. sub_category: תת-קטגוריה ספציפית
5. colors_detected: צבעים שזיהית מהשם/תיאור (עברית)
6. Return ONLY valid JSON, no markdown fences"""

BATCH_PROMPT_TEMPLATE = """עבד על {n} מוצרי אופנה הבאים. החזר JSON array עם {n} אובייקטים, בדיוק בסדר הזה:

מוצרים:
{products_json}

פורמט לכל מוצר (JSON):
{{
  "description_ai_expanded": "...",
  "tags": ["tag1", "tag2", ...],
  "category": "...",
  "sub_category": "...",
  "colors_detected": ["..."]
}}

החזר רק JSON array ללא markdown:"""


class GeminiProcessor:
    def __init__(self):
        if not config.GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not configured")
        genai.configure(api_key=config.GEMINI_API_KEY)
        self._model = genai.GenerativeModel(
            model_name=config.GEMINI_MODEL,
            system_instruction=SYSTEM_PROMPT,
            generation_config={
                "temperature": 0.7,
                "max_output_tokens": 8192,
                "response_mime_type": "application/json",
            },
        )
        self._rate_limiter = GeminiRateLimiter(rpm=config.GEMINI_RPM_LIMIT)

    def enrich_batch(
        self,
        products: list[RawProduct],
        existing_dates: dict[str, str] | None = None,
    ) -> list[EnrichedProduct]:
        """
        Enrich a list of raw products.
        existing_dates: {product_id: first_seen_date} from Firestore (for upsert logic).
        """
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        results: list[EnrichedProduct] = []

        # Process in batches to minimise API calls
        batch_size = config.AI_BATCH_SIZE
        for i in range(0, len(products), batch_size):
            batch = products[i : i + batch_size]
            log.info(f"AI processing batch {i//batch_size + 1} ({len(batch)} products)...")
            try:
                enrichments = self._call_gemini(batch)
            except Exception as e:
                log.error(f"Gemini batch failed: {e} — using defaults")
                enrichments = [self._default_enrichment(p) for p in batch]

            for raw, enr in zip(batch, enrichments):
                pid = raw.product_id
                first_seen = (existing_dates or {}).get(pid, now_iso)
                ep = EnrichedProduct(
                    product_id=pid,
                    site=raw.site,
                    name=raw.name,
                    original_url=raw.original_url,
                    scrape_date=now_iso,
                    first_seen_date=first_seen,
                    description_short=raw.description_short,
                    description_ai_expanded=enr.get("description_ai_expanded", raw.description_short),
                    tags=enr.get("tags", []),
                    colors_available=self._merge_lists(raw.colors_available, enr.get("colors_detected", [])),
                    sizes_available=raw.sizes_available,
                    price=raw.price,
                    original_price=raw.original_price,
                    discount_percentage=raw.discount_percentage,
                    is_on_sale=raw.is_on_sale,
                    currency=raw.currency,
                    images=raw.images,
                    category=enr.get("category", raw.category) or raw.category,
                    sub_category=enr.get("sub_category", raw.sub_category),
                )
                results.append(ep)

        return results

    def _call_gemini(self, batch: list[RawProduct]) -> list[dict]:
        """Single Gemini API call for a batch. Returns list of enrichment dicts."""
        self._rate_limiter.wait()

        products_summary = []
        for p in batch:
            products_summary.append({
                "name": p.name,
                "description": p.description_short[:300],
                "category_hint": p.category,
                "price": p.price,
                "colors_hint": p.colors_available[:5],
                "sizes_hint": p.sizes_available[:8],
                "site": p.site,
            })

        prompt = BATCH_PROMPT_TEMPLATE.format(
            n=len(batch),
            products_json=json.dumps(products_summary, ensure_ascii=False, indent=2),
        )

        response = self._model.generate_content(prompt)
        raw_text = response.text.strip()

        # Parse JSON — handle potential wrapping
        raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
        raw_text = re.sub(r"\s*```$", "", raw_text)

        parsed = json.loads(raw_text)
        if isinstance(parsed, dict):
            parsed = [parsed]

        # Ensure we have the right count
        while len(parsed) < len(batch):
            parsed.append(self._default_enrichment(batch[len(parsed)]))

        return parsed[:len(batch)]

    @staticmethod
    def _default_enrichment(product: RawProduct) -> dict:
        """Fallback when Gemini is unavailable."""
        return {
            "description_ai_expanded": product.description_short or f"מוצר אופנה: {product.name}",
            "tags": [product.site, "אופנה", "fashion", product.name.split()[0] if product.name else ""],
            "category": product.category or "אופנה",
            "sub_category": "",
            "colors_detected": product.colors_available,
        }

    @staticmethod
    def _merge_lists(a: list, b: list) -> list:
        seen = set()
        result = []
        for item in list(a) + list(b):
            if item and item not in seen:
                seen.add(item)
                result.append(item)
        return result
