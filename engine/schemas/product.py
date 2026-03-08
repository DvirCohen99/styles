"""
Product schemas — Pydantic v2 models for the full normalized product lifecycle.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator, model_validator
import hashlib


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ──────────────────────────────────────────────────────────────────────────────
# ProductVariant
# ──────────────────────────────────────────────────────────────────────────────

class ProductVariant(BaseModel):
    """Single SKU variant within a product (color × size combination)."""

    variant_id: Optional[str] = None
    sku: Optional[str] = None
    color: Optional[str] = None
    size: Optional[str] = None
    size_normalized: Optional[str] = None          # XS / S / M / L / XL / etc.
    price: Optional[float] = None
    original_price: Optional[float] = None
    in_stock: bool = True
    stock_quantity: Optional[int] = None
    barcode: Optional[str] = None
    image_url: Optional[str] = None
    extra: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_on_sale(self) -> bool:
        return bool(
            self.original_price
            and self.price
            and self.original_price > self.price
        )


# ──────────────────────────────────────────────────────────────────────────────
# RawProductPayload  (before normalization)
# ──────────────────────────────────────────────────────────────────────────────

class RawProductPayload(BaseModel):
    """Unprocessed payload exactly as extracted from the page."""

    source_site: str
    product_url: str
    html_snapshot: Optional[str] = None          # full page HTML (truncated)
    json_ld_data: Optional[list[dict]] = None     # all JSON-LD blocks found
    script_payload: Optional[dict] = None         # hydration / window.__INITIAL_STATE__
    dom_data: Optional[dict] = None               # raw DOM-extracted fields
    headers_received: dict[str, str] = Field(default_factory=dict)
    status_code: int = 200
    extraction_method: str = "unknown"            # json_ld | script | dom | heuristic
    extracted_at: str = Field(default_factory=_now_iso)
    extra: dict[str, Any] = Field(default_factory=dict)


# ──────────────────────────────────────────────────────────────────────────────
# NormalizedProduct  (fully normalized, Firebase-ready)
# ──────────────────────────────────────────────────────────────────────────────

class NormalizedProduct(BaseModel):
    """
    Fully normalized product record.
    All fields optional except the identity triad.
    """

    # ── IDENTITY ──────────────────────────────────────────────────────────────
    product_id: str = ""                          # auto-generated: md5(source_site + product_url)
    source_site: str                              # "renuar" | "zara" | ...
    source_name: str                              # "Renuar" | "Zara Israel" | ...
    product_url: str
    canonical_url: Optional[str] = None
    source_product_reference: Optional[str] = None  # internal ID from source
    sku_if_available: Optional[str] = None
    source_category_url: Optional[str] = None

    # ── TEXT ──────────────────────────────────────────────────────────────────
    product_name: str = ""
    original_product_title: Optional[str] = None
    short_description: Optional[str] = None
    original_description: Optional[str] = None
    bullet_points: list[str] = Field(default_factory=list)
    breadcrumbs: list[str] = Field(default_factory=list)
    searchable_text_blob: Optional[str] = None

    # ── PRICING ───────────────────────────────────────────────────────────────
    current_price: Optional[float] = None
    original_price: Optional[float] = None
    currency: str = "ILS"
    is_on_sale: bool = False
    discount_amount: Optional[float] = None
    discount_percent: Optional[float] = None
    sale_label: Optional[str] = None
    promotion_text: Optional[str] = None

    # ── FRESHNESS ─────────────────────────────────────────────────────────────
    first_seen_at: Optional[str] = None
    last_seen_at: Optional[str] = None
    scraped_at: str = Field(default_factory=_now_iso)
    source_publish_date_if_detected: Optional[str] = None
    source_last_modified_if_detected: Optional[str] = None

    # ── CLASSIFICATION ────────────────────────────────────────────────────────
    category: Optional[str] = None
    subcategory: Optional[str] = None
    product_type: Optional[str] = None
    collection: Optional[str] = None
    subcollection: Optional[str] = None
    collection_type: Optional[str] = None
    is_new_collection: bool = False
    brand: Optional[str] = None
    gender_target: Optional[str] = None           # women | men | kids | unisex

    # ── VISUAL ────────────────────────────────────────────────────────────────
    primary_image_url: Optional[str] = None
    image_urls: list[str] = Field(default_factory=list)
    image_count: int = 0
    video_urls_if_available: list[str] = Field(default_factory=list)

    # ── VARIANTS ──────────────────────────────────────────────────────────────
    colors_available: list[str] = Field(default_factory=list)
    sizes_available: list[str] = Field(default_factory=list)
    size_grid_raw: Optional[str] = None
    size_labels_normalized: list[str] = Field(default_factory=list)
    color_variant_objects: list[ProductVariant] = Field(default_factory=list)
    per_variant_stock_if_available: dict[str, bool] = Field(default_factory=dict)
    per_variant_price_if_available: dict[str, float] = Field(default_factory=dict)
    default_selected_variant: Optional[str] = None

    # ── STOCK ─────────────────────────────────────────────────────────────────
    stock_status: str = "unknown"                 # in_stock | out_of_stock | low_stock | unknown
    availability_text: Optional[str] = None
    in_stock: bool = True
    low_stock: bool = False
    out_of_stock: bool = False
    backorder_if_available: bool = False

    # ── SPEC / MATERIAL ───────────────────────────────────────────────────────
    material_info: Optional[str] = None
    composition: Optional[str] = None
    fabric_type: Optional[str] = None
    care_info: Optional[str] = None
    sleeve_type: Optional[str] = None
    neckline: Optional[str] = None
    length_type: Optional[str] = None
    heel_height: Optional[str] = None
    closure_type: Optional[str] = None
    fit_description: Optional[str] = None
    measurements_if_available: Optional[dict] = None

    # ── SYSTEM ────────────────────────────────────────────────────────────────
    raw_source_payload: Optional[dict] = None
    parser_version: str = "1.0.0"
    parser_status: str = "ok"                     # ok | partial | failed
    extraction_confidence: float = 1.0            # 0.0 – 1.0
    warnings: list[str] = Field(default_factory=list)
    is_active: bool = True
    is_missing_from_source: bool = False

    # ── Validators ────────────────────────────────────────────────────────────

    @model_validator(mode="after")
    def _compute_derived(self) -> "NormalizedProduct":
        # Auto-generate product_id if not provided
        if not self.product_id:
            self.product_id = self.__class__.make_id(self.source_site, self.product_url)

        # image_count
        self.image_count = len(self.image_urls)
        if self.image_urls and not self.primary_image_url:
            self.primary_image_url = self.image_urls[0]

        # sale fields
        if self.current_price and self.original_price and self.original_price > self.current_price:
            self.is_on_sale = True
            self.discount_amount = round(self.original_price - self.current_price, 2)
            self.discount_percent = round(
                (self.discount_amount / self.original_price) * 100, 1
            )

        # stock_status
        if self.out_of_stock:
            self.stock_status = "out_of_stock"
            self.in_stock = False
        elif self.low_stock:
            self.stock_status = "low_stock"
        elif self.in_stock:
            self.stock_status = "in_stock"

        # searchable text blob
        parts = filter(None, [
            self.product_name,
            self.short_description,
            " ".join(self.breadcrumbs),
            self.category,
            self.subcategory,
            self.brand,
            " ".join(self.colors_available),
            " ".join(self.sizes_available),
        ])
        self.searchable_text_blob = " ".join(parts)

        return self

    @field_validator("product_id", mode="before")
    @classmethod
    def _ensure_product_id(cls, v: Any) -> str:
        return v or ""

    @classmethod
    def make_id(cls, source_site: str, product_url: str) -> str:
        key = f"{source_site}:{product_url.split('?')[0]}"
        return hashlib.md5(key.encode()).hexdigest()

    def to_firebase_dict(self) -> dict:
        """Return a flat Firebase-ready dict (no nested Pydantic objects)."""
        d = self.model_dump(exclude={"raw_source_payload"})
        # Flatten variants to plain dicts
        d["color_variant_objects"] = [v.model_dump() for v in self.color_variant_objects]
        return d

    def to_json_dict(self) -> dict:
        """Full JSON-serializable dict including raw payload."""
        d = self.model_dump()
        d["color_variant_objects"] = [v.model_dump() for v in self.color_variant_objects]
        return d

    @property
    def completeness_score(self) -> float:
        """
        0–1 score of how complete this product record is.
        Based on 10 key fields: name, price, images, category, description,
        sizes, colors, brand, stock status, and identity reference.
        """
        checks = [
            bool(self.product_name),
            bool(self.current_price),
            bool(self.image_urls),
            bool(self.category),
            bool(self.short_description or self.original_description),
            bool(self.sizes_available or self.color_variant_objects),
            bool(self.colors_available),
            bool(self.brand),
            self.stock_status != "unknown",
            bool(self.sku_if_available or self.source_product_reference),
        ]
        return round(sum(checks) / len(checks), 2)

    @property
    def is_valid(self) -> bool:
        """True if the product has the minimum required fields."""
        return bool(self.product_name and self.product_url and self.source_site)
