"""
Source-level schemas — metadata, stats, health.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from pydantic import BaseModel, Field


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SourceMeta(BaseModel):
    """Static metadata about a source/site."""
    source_key: str
    source_name: str
    base_url: str
    platform_family: str = "unknown"       # shopify | magento | woocommerce | custom | unknown
    priority: int = 5                      # 1 = highest priority
    extraction_order: int = 0             # run order
    active: bool = True
    requires_playwright: bool = False
    js_heavy: bool = False
    has_sitemap: bool = True
    has_api: bool = False
    language: str = "he"
    country: str = "IL"
    currency: str = "ILS"
    notes: str = ""
    extra: dict[str, Any] = Field(default_factory=dict)


class SourceStats(BaseModel):
    """Per-source runtime statistics for a single scrape run."""
    source_key: str
    run_id: Optional[str] = None
    started_at: str = Field(default_factory=_now_iso)
    finished_at: Optional[str] = None

    # Counts
    urls_discovered: int = 0
    products_attempted: int = 0
    products_parsed: int = 0
    products_failed: int = 0
    products_skipped: int = 0
    products_saved: int = 0

    # Specific counts
    total_live_products: int = 0
    sale_products_count: int = 0
    new_collection_products_count: int = 0
    out_of_stock_count: int = 0
    partial_parse_count: int = 0
    warning_count: int = 0

    # Health
    last_successful_scrape: Optional[str] = None
    parser_health_status: str = "unknown"          # ok | degraded | failed
    error_messages: list[str] = Field(default_factory=list)

    # Timing
    duration_sec: Optional[float] = None

    def mark_finished(self) -> None:
        self.finished_at = _now_iso()
        if self.started_at:
            from datetime import datetime, timezone
            start = datetime.fromisoformat(self.started_at)
            now = datetime.now(timezone.utc)
            self.duration_sec = round((now - start).total_seconds(), 2)

        # Set health status
        if self.products_failed == 0 and self.products_parsed > 0:
            self.parser_health_status = "ok"
        elif self.products_parsed > 0:
            ratio = self.products_parsed / max(1, self.products_attempted)
            self.parser_health_status = "ok" if ratio >= 0.7 else "degraded"
        else:
            self.parser_health_status = "failed"

    def to_dict(self) -> dict:
        return self.model_dump()
