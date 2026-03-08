"""
Parse result and health check schemas.
"""
from __future__ import annotations

from typing import Any, Optional
from pydantic import BaseModel, Field

from engine.schemas.product import NormalizedProduct


class ParseWarning(BaseModel):
    """Non-fatal issue encountered during parsing."""
    field: str                          # which field is affected
    message: str
    severity: str = "warning"           # warning | error | info
    raw_value: Optional[Any] = None


class ParseResult(BaseModel):
    """Result of parsing a single product page."""
    success: bool
    product_url: str
    source_site: str
    product: Optional[NormalizedProduct] = None
    warnings: list[ParseWarning] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    extraction_method: str = "unknown"
    confidence: float = 1.0
    skipped: bool = False
    skip_reason: Optional[str] = None

    @property
    def is_partial(self) -> bool:
        return self.success and len(self.warnings) > 0


class HealthCheckResult(BaseModel):
    """Result of a source healthcheck."""
    source_key: str
    status: str                          # ok | degraded | failed
    reachable: bool = False
    discovery_ok: bool = False
    parse_ok: bool = False
    sample_product_url: Optional[str] = None
    sample_product_name: Optional[str] = None
    error: Optional[str] = None
    warnings: list[str] = Field(default_factory=list)
    response_time_ms: Optional[float] = None
    checked_at: Optional[str] = None
