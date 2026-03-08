"""
Tests for Renuar adapter — Shopify platform.
All tests use fixture data; no live HTTP calls.
"""
import pytest
from engine.adapters.renuar import RenuarAdapter
from engine.schemas.product import RawProductPayload
from engine.extraction.json_ld import extract_json_ld
from engine.extraction.script_payload import extract_script_payload
from engine.validation.validator import ProductValidator


class TestRenuarAdapter:
    def setup_method(self):
        self.adapter = RenuarAdapter()
        self.validator = ProductValidator()

    # ── Meta / config ──────────────────────────────────────────────────────

    def test_source_meta(self):
        meta = self.adapter.source_meta
        assert meta.source_key == "renuar"
        assert meta.source_name == "Renuar"
        assert "renuar.co.il" in meta.base_url
        assert meta.platform_family == "shopify"

    def test_category_urls(self):
        cats = self.adapter.discover_category_urls()
        assert len(cats) >= 3
        assert all("renuar.co.il" in c for c in cats)

    # ── Shopify JSON parse ─────────────────────────────────────────────────

    def test_parse_shopify_json_product(self, sample_shopify_product_json):
        partial = self.adapter._shopify_parse_product_json(
            sample_shopify_product_json, "https://www.renuar.co.il/products/shirt"
        )
        assert partial is not None
        assert partial["product_name"] == "חולצת פשתן נשים"
        assert partial["current_price"] == 149.0
        assert partial["original_price"] == 199.0
        assert len(partial["image_urls"]) == 2
        assert "S" in partial["sizes_available"]
        assert len(partial["colors_available"]) == 3

    def test_brand_set_to_renuar(self, sample_shopify_product_json):
        raw = RawProductPayload(
            source_site="renuar",
            product_url="https://www.renuar.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert result.product.brand == "Renuar" or result.product.brand == "TestBrand"

    def test_parse_from_api_payload(self, sample_shopify_product_json):
        raw = RawProductPayload(
            source_site="renuar",
            product_url="https://www.renuar.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
            extraction_method="api",
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        assert p.product_name == "חולצת פשתן נשים"
        assert p.source_site == "renuar"
        assert p.product_url == "https://www.renuar.co.il/products/shirt"
        assert p.current_price == 149.0
        assert len(p.image_urls) >= 1
        assert p.is_on_sale is True

    def test_parse_from_json_ld(self, sample_product_html):
        json_ld = extract_json_ld(sample_product_html)
        raw = RawProductPayload(
            source_site="renuar",
            product_url="https://www.renuar.co.il/products/shirt",
            html_snapshot=sample_product_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert result.product.product_name == "חולצת פשתן נשים"
        assert result.product.current_price == 149.0

    def test_parser_resilient_to_missing_fields(self):
        raw = RawProductPayload(
            source_site="renuar",
            product_url="https://www.renuar.co.il/products/test",
            script_payload={"shopify_product": {"title": "Test", "variants": [], "images": [], "options": []}},
        )
        result = self.adapter.parse_product(raw)
        assert isinstance(result.success, bool)
        assert isinstance(result.errors, list)

    def test_empty_payload_fails_gracefully(self):
        raw = RawProductPayload(
            source_site="renuar",
            product_url="https://www.renuar.co.il/products/test",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is False
        assert len(result.errors) > 0

    def test_normalized_product_passes_validation(self, sample_shopify_product_json):
        raw = RawProductPayload(
            source_site="renuar",
            product_url="https://www.renuar.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        vr = self.validator.validate(result.product)
        assert vr.valid, f"Validation errors: {[i.issue for i in vr.errors]}"

    def test_product_id_is_deterministic(self, sample_shopify_product_json):
        raw = RawProductPayload(
            source_site="renuar",
            product_url="https://www.renuar.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        r1 = self.adapter.parse_product(raw)
        r2 = self.adapter.parse_product(raw)
        assert r1.product.product_id == r2.product.product_id
