"""
Tests for Shopify mixin and Shopify-based adapters.
Tests use fixture data — no live HTTP calls.
"""
import pytest
from unittest.mock import MagicMock, patch

from engine.adapters._shopify_mixin import ShopifyMixin
from engine.adapters.renuar import RenuarAdapter
from engine.adapters.sde_bar import SdeBarAdapter
from engine.adapters.hodula import HodulaAdapter
from engine.adapters.shoshi_tamam import ShoshiTamamAdapter
from engine.schemas.product import RawProductPayload


class TestShopifyProductJsonParsing:
    """Test _shopify_parse_product_json against fixture data."""

    def setup_method(self):
        self.adapter = RenuarAdapter()

    def test_parses_name(self, sample_shopify_product_json):
        result = self.adapter._shopify_parse_product_json(
            sample_shopify_product_json, "https://example.com/products/shirt"
        )
        assert result is not None
        assert result["product_name"] == "חולצת פשתן נשים"

    def test_parses_price(self, sample_shopify_product_json):
        result = self.adapter._shopify_parse_product_json(
            sample_shopify_product_json, "https://example.com/products/shirt"
        )
        assert result["current_price"] == 149.0
        assert result["original_price"] == 199.0

    def test_parses_images(self, sample_shopify_product_json):
        result = self.adapter._shopify_parse_product_json(
            sample_shopify_product_json, "https://example.com/products/shirt"
        )
        assert len(result["image_urls"]) == 2
        assert "cdn.shopify.com" in result["image_urls"][0]

    def test_parses_sizes(self, sample_shopify_product_json):
        result = self.adapter._shopify_parse_product_json(
            sample_shopify_product_json, "https://example.com/products/shirt"
        )
        sizes = result["sizes_available"]
        assert "S" in sizes
        assert "M" in sizes
        assert "L" in sizes

    def test_parses_colors(self, sample_shopify_product_json):
        result = self.adapter._shopify_parse_product_json(
            sample_shopify_product_json, "https://example.com/products/shirt"
        )
        colors = result["colors_available"]
        assert len(colors) == 3

    def test_parses_variants(self, sample_shopify_product_json):
        result = self.adapter._shopify_parse_product_json(
            sample_shopify_product_json, "https://example.com/products/shirt"
        )
        variants = result["color_variant_objects"]
        assert len(variants) == 2
        in_stock_variants = [v for v in variants if v.in_stock]
        assert len(in_stock_variants) == 1  # Only first variant is in stock

    def test_parses_brand(self, sample_shopify_product_json):
        result = self.adapter._shopify_parse_product_json(
            sample_shopify_product_json, "https://example.com/products/shirt"
        )
        assert result["brand"] == "TestBrand"

    def test_returns_none_on_empty(self):
        result = self.adapter._shopify_parse_product_json({}, "https://example.com/p")
        assert result is None

    def test_returns_none_on_no_title(self):
        result = self.adapter._shopify_parse_product_json({"id": 1}, "https://x.com/p")
        assert result is None


class TestShopifyParseProduct:
    """Test the full parse_product flow with mocked HTTP."""

    def setup_method(self):
        self.adapter = RenuarAdapter()

    def test_parse_from_api_payload(self, sample_shopify_product_json):
        """Product parses correctly from Shopify API payload."""
        raw = RawProductPayload(
            source_site="renuar",
            product_url="https://www.renuar.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
            extraction_method="api",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is True
        assert result.product is not None
        assert result.product.product_name == "חולצת פשתן נשים"
        assert result.product.current_price == 149.0
        assert result.product.source_site == "renuar"
        assert result.product.product_url == "https://www.renuar.co.il/products/shirt"
        assert len(result.product.image_urls) > 0

    def test_parse_from_json_ld(self, sample_product_html):
        """Product parses correctly from JSON-LD when no API data."""
        from engine.extraction.json_ld import extract_json_ld
        json_ld = extract_json_ld(sample_product_html)
        raw = RawProductPayload(
            source_site="renuar",
            product_url="https://www.renuar.co.il/products/shirt",
            html_snapshot=sample_product_html,
            json_ld_data=json_ld,
            extraction_method="json_ld",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is True
        assert result.product.product_name == "חולצת פשתן נשים"

    def test_parse_result_has_source_site(self, sample_shopify_product_json):
        raw = RawProductPayload(
            source_site="renuar",
            product_url="https://www.renuar.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        result = self.adapter.parse_product(raw)
        assert result.product.source_site == "renuar"
        assert result.product.source_name == "Renuar"

    def test_parse_fails_gracefully_on_empty(self):
        """Parser returns failure instead of raising on empty payload."""
        raw = RawProductPayload(
            source_site="renuar",
            product_url="https://www.renuar.co.il/products/404",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is False
        assert len(result.errors) > 0

    def test_all_shopify_adapters_have_source_meta(self):
        """All Shopify adapters correctly implement source_meta."""
        for AdapterClass in [RenuarAdapter, SdeBarAdapter, HodulaAdapter, ShoshiTamamAdapter]:
            adapter = AdapterClass()
            meta = adapter.source_meta
            assert meta.source_key
            assert meta.source_name
            assert meta.base_url.startswith("http")
            assert meta.platform_family == "shopify"
