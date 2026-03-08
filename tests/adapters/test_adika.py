"""
Tests for Adika adapter (Next.js platform).
"""
import pytest
from engine.adapters.adika import AdikaAdapter
from engine.schemas.product import RawProductPayload
from engine.extraction.script_payload import extract_script_payload


class TestAdikaAdapter:
    def setup_method(self):
        self.adapter = AdikaAdapter()

    def test_source_meta(self):
        meta = self.adapter.source_meta
        assert meta.source_key == "adika"
        assert meta.source_name == "Adika"
        assert "adika.co.il" in meta.base_url

    def test_parse_from_next_data(self, sample_next_data_html):
        """Parse product from Next.js __NEXT_DATA__ payload."""
        payloads = extract_script_payload(sample_next_data_html)
        raw = RawProductPayload(
            source_site="adika",
            product_url="https://www.adika.co.il/product/dress-123",
            html_snapshot=sample_next_data_html,
            script_payload=payloads,
        )
        result = self.adapter.parse_product(raw)
        assert result.success is True
        assert result.product is not None
        p = result.product

        # Required core fields
        assert p.product_name == "שמלת קיץ פרחונית"
        assert p.source_site == "adika"
        assert p.product_url == "https://www.adika.co.il/product/dress-123"
        assert p.current_price == 259.0
        assert p.original_price == 349.0
        assert len(p.image_urls) == 2
        assert p.is_on_sale is True

    def test_parse_result_does_not_crash_on_missing_fields(self):
        """Parser is resilient to empty/partial data."""
        raw = RawProductPayload(
            source_site="adika",
            product_url="https://www.adika.co.il/product/test",
            script_payload={"next_data": {"props": {"pageProps": {}}}},
        )
        result = self.adapter.parse_product(raw)
        # Should fail gracefully, not raise
        assert isinstance(result.success, bool)
        assert isinstance(result.errors, list)

    def test_parse_from_json_ld_fallback(self, sample_product_html):
        from engine.extraction.json_ld import extract_json_ld
        json_ld = extract_json_ld(sample_product_html)
        raw = RawProductPayload(
            source_site="adika",
            product_url="https://www.adika.co.il/product/test",
            html_snapshot=sample_product_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success is True
        assert result.product.product_name == "חולצת פשתן נשים"

    def test_normalized_product_passes_schema(self, sample_next_data_html):
        """Parsed product passes schema validation."""
        from engine.validation.validator import ProductValidator
        payloads = extract_script_payload(sample_next_data_html)
        raw = RawProductPayload(
            source_site="adika",
            product_url="https://www.adika.co.il/product/dress-123",
            script_payload=payloads,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        validator = ProductValidator()
        vr = validator.validate(result.product)
        assert vr.valid


class TestAdikaNextDataParsing:
    def setup_method(self):
        self.adapter = AdikaAdapter()

    def test_parses_sizes(self, sample_next_data_html):
        payloads = extract_script_payload(sample_next_data_html)
        product_data = payloads["next_data"]["props"]["pageProps"]["product"]
        partial = self.adapter._parse_next_data_product(product_data)
        assert "XS" in partial["sizes_available"] or "S" in partial["sizes_available"]

    def test_parses_colors(self, sample_next_data_html):
        payloads = extract_script_payload(sample_next_data_html)
        product_data = payloads["next_data"]["props"]["pageProps"]["product"]
        partial = self.adapter._parse_next_data_product(product_data)
        assert len(partial["colors_available"]) == 2

    def test_returns_none_on_no_name(self):
        result = self.adapter._parse_next_data_product({"price": 100})
        assert result is None
