"""
Tests for remaining Shopify-based adapters:
  - Sde Bar
  - Hodula
  - Lidor Bar
  - Shoshi Tamam

All use the ShopifyMixin — tested for correct branding, meta,
category URLs, and parse pipeline.
"""
import pytest
from engine.schemas.product import RawProductPayload
from engine.extraction.json_ld import extract_json_ld
from engine.validation.validator import ProductValidator


@pytest.fixture
def validator():
    return ProductValidator()


# ──────────────────────────────────────────────────────────────────────────────
# SDE BAR
# ──────────────────────────────────────────────────────────────────────────────

class TestSdeBarAdapter:
    def setup_method(self):
        from engine.adapters.sde_bar import SdeBarAdapter
        self.adapter = SdeBarAdapter()

    def test_source_meta(self):
        meta = self.adapter.source_meta
        assert meta.source_key == "sde_bar"
        assert "sdebar.co.il" in meta.base_url
        assert meta.platform_family == "shopify"
        assert meta.has_api is True

    def test_category_urls(self):
        cats = self.adapter.discover_category_urls()
        assert len(cats) >= 2
        assert all("sdebar.co.il" in c for c in cats)

    def test_parse_from_shopify_json(self, sample_shopify_product_json, validator):
        raw = RawProductPayload(
            source_site="sde_bar",
            product_url="https://www.sdebar.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        assert p.source_site == "sde_bar"
        assert p.product_name == "חולצת פשתן נשים"
        assert p.current_price == 149.0
        assert len(p.image_urls) >= 1
        vr = validator.validate(p)
        assert vr.valid

    def test_brand_set_to_sde_bar(self, sample_shopify_product_json):
        # Override vendor to be empty to trigger fallback brand assignment
        sample_shopify_product_json["vendor"] = ""
        raw = RawProductPayload(
            source_site="sde_bar",
            product_url="https://www.sdebar.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert result.product.brand == "Sde Bar"

    def test_parse_from_json_ld(self, sample_product_html, validator):
        json_ld = extract_json_ld(sample_product_html)
        raw = RawProductPayload(
            source_site="sde_bar",
            product_url="https://www.sdebar.co.il/products/test",
            html_snapshot=sample_product_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        vr = validator.validate(result.product)
        assert vr.valid

    def test_empty_payload_fails_gracefully(self):
        raw = RawProductPayload(
            source_site="sde_bar",
            product_url="https://www.sdebar.co.il/products/test",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is False


# ──────────────────────────────────────────────────────────────────────────────
# HODULA
# ──────────────────────────────────────────────────────────────────────────────

class TestHodulaAdapter:
    def setup_method(self):
        from engine.adapters.hodula import HodulaAdapter
        self.adapter = HodulaAdapter()

    def test_source_meta(self):
        meta = self.adapter.source_meta
        assert meta.source_key == "hodula"
        assert "hodula.co.il" in meta.base_url
        assert meta.platform_family == "shopify"

    def test_category_urls(self):
        cats = self.adapter.discover_category_urls()
        assert len(cats) >= 2

    def test_parse_from_shopify_json(self, sample_shopify_product_json, validator):
        raw = RawProductPayload(
            source_site="hodula",
            product_url="https://www.hodula.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        assert p.source_site == "hodula"
        assert p.current_price == 149.0
        assert len(p.image_urls) >= 1
        vr = validator.validate(p)
        assert vr.valid

    def test_sale_detection(self, sample_shopify_product_json, validator):
        raw = RawProductPayload(
            source_site="hodula",
            product_url="https://www.hodula.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert result.product.is_on_sale is True
        assert result.product.discount_amount == 50.0

    def test_empty_payload_fails_gracefully(self):
        raw = RawProductPayload(
            source_site="hodula",
            product_url="https://www.hodula.co.il/products/test",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is False


# ──────────────────────────────────────────────────────────────────────────────
# LIDOR BAR
# ──────────────────────────────────────────────────────────────────────────────

class TestLidorBarAdapter:
    def setup_method(self):
        from engine.adapters.lidor_bar import LidorBarAdapter
        self.adapter = LidorBarAdapter()

    def test_source_meta(self):
        meta = self.adapter.source_meta
        assert meta.source_key == "lidor_bar"
        assert meta.platform_family == "shopify"
        assert "URL needs verification" in meta.notes

    def test_category_urls_include_base_url(self):
        # Force resolved URL to avoid HTTP call
        self.adapter._resolved_url = self.adapter.BASE_URL
        cats = self.adapter.discover_category_urls()
        assert len(cats) >= 2

    def test_parse_from_shopify_json(self, sample_shopify_product_json, validator):
        raw = RawProductPayload(
            source_site="lidor_bar",
            product_url="https://www.lidorbar.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        assert p.source_site == "lidor_bar"
        assert p.current_price == 149.0

    def test_brand_fallback_to_lidor_bar(self, sample_shopify_product_json, validator):
        sample_shopify_product_json["vendor"] = ""
        raw = RawProductPayload(
            source_site="lidor_bar",
            product_url="https://www.lidorbar.co.il/products/shirt",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert result.product.brand == "Lidor Bar"

    def test_empty_payload_fails_gracefully(self):
        raw = RawProductPayload(
            source_site="lidor_bar",
            product_url="https://www.lidorbar.co.il/products/test",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is False


# ──────────────────────────────────────────────────────────────────────────────
# SHOSHI TAMAM
# ──────────────────────────────────────────────────────────────────────────────

class TestShoshiTamamAdapter:
    def setup_method(self):
        from engine.adapters.shoshi_tamam import ShoshiTamamAdapter
        self.adapter = ShoshiTamamAdapter()

    def test_source_meta(self):
        meta = self.adapter.source_meta
        assert meta.source_key == "shoshi_tamam"
        assert "shoshitamam.co.il" in meta.base_url
        assert meta.platform_family == "shopify"

    def test_category_urls(self):
        cats = self.adapter.discover_category_urls()
        assert len(cats) >= 2

    def test_parse_from_shopify_json(self, sample_shopify_product_json, validator):
        raw = RawProductPayload(
            source_site="shoshi_tamam",
            product_url="https://www.shoshitamam.co.il/products/dress",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        assert p.source_site == "shoshi_tamam"
        assert p.current_price == 149.0
        vr = validator.validate(p)
        assert vr.valid

    def test_brand_fallback(self, sample_shopify_product_json, validator):
        sample_shopify_product_json["vendor"] = ""
        raw = RawProductPayload(
            source_site="shoshi_tamam",
            product_url="https://www.shoshitamam.co.il/products/dress",
            script_payload={"shopify_product": sample_shopify_product_json},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert result.product.brand == "Shoshi Tamam"

    def test_parse_from_json_ld(self, sample_product_html, validator):
        json_ld = extract_json_ld(sample_product_html)
        raw = RawProductPayload(
            source_site="shoshi_tamam",
            product_url="https://www.shoshitamam.co.il/products/test",
            html_snapshot=sample_product_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        vr = validator.validate(result.product)
        assert vr.valid

    def test_empty_payload_fails_gracefully(self):
        raw = RawProductPayload(
            source_site="shoshi_tamam",
            product_url="https://www.shoshitamam.co.il/products/test",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is False


# ──────────────────────────────────────────────────────────────────────────────
# Cross-adapter invariants
# ──────────────────────────────────────────────────────────────────────────────

class TestShopifyAdapterInvariants:
    """Properties that all Shopify adapters must satisfy."""

    SHOPIFY_KEYS = ["renuar", "sde_bar", "hodula", "lidor_bar", "shoshi_tamam"]

    def test_all_have_shopify_platform(self):
        from engine.registry.source_registry import get_adapter
        for key in self.SHOPIFY_KEYS:
            adapter = get_adapter(key)
            assert adapter.PLATFORM_FAMILY == "shopify", f"{key} not shopify"

    def test_all_have_category_urls(self):
        from engine.registry.source_registry import get_adapter
        for key in self.SHOPIFY_KEYS:
            adapter = get_adapter(key)
            # Lidor Bar needs resolved URL; skip network call by setting manually
            if key == "lidor_bar":
                adapter._resolved_url = adapter.BASE_URL
            cats = adapter.discover_category_urls()
            assert len(cats) >= 1, f"{key} has no categories"

    def test_all_parse_shopify_json(self, sample_shopify_product_json):
        from engine.registry.source_registry import get_adapter
        validator = ProductValidator()
        for key in self.SHOPIFY_KEYS:
            adapter = get_adapter(key)
            # Use a URL from the adapter's base
            base = adapter.BASE_URL
            raw = RawProductPayload(
                source_site=key,
                product_url=f"{base}/products/test-shirt",
                script_payload={"shopify_product": sample_shopify_product_json},
            )
            result = adapter.parse_product(raw)
            assert result.success, f"{key} parse failed: {result.errors}"
            vr = validator.validate(result.product)
            assert vr.valid, f"{key} validation failed: {[i.issue for i in vr.errors]}"
