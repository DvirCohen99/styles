"""
Tests for Pydantic schemas.
"""
import pytest
from engine.schemas.product import NormalizedProduct, ProductVariant, RawProductPayload
from engine.schemas.source import SourceStats, SourceMeta
from engine.schemas.result import ParseResult, ParseWarning, HealthCheckResult


class TestNormalizedProduct:
    def test_basic_creation(self):
        p = NormalizedProduct(
            product_id="abc123",
            source_site="renuar",
            source_name="Renuar",
            product_url="https://www.renuar.co.il/products/shirt",
            product_name="חולצת פשתן",
            current_price=149.0,
            image_urls=["https://cdn.renuar.co.il/img.jpg"],
        )
        assert p.product_id == "abc123"
        assert p.product_name == "חולצת פשתן"
        assert p.current_price == 149.0
        assert p.image_count == 1
        assert p.primary_image_url == "https://cdn.renuar.co.il/img.jpg"

    def test_sale_detection(self):
        p = NormalizedProduct(
            product_id="abc",
            source_site="renuar",
            source_name="Renuar",
            product_url="https://example.com/p",
            product_name="Test",
            current_price=100.0,
            original_price=150.0,
        )
        assert p.is_on_sale is True
        assert p.discount_amount == 50.0
        assert p.discount_percent == 33.3

    def test_no_sale_when_prices_equal(self):
        p = NormalizedProduct(
            product_id="abc",
            source_site="renuar",
            source_name="Renuar",
            product_url="https://example.com/p",
            product_name="Test",
            current_price=100.0,
            original_price=100.0,
        )
        assert p.is_on_sale is False

    def test_make_id_deterministic(self):
        id1 = NormalizedProduct.make_id("renuar", "https://example.com/products/shirt")
        id2 = NormalizedProduct.make_id("renuar", "https://example.com/products/shirt")
        assert id1 == id2
        assert len(id1) == 32  # MD5 hex

    def test_make_id_strips_query_params(self):
        id1 = NormalizedProduct.make_id("renuar", "https://example.com/products/shirt")
        id2 = NormalizedProduct.make_id("renuar", "https://example.com/products/shirt?color=red")
        assert id1 == id2

    def test_stock_status_out_of_stock(self):
        p = NormalizedProduct(
            product_id="x",
            source_site="s",
            source_name="S",
            product_url="https://x.com/p",
            product_name="T",
            out_of_stock=True,
        )
        assert p.stock_status == "out_of_stock"
        assert p.in_stock is False

    def test_to_firebase_dict_no_raw_payload(self):
        p = NormalizedProduct(
            product_id="x",
            source_site="s",
            source_name="S",
            product_url="https://x.com/p",
            product_name="Test Product",
            raw_source_payload={"big": "data"},
        )
        d = p.to_firebase_dict()
        assert "raw_source_payload" not in d

    def test_searchable_text_blob_populated(self):
        p = NormalizedProduct(
            product_id="x",
            source_site="s",
            source_name="S",
            product_url="https://x.com/p",
            product_name="חולצה לבנה",
            category="חולצות",
            colors_available=["לבן", "שחור"],
        )
        assert "חולצה לבנה" in p.searchable_text_blob
        assert "חולצות" in p.searchable_text_blob


class TestProductVariant:
    def test_basic_variant(self):
        v = ProductVariant(
            variant_id="v1",
            color="אדום",
            size="M",
            price=149.0,
            in_stock=True,
        )
        assert v.is_on_sale is False

    def test_variant_on_sale(self):
        v = ProductVariant(price=100.0, original_price=150.0, in_stock=True)
        assert v.is_on_sale is True


class TestSourceStats:
    def test_mark_finished_sets_health_ok(self):
        stats = SourceStats(source_key="test")
        stats.products_attempted = 10
        stats.products_parsed = 10
        stats.products_failed = 0
        stats.mark_finished()
        assert stats.parser_health_status == "ok"
        assert stats.duration_sec is not None

    def test_mark_finished_sets_health_degraded(self):
        stats = SourceStats(source_key="test")
        stats.products_attempted = 10
        stats.products_parsed = 5
        stats.products_failed = 5
        stats.mark_finished()
        assert stats.parser_health_status == "degraded"

    def test_mark_finished_sets_health_failed(self):
        stats = SourceStats(source_key="test")
        stats.products_attempted = 5
        stats.products_parsed = 0
        stats.mark_finished()
        assert stats.parser_health_status == "failed"
