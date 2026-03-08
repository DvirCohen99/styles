"""
Tests for output layer — JSON writer and stats.
"""
import json
import pytest
from pathlib import Path
from engine.schemas.product import NormalizedProduct
from engine.schemas.source import SourceStats
from engine.output.json_writer import JSONWriter


def make_product(source="renuar", url_suffix="shirt") -> NormalizedProduct:
    return NormalizedProduct(
        source_site=source,
        source_name=source.title(),
        product_url=f"https://example.com/products/{url_suffix}",
        product_name=f"Test Product {url_suffix}",
        current_price=149.0,
        original_price=199.0,
        image_urls=["https://cdn.example.com/img.jpg"],
        sizes_available=["S", "M", "L"],
        colors_available=["Red", "Blue"],
        currency="ILS",
    )


class TestJSONWriter:
    def test_write_products(self, tmp_path):
        writer = JSONWriter(output_dir=tmp_path)
        products = [make_product(url_suffix=f"item-{i}") for i in range(5)]
        out = writer.write_products("renuar", products)
        assert out.exists()
        data = json.loads(out.read_text(encoding="utf-8"))
        assert len(data) == 5

    def test_write_products_is_valid_json(self, tmp_path):
        writer = JSONWriter(output_dir=tmp_path)
        products = [make_product()]
        out = writer.write_products("renuar", products)
        records = json.loads(out.read_text(encoding="utf-8"))
        assert isinstance(records, list)
        assert isinstance(records[0], dict)
        assert records[0]["product_name"] == "Test Product shirt"

    def test_write_ndjson(self, tmp_path):
        writer = JSONWriter(output_dir=tmp_path)
        products = [make_product(url_suffix=f"x{i}") for i in range(3)]
        out = writer.write_ndjson("renuar", products)
        assert out.exists()
        lines = out.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 3
        for line in lines:
            obj = json.loads(line)
            assert "product_name" in obj

    def test_firebase_dict_excludes_raw_payload(self, tmp_path):
        writer = JSONWriter(output_dir=tmp_path)
        p = make_product()
        p.raw_source_payload = {"huge": "blob" * 1000}
        out = writer.write_products("renuar", [p], include_raw=False)
        records = json.loads(out.read_text(encoding="utf-8"))
        assert "raw_source_payload" not in records[0]

    def test_write_stats(self, tmp_path):
        writer = JSONWriter(output_dir=tmp_path)
        products = [make_product()]
        stats = SourceStats(source_key="renuar")
        stats.products_parsed = 1
        stats.mark_finished()
        out = writer.write_stats("renuar", stats, products)
        assert out.exists()
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["source_key"] == "renuar"
        assert "computed" in data
        assert data["computed"]["products_on_sale"] == 1

    def test_write_stats_computed_fields(self, tmp_path):
        writer = JSONWriter(output_dir=tmp_path)
        products = [
            make_product(url_suffix="a"),
            NormalizedProduct(
                source_site="renuar",
                source_name="Renuar",
                product_url="https://example.com/b",
                product_name="No Price Item",
                image_urls=["https://cdn.example.com/b.jpg"],
            ),
        ]
        stats = SourceStats(source_key="renuar")
        out = writer.write_stats("renuar", stats, products)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["computed"]["products_with_price"] == 1
        assert data["computed"]["total_products_in_output"] == 2

    def test_output_dir_created_if_not_exists(self, tmp_path):
        new_dir = tmp_path / "nested" / "deep"
        writer = JSONWriter(output_dir=new_dir)
        assert new_dir.exists()


class TestNormalizedProductFirebaseDict:
    def test_firebase_dict_has_required_fields(self):
        p = make_product()
        d = p.to_firebase_dict()
        for field in ["product_id", "source_site", "product_url", "product_name"]:
            assert field in d, f"Missing: {field}"

    def test_firebase_dict_variants_are_dicts(self):
        from engine.schemas.product import ProductVariant
        p = make_product()
        p.color_variant_objects = [
            ProductVariant(color="Red", size="M", price=149.0, in_stock=True)
        ]
        d = p.to_firebase_dict()
        assert isinstance(d["color_variant_objects"], list)
        assert isinstance(d["color_variant_objects"][0], dict)

    def test_to_json_dict_includes_raw_payload(self):
        p = make_product()
        p.raw_source_payload = {"method": "api", "url": "x"}
        d = p.to_json_dict()
        assert "raw_source_payload" in d
        assert d["raw_source_payload"]["method"] == "api"

    def test_completeness_score_full_product(self):
        p = NormalizedProduct(
            source_site="renuar",
            source_name="Renuar",
            product_url="https://example.com/p",
            product_name="Test",
            current_price=100.0,
            original_price=150.0,
            image_urls=["https://cdn.example.com/img.jpg"],
            category="חולצות",
            short_description="תיאור",
            sizes_available=["S", "M"],
            colors_available=["Red"],
            brand="TestBrand",
            sku="SKU-001",
            stock_status="in_stock",
        )
        score = p.completeness_score
        assert score > 0.7

    def test_completeness_score_minimal_product(self):
        p = NormalizedProduct(
            source_site="renuar",
            source_name="Renuar",
            product_url="https://example.com/p",
            product_name="Test",
        )
        assert p.completeness_score < 0.5
