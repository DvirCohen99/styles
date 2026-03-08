"""
Tests for the validation layer.
"""
import pytest
from engine.schemas.product import NormalizedProduct
from engine.validation.validator import ProductValidator, ValidationReport


def make_valid_product(**kwargs) -> NormalizedProduct:
    """Helper to build a valid product with required overrides."""
    defaults = {
        "product_id": "abc123",
        "source_site": "renuar",
        "source_name": "Renuar",
        "product_url": "https://www.renuar.co.il/products/shirt",
        "product_name": "חולצת פשתן",
        "current_price": 149.0,
        "image_urls": ["https://cdn.renuar.co.il/img.jpg"],
        "currency": "ILS",
    }
    defaults.update(kwargs)
    return NormalizedProduct(**defaults)


class TestProductValidator:
    def test_valid_product_passes(self):
        p = make_valid_product()
        validator = ProductValidator()
        result = validator.validate(p)
        assert result.valid is True
        assert len(result.errors) == 0

    def test_missing_product_name_fails(self):
        p = make_valid_product(product_name="")
        validator = ProductValidator()
        result = validator.validate(p)
        assert result.valid is False
        field_names = [i.field for i in result.errors]
        assert "product_name" in field_names

    def test_missing_source_site_fails(self):
        p = make_valid_product(source_site="")
        validator = ProductValidator()
        result = validator.validate(p)
        assert result.valid is False

    def test_missing_price_warns(self):
        p = make_valid_product(current_price=None)
        validator = ProductValidator()
        result = validator.validate(p)
        # Price is warned, not error
        warning_fields = [i.field for i in result.warnings]
        assert "current_price" in warning_fields

    def test_missing_images_warns(self):
        p = make_valid_product(image_urls=[])
        validator = ProductValidator()
        result = validator.validate(p)
        warning_fields = [i.field for i in result.warnings]
        assert "image_urls" in warning_fields

    def test_zero_price_fails(self):
        p = make_valid_product(current_price=0.0)
        validator = ProductValidator()
        result = validator.validate(p)
        assert result.valid is False

    def test_non_http_product_url_fails(self):
        p = make_valid_product(product_url="not-a-url")
        validator = ProductValidator()
        result = validator.validate(p)
        assert result.valid is False

    def test_validate_all_returns_report(self):
        products = [
            make_valid_product(product_id=f"id{i}", product_url=f"https://x.com/p/{i}")
            for i in range(5)
        ]
        validator = ProductValidator()
        report = validator.validate_all("renuar", products)
        assert report.total == 5
        assert report.passed == 5
        assert report.failed == 0

    def test_validate_all_counts_failures(self):
        products = [
            make_valid_product(product_id=f"id{i}", product_url=f"https://x.com/p/{i}")
            for i in range(3)
        ] + [
            make_valid_product(product_id=f"fail{i}", product_name="",
                               product_url=f"https://x.com/fail/{i}")
            for i in range(2)
        ]
        validator = ProductValidator()
        report = validator.validate_all("renuar", products)
        assert report.total == 5
        assert report.passed == 3
        assert report.failed == 2
        assert report.missing_name == 2

    def test_summary_dict_structure(self):
        validator = ProductValidator()
        report = validator.validate_all("test", [])
        d = report.summary_dict()
        assert "source_key" in d
        assert "total" in d
        assert "pass_rate" in d
