"""
Tests for Zara adapter — Inditex custom API.
All tests use fixture data; no live HTTP.
"""
import pytest
from engine.adapters.zara import ZaraAdapter
from engine.schemas.product import RawProductPayload
from engine.validation.validator import ProductValidator


@pytest.fixture
def zara_product_api():
    """Zara product API response structure (from /itxrest/ endpoint)."""
    return {
        "id": 987654,
        "name": "שמלת מידי פרחונית",
        "description": "שמלת מידי עם הדפס פרחים ורוד. עשויה ויסקוזה 100%.",
        "price": 19900,           # in cents
        "originalPrice": 29900,   # in cents
        "seoKeyword": "midi-floral-dress",
        "detail": {
            "colors": [
                {
                    "name": "CORAL PINK",
                    "xmedia": [
                        {"path": "2024/p", "name": "dress_01", "type": "image"},
                        {"path": "2024/p", "name": "dress_02", "type": "image"},
                    ],
                    "sizes": [
                        {"name": "XS", "availability": 1},
                        {"name": "S", "availability": 1},
                        {"name": "M", "availability": 0},
                        {"name": "L", "availability": 1},
                    ],
                },
                {
                    "name": "WHITE",
                    "xmedia": [{"path": "2024/p", "name": "dress_white_01", "type": "image"}],
                    "sizes": [
                        {"name": "XS", "availability": 1},
                        {"name": "S", "availability": 0},
                    ],
                },
            ]
        },
        "sectionName": "WOMAN",
        "familyName": "DRESSES",
    }


@pytest.fixture
def zara_html_with_json_ld():
    return """
    <html><head>
    <title>שמלת מידי פרחונית | ZARA ישראל</title>
    <script type="application/ld+json">
    {
      "@context": "https://schema.org",
      "@type": "Product",
      "name": "שמלת מידי פרחונית",
      "description": "שמלה קיצית עם הדפס פרחים",
      "image": "https://static.zara.net/photos/2024/p/dress_01/w/750/dress_01.jpg",
      "brand": {"@type": "Brand", "name": "Zara"},
      "offers": {
        "@type": "Offer",
        "price": "199.00",
        "priceCurrency": "ILS",
        "availability": "https://schema.org/InStock"
      }
    }
    </script>
    </head>
    <body><h1>שמלת מידי פרחונית</h1></body></html>
    """


class TestZaraAdapter:
    def setup_method(self):
        self.adapter = ZaraAdapter()
        self.validator = ProductValidator()

    def test_source_meta(self):
        meta = self.adapter.source_meta
        assert meta.source_key == "zara"
        assert "zara.com" in meta.base_url
        assert meta.platform_family == "custom"
        assert meta.has_api is True

    def test_category_urls(self):
        cats = self.adapter.discover_category_urls()
        assert len(cats) >= 2
        assert all("zara.com" in c for c in cats)

    def test_parse_from_api_payload(self, zara_product_api):
        raw = RawProductPayload(
            source_site="zara",
            product_url="https://www.zara.com/il/he/midi-floral-dress-p987654.html",
            script_payload={"zara_product": zara_product_api},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        assert "שמלת" in p.product_name
        assert p.source_site == "zara"
        assert p.current_price == 199.0           # 19900 cents → 199.00 ILS
        assert p.original_price == 299.0           # 29900 cents → 299.00 ILS
        assert p.is_on_sale is True
        assert "CORAL PINK" in p.colors_available or len(p.colors_available) >= 1
        assert len(p.sizes_available) >= 2
        assert len(p.image_urls) >= 1

    def test_price_conversion_from_cents(self, zara_product_api):
        raw = RawProductPayload(
            source_site="zara",
            product_url="https://www.zara.com/il/he/dress-p987654.html",
            script_payload={"zara_product": zara_product_api},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        # 19900 cents = 199.00 ILS
        assert result.product.current_price == 199.0

    def test_stock_detection_from_sizes(self, zara_product_api):
        raw = RawProductPayload(
            source_site="zara",
            product_url="https://www.zara.com/il/he/dress-p987654.html",
            script_payload={"zara_product": zara_product_api},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        # Some sizes are in stock
        assert result.product.in_stock is not False

    def test_parse_from_json_ld_fallback(self, zara_html_with_json_ld):
        from engine.extraction.json_ld import extract_json_ld
        json_ld = extract_json_ld(zara_html_with_json_ld)
        raw = RawProductPayload(
            source_site="zara",
            product_url="https://www.zara.com/il/he/dress-p987654.html",
            html_snapshot=zara_html_with_json_ld,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert "שמלת" in result.product.product_name

    def test_empty_payload_fails_gracefully(self):
        raw = RawProductPayload(
            source_site="zara",
            product_url="https://www.zara.com/il/he/test-p000.html",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is False

    def test_validation_passes(self, zara_product_api):
        raw = RawProductPayload(
            source_site="zara",
            product_url="https://www.zara.com/il/he/dress-p987654.html",
            script_payload={"zara_product": zara_product_api},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        vr = self.validator.validate(result.product)
        assert vr.valid, f"Errors: {[i.issue for i in vr.errors]}"

    def test_gender_detection_woman(self, zara_product_api):
        raw = RawProductPayload(
            source_site="zara",
            product_url="https://www.zara.com/il/he/dress-p987654.html",
            script_payload={"zara_product": zara_product_api},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        # sectionName is "WOMAN" → should detect as women
        assert result.product.gender_target in ("women", "unisex")

    def test_build_product_url(self):
        """_build_product_url generates correct URL format."""
        product_data = {
            "id": 987654,
            "seo": {"keyword": "midi-dress-floral"},
        }
        url = self.adapter._build_product_url(product_data)
        assert url is not None
        assert "987654" in url
        assert "zara.com" in url
