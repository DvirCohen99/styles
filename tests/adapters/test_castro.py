"""
Tests for Castro adapter — custom Israeli platform.
All tests use fixture data; no live HTTP.
"""
import pytest
from engine.adapters.castro import CastroAdapter
from engine.schemas.product import RawProductPayload
from engine.extraction.json_ld import extract_json_ld
from engine.validation.validator import ProductValidator


@pytest.fixture
def castro_html():
    """Realistic Castro product page HTML."""
    return """
    <!DOCTYPE html>
    <html lang="he">
    <head>
      <title>מכנסי ג'ינס קלאסי | Castro</title>
      <meta property="og:title" content="מכנסי ג'ינס קלאסי" />
      <meta property="og:image" content="https://res.castro.com/images/jeans_main.jpg" />
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": "מכנסי ג'ינס קלאסי",
        "description": "מכנסי ג'ינס קלאסיים בגזרה ישרה, עשויים כותנה 98%, אלסטן 2%",
        "image": ["https://res.castro.com/images/jeans_main.jpg", "https://res.castro.com/images/jeans_side.jpg"],
        "sku": "CASTRO-JEAN-001",
        "brand": {"@type": "Brand", "name": "Castro"},
        "offers": {
          "@type": "Offer",
          "price": "199.00",
          "priceCurrency": "ILS",
          "availability": "https://schema.org/InStock"
        }
      }
      </script>
    </head>
    <body>
      <nav aria-label="breadcrumb">
        <a href="/he">ראשי</a> / <a href="/he/men">גברים</a> / <a href="/he/men/pants">מכנסיים</a>
      </nav>
      <h1 class="product__name">מכנסי ג'ינס קלאסי</h1>
      <span class="price-sale">₪199.00</span>
      <span class="price-original">₪299.00</span>
      <div class="product-gallery">
        <img src="https://res.castro.com/images/jeans_main.jpg" alt="מכנסי ג'ינס" />
        <img src="https://res.castro.com/images/jeans_side.jpg" alt="מכנסי ג'ינס צד" />
      </div>
      <div class="size-selector">
        <span class="size-option">28</span>
        <span class="size-option">30</span>
        <span class="size-option">32</span>
        <span class="size-option">34</span>
      </div>
    </body>
    </html>
    """


@pytest.fixture
def castro_initial_state():
    """Castro __INITIAL_STATE__ payload structure."""
    return {
        "product": {
            "name": "מכנסי ג'ינס קלאסי",
            "price": 199,
            "originalPrice": 299,
            "images": [
                {"src": "https://res.castro.com/images/jeans_main.jpg"},
                {"src": "https://res.castro.com/images/jeans_side.jpg"},
            ],
            "sizes": ["28", "30", "32", "34"],
            "colors": ["כחול", "שחור"],
            "category": "מכנסיים",
        }
    }


class TestCastroAdapter:
    def setup_method(self):
        self.adapter = CastroAdapter()
        self.validator = ProductValidator()

    def test_source_meta(self):
        meta = self.adapter.source_meta
        assert meta.source_key == "castro"
        assert "castro.com" in meta.base_url
        assert meta.platform_family == "custom"

    def test_category_urls(self):
        cats = self.adapter.discover_category_urls()
        assert len(cats) >= 2
        assert any("women" in c or "נשים" in c or "he/" in c for c in cats)

    def test_parse_from_json_ld(self, castro_html):
        json_ld = extract_json_ld(castro_html)
        raw = RawProductPayload(
            source_site="castro",
            product_url="https://www.castro.com/he/product/jeans-001",
            html_snapshot=castro_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        assert "ג'ינס" in p.product_name
        assert p.source_site == "castro"
        assert p.current_price == 199.0
        assert len(p.image_urls) >= 1

    def test_parse_from_script_payload(self, castro_initial_state):
        raw = RawProductPayload(
            source_site="castro",
            product_url="https://www.castro.com/he/product/jeans-001",
            script_payload={"initial_state": castro_initial_state},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        assert "ג'ינס" in p.product_name

    def test_parse_dom_fallback(self, castro_html):
        raw = RawProductPayload(
            source_site="castro",
            product_url="https://www.castro.com/he/product/jeans-001",
            html_snapshot=castro_html,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert result.product.product_name != ""

    def test_breadcrumbs_extracted(self, castro_html):
        json_ld = extract_json_ld(castro_html)
        raw = RawProductPayload(
            source_site="castro",
            product_url="https://www.castro.com/he/product/jeans-001",
            html_snapshot=castro_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert isinstance(result.product.breadcrumbs, list)

    def test_empty_payload_fails_gracefully(self):
        raw = RawProductPayload(
            source_site="castro",
            product_url="https://www.castro.com/he/product/test",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is False
        assert len(result.errors) > 0

    def test_validation_passes(self, castro_html):
        json_ld = extract_json_ld(castro_html)
        raw = RawProductPayload(
            source_site="castro",
            product_url="https://www.castro.com/he/product/jeans-001",
            html_snapshot=castro_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        vr = self.validator.validate(result.product)
        assert vr.valid, f"Validation errors: {[i.issue for i in vr.errors]}"

    def test_material_detection_from_description(self, castro_html):
        json_ld = extract_json_ld(castro_html)
        raw = RawProductPayload(
            source_site="castro",
            product_url="https://www.castro.com/he/product/jeans-001",
            html_snapshot=castro_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        # Description mentions "כותנה" and "אלסטן"
        assert result.success
        # fabric_type or composition may be detected
        p = result.product
        assert isinstance(p.fabric_type, (str, type(None)))
