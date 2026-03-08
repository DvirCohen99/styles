"""
Tests for CStyle adapter — WooCommerce platform.
"""
import pytest
from engine.adapters.cstyle import CStyleAdapter
from engine.schemas.product import RawProductPayload
from engine.extraction.json_ld import extract_json_ld
from engine.validation.validator import ProductValidator


@pytest.fixture
def wc_product_api():
    """WooCommerce REST API product response."""
    return {
        "id": 5501,
        "name": "חצאית פליסה קצרה",
        "slug": "pleated-mini-skirt",
        "permalink": "https://www.cstyle.co.il/product/pleated-mini-skirt/",
        "price": "149",
        "regular_price": "199",
        "sale_price": "149",
        "on_sale": True,
        "in_stock": True,
        "description": "<p>חצאית פליסה קצרה בסגנון Y2K. עשויה פוליאסטר 95%, ספנדקס 5%.</p>",
        "short_description": "<p>חצאית פליסה בסגנון Y2K</p>",
        "images": [
            {"src": "https://www.cstyle.co.il/wp-content/uploads/skirt_main.jpg", "alt": "חצאית פליסה"},
            {"src": "https://www.cstyle.co.il/wp-content/uploads/skirt_back.jpg", "alt": "חצאית פליסה גב"},
        ],
        "categories": [{"id": 12, "name": "חצאיות", "slug": "skirts"}],
        "attributes": [
            {"id": 1, "name": "מידה", "options": ["XS", "S", "M", "L", "XL"]},
            {"id": 2, "name": "צבע", "options": ["שחור", "לבן", "ורוד"]},
        ],
    }


@pytest.fixture
def woocommerce_html():
    return """
    <html lang="he">
    <head>
      <title>חצאית פליסה קצרה | CStyle</title>
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": "חצאית פליסה קצרה",
        "description": "חצאית פליסה בסגנון Y2K",
        "image": "https://www.cstyle.co.il/wp-content/uploads/skirt_main.jpg",
        "brand": {"@type": "Brand", "name": "CStyle"},
        "offers": {
          "@type": "Offer",
          "price": "149",
          "priceCurrency": "ILS",
          "availability": "https://schema.org/InStock"
        }
      }
      </script>
    </head>
    <body>
      <h1 class="product_title">חצאית פליסה קצרה</h1>
      <div class="price">
        <del><span class="woocommerce-Price-amount">₪199.00</span></del>
        <ins><span class="woocommerce-Price-amount">₪149.00</span></ins>
      </div>
      <div class="woocommerce-product-gallery__image">
        <img src="https://www.cstyle.co.il/wp-content/uploads/skirt_main.jpg" />
      </div>
    </body>
    </html>
    """


class TestCStyleAdapter:
    def setup_method(self):
        self.adapter = CStyleAdapter()
        self.validator = ProductValidator()

    def test_source_meta(self):
        meta = self.adapter.source_meta
        assert meta.source_key == "cstyle"
        assert "cstyle.co.il" in meta.base_url
        assert meta.platform_family == "woocommerce"

    def test_category_urls(self):
        cats = self.adapter.discover_category_urls()
        assert len(cats) >= 1
        assert all("cstyle.co.il" in c for c in cats)

    def test_parse_from_wc_api(self, wc_product_api):
        raw = RawProductPayload(
            source_site="cstyle",
            product_url="https://www.cstyle.co.il/product/pleated-mini-skirt/",
            script_payload={"wc_product": wc_product_api},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        p = result.product
        assert "חצאית" in p.product_name
        assert p.source_site == "cstyle"
        assert p.current_price == 149.0
        assert p.original_price == 199.0
        assert p.is_on_sale is True
        assert len(p.image_urls) == 2
        assert "S" in p.sizes_available
        assert len(p.colors_available) == 3

    def test_parse_from_json_ld(self, woocommerce_html):
        json_ld = extract_json_ld(woocommerce_html)
        raw = RawProductPayload(
            source_site="cstyle",
            product_url="https://www.cstyle.co.il/product/skirt/",
            html_snapshot=woocommerce_html,
            json_ld_data=json_ld,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert "חצאית" in result.product.product_name
        assert result.product.current_price == 149.0

    def test_parse_dom_fallback(self, woocommerce_html):
        raw = RawProductPayload(
            source_site="cstyle",
            product_url="https://www.cstyle.co.il/product/skirt/",
            html_snapshot=woocommerce_html,
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert result.product.product_name != ""

    def test_empty_payload_fails_gracefully(self):
        raw = RawProductPayload(
            source_site="cstyle",
            product_url="https://www.cstyle.co.il/product/test/",
        )
        result = self.adapter.parse_product(raw)
        assert result.success is False

    def test_wc_slug_extraction(self):
        slug = self.adapter._extract_wc_slug(
            "https://www.cstyle.co.il/product/pleated-mini-skirt/"
        )
        assert slug == "pleated-mini-skirt"

    def test_wc_slug_extraction_no_match(self):
        slug = self.adapter._extract_wc_slug("https://www.cstyle.co.il/category/tops/")
        assert slug is None

    def test_validation_passes(self, wc_product_api):
        raw = RawProductPayload(
            source_site="cstyle",
            product_url="https://www.cstyle.co.il/product/skirt/",
            script_payload={"wc_product": wc_product_api},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        vr = self.validator.validate(result.product)
        assert vr.valid, f"Errors: {[i.issue for i in vr.errors]}"

    def test_in_stock_detected(self, wc_product_api):
        raw = RawProductPayload(
            source_site="cstyle",
            product_url="https://www.cstyle.co.il/product/skirt/",
            script_payload={"wc_product": wc_product_api},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert result.product.in_stock is True
        assert result.product.out_of_stock is False

    def test_out_of_stock_detected(self, wc_product_api):
        wc_product_api["in_stock"] = False
        raw = RawProductPayload(
            source_site="cstyle",
            product_url="https://www.cstyle.co.il/product/skirt/",
            script_payload={"wc_product": wc_product_api},
        )
        result = self.adapter.parse_product(raw)
        assert result.success
        assert result.product.out_of_stock is True
